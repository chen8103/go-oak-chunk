package conf

import (
	"fmt"
	"os"
	"strings"

	"github.com/pelletier/go-toml"

	"github.com/SisyphusSQ/go-oak-chunk/v3/log"
)

type Config struct {
	ChunkSize           int64  `toml:"chunk_size"`
	ExecuteQuery        string `toml:"execute_query"`
	ForceChunkingColumn string `toml:"forced_chunking_column"`
	Host                string `toml:"host"`
	NoLogBin            bool   `toml:"no_log_bin"`
	User                string `toml:"user"`
	Password            string `toml:"password"`
	Port                int    `toml:"port"`
	PrintProgress       bool   `toml:"print_progress"`
	Sleep               int64  `toml:"sleep"`
	NoConsiderLag       bool   `toml:"no_consider_lag"`
	MaxLag              int64  `toml:"max_lag"`
	IncludeSlaves       string `toml:"include_slaves"`
	ExcludeSlaves       string `toml:"exclude_slaves"`
	NoSlaves            bool   `toml:"no_slaves"`
	RowsPerSec          int64  `toml:"rows_per_sec"` // row-rate cap (0=unlimited)

	//SkipLockTables      bool   `toml:"skip_lock_tables"`
	Database string `toml:"database"`
	TxnSize  int64  `toml:"txn_size"`
	Debug    bool   `toml:"debug_mode"`

	// P2: OB covering-index fast-path (DELETE only, single worker).
	SelectIndex   string `toml:"select_index"`    // optional FORCE INDEX name
	SelectOrderBy string `toml:"select_order_by"` // order columns (comma-separated)
	SelectCursor  bool   `toml:"select_cursor"`   // advance via cursor, avoid re-scan

	// P3: OceanBase partition-parallel covering DELETE.
	PartitionConcurrency int `toml:"partition_concurrency"` // >1 enables OB partition-parallel covering DELETE; 0/1=off

	// P4: TiDB _tidb_rowid chunked DELETE (NONCLUSTERED tables, no PK/UK needed).
	TiDBRowID bool `toml:"tidb_rowid"`

	// P2: shared guardrails and preflight.
	MaxRows            int64 `toml:"max_rows"`            // max rows to act on (0=unlimited)
	MaxDuration        int64 `toml:"max_duration_ms"`     // max run time in ms (0=unlimited)
	DryRun             bool  `toml:"dry_run"`             // print sample SQL, do not execute
	PreflightThreshold int64 `toml:"preflight_threshold"` // EXPLAIN confirm threshold (0=default)
	AutoConfirm        bool  `toml:"auto_confirm"`        // skip large-table confirmation

	// 修正
	Correct int64 `toml:"correct"`
}

func NewConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	decoder := toml.NewDecoder(file)
	c := new(Config)
	err = decoder.Decode(c)
	if err != nil {
		return nil, err
	}
	if err = c.PreCheck(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Config) PreCheck() error {
	if c.ChunkSize < 0 {
		log.Logger.Error("Chunk size must be nonnegative number. You can leave the default 1000 if unsure")
		return fmt.Errorf("chunk size must be nonnegative number")
	}

	if c.ExecuteQuery == "" {
		log.Logger.Error("Query to execute must be provided via -e or --execute")
		return fmt.Errorf("query to execute must be provided via -e or --execute")
	}

	if c.IncludeSlaves != "" && c.ExcludeSlaves != "" {
		log.Logger.Error("--include-slaves and --exclude-slaves are mutually exclusive.")
		return fmt.Errorf("--include-slaves and --exclude-slaves are mutually exclusive")
	}

	// P4: TiDB _tidb_rowid chunked DELETE. Owns its own routing/path, so it is
	// mutually exclusive with the covering/partition fast-path and skips PK/UK
	// resolution (forced chunking column would be silently ignored). Checked
	// before the fast-path block so the user gets the tidb-rowid-specific
	// message rather than a confusing "requires --select-order-by".
	if c.TiDBRowID {
		sqlType, err := ParseSQLType(c.ExecuteQuery)
		if err != nil {
			return fmt.Errorf("--tidb-rowid requires a parseable DELETE: %w", err)
		}
		if sqlType != SQLTypeDelete {
			return fmt.Errorf("--tidb-rowid chunks by _tidb_rowid and requires DELETE, got %s", sqlType)
		}
		if strings.TrimSpace(c.SelectOrderBy) != "" {
			return fmt.Errorf("--tidb-rowid is mutually exclusive with the --select-order-by covering fast-path")
		}
		if c.SelectCursor {
			return fmt.Errorf("--tidb-rowid is mutually exclusive with --select-cursor")
		}
		if strings.TrimSpace(c.SelectIndex) != "" {
			return fmt.Errorf("--tidb-rowid is mutually exclusive with --select-index")
		}
		if c.PartitionConcurrency > 1 {
			return fmt.Errorf("--tidb-rowid is mutually exclusive with --partition-concurrency")
		}
		if strings.TrimSpace(c.ForceChunkingColumn) != "" {
			return fmt.Errorf("--tidb-rowid skips key resolution; --force-chunking-column is not applicable")
		}
		if c.ChunkSize <= 0 {
			return fmt.Errorf("--tidb-rowid requires --chunk-size > 0")
		}
	}

	// P2: covering-index fast-path dependency / mutual-exclusion checks.
	fastPath := strings.TrimSpace(c.SelectOrderBy) != ""
	if fastPath {
		sqlType, err := ParseSQLType(c.ExecuteQuery)
		if err != nil {
			return fmt.Errorf("--select-order-by requires a parseable DELETE/UPDATE: %w", err)
		}
		if sqlType != SQLTypeDelete {
			return fmt.Errorf(
				"--select-order-by (covering index fast-path) requires DELETE, got %s",
				sqlType,
			)
		}
	}
	if c.SelectCursor && !fastPath {
		return fmt.Errorf("--select-cursor requires --select-order-by")
	}
	if strings.TrimSpace(c.SelectIndex) != "" && !fastPath {
		return fmt.Errorf("--select-index requires --select-order-by")
	}

	if c.MaxRows < 0 {
		return fmt.Errorf("--max-rows must be >= 0 (0=unlimited)")
	}
	if c.MaxDuration < 0 {
		return fmt.Errorf("--max-duration-ms must be >= 0 (0=unlimited)")
	}
	// P3: max-rows / max-duration are now enforced on both the range path and the
	// covering fast-path, so the earlier fast-path-only restriction is removed.

	// P3: partition concurrency is OceanBase-only and requires the covering
	// DELETE fast-path (--select-order-by). The authoritative "is the table
	// partitioned / is this OceanBase" check happens at runtime once a DB
	// connection exists; here we only validate the flag dependencies.
	if c.PartitionConcurrency < 0 {
		return fmt.Errorf("--partition-concurrency must be >= 0 (0/1=off)")
	}
	if c.PartitionConcurrency > 1 && !fastPath {
		return fmt.Errorf(
			"--partition-concurrency requires the covering-index fast-path " +
				"(--select-order-by, DELETE only)",
		)
	}

	if c.PreflightThreshold < 0 {
		return fmt.Errorf("--preflight-threshold must be >= 0 (0=default)")
	}
	if c.RowsPerSec < 0 {
		return fmt.Errorf("--rows-per-sec must be >= 0 (0=unlimited)")
	}

	return nil
}

// SQL operation types recognised by ParseSQLType.
const (
	SQLTypeDelete = "DELETE"
	SQLTypeUpdate = "UPDATE"
)

// ParseSQLType performs a lightweight prefix classification of the execute
// query into DELETE or UPDATE. It intentionally avoids a full parse (the
// Writer does the authoritative AST parse later); this is only used to gate the
// fast-path before a DB connection exists.
func ParseSQLType(query string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(upper, "DELETE"):
		return SQLTypeDelete, nil
	case strings.HasPrefix(upper, "UPDATE"):
		return SQLTypeUpdate, nil
	default:
		return "", fmt.Errorf("unsupported SQL type (expected UPDATE or DELETE)")
	}
}
