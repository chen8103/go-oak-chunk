package conf

import (
	"fmt"
	"os"

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

	//SkipLockTables      bool   `toml:"skip_lock_tables"`
	Database string `toml:"database"`
	TxnSize  int64  `toml:"txn_size"`
	Debug    bool   `toml:"debug_mode"`

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

	return nil
}
