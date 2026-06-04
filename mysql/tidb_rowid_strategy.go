package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/internal/retry"
	"github.com/SisyphusSQ/go-oak-chunk/v3/log"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

// TiDBRowIDOptions configures the TiDB `_tidb_rowid` chunked-DELETE strategy.
type TiDBRowIDOptions struct {
	// DryRun, when true, makes Run print sample SQL instead of executing.
	DryRun bool
	// MaxRows stops the run once this many rows have been deleted (0=unlimited).
	MaxRows int64
	// MaxDuration stops the run after this wall-clock budget (0=unlimited).
	MaxDuration time.Duration
}

// TiDBRowIDStrategy chunks a DELETE by TiDB's hidden `_tidb_rowid` row handle.
//
// 适用于 TiDB 非聚簇(NONCLUSTERED)表——它们带一个隐藏的 int64 行句柄
// `_tidb_rowid`, 可作分块键, 因此无需用户主键/唯一键。每个 chunk 分两步:
//  1. SELECT _tidb_rowid FROM <t> WHERE <frozen-where> [AND _tidb_rowid > ?]
//     ORDER BY _tidb_rowid LIMIT <chunk-size>
//  2. DELETE FROM <t> WHERE <frozen-where> AND _tidb_rowid IN (...)
//     在独立短事务里执行, 仅在 SELECT 成功后。
//
// 采用 seek 游标(WHERE _tidb_rowid > cursor)而非 MIN/MAX 算术步进, 因为
// SHARD_ROW_ID_BITS / AUTO_RANDOM 会把 rowid 散布到 int64 高位, 算术步进会
// 产生海量空范围。游标只在 DELETE 提交后前移; 冻结的 WHERE
// (Writer.OriginWhereClause) 始终复用, DELETE 绝不会触及谓词外的行。
type TiDBRowIDStrategy struct {
	writer *Writer

	dryRun      bool
	maxRows     int64
	maxDuration time.Duration
}

// NewTiDBRowIDStrategy creates a `_tidb_rowid` strategy from a Writer.
func NewTiDBRowIDStrategy(w *Writer, opts *TiDBRowIDOptions) *TiDBRowIDStrategy {
	if opts == nil {
		opts = &TiDBRowIDOptions{}
	}
	return &TiDBRowIDStrategy{
		writer:      w,
		dryRun:      opts.DryRun,
		maxRows:     opts.MaxRows,
		maxDuration: opts.MaxDuration,
	}
}

// Name implements ChunkStrategy.Name.
func (s *TiDBRowIDStrategy) Name() string {
	return "TiDBRowIDStrategy"
}

// Run implements ChunkStrategy.Run.
//
// A producer goroutine pages candidate `_tidb_rowid` values; the current
// goroutine consumes each batch and runs an independent short-transaction
// DELETE wrapped in retry.WithRetry. Counting / rate-limiting reuse the Writer
// infrastructure to stay consistent with the other strategies.
func (s *TiDBRowIDStrategy) Run(ctx context.Context, params RunParams) error {
	if s.dryRun {
		return s.PrintDryRunSample()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// _tidb_rowid 只在 NONCLUSTERED 表上存在; 聚簇表清晰报错而非静默回退。
	if err := s.ensureApplicable(runCtx); err != nil {
		return err
	}

	startedAt := time.Now()
	tableRef := s.defaultTableRef()

	// stopReached reports whether a max-rows / max-duration guardrail has been
	// hit. Single worker means these stop on an exact chunk boundary.
	stopReached := func() bool {
		if s.maxRows > 0 && s.writer.GetRowAffects() >= s.maxRows {
			log.Logger.Infof("max-rows reached (%d), stopping", s.maxRows)
			return true
		}
		if s.maxDuration > 0 && time.Since(startedAt) >= s.maxDuration {
			log.Logger.Infof("max-duration reached (%s), stopping", s.maxDuration)
			return true
		}
		return false
	}

	// The producer owns batchChan and closes it when paging finishes; its
	// terminal error is stored in producerErr (read after the channel drains).
	batchChan := make(chan []int64, 1)
	var producerErr error
	var producerDone sync.WaitGroup
	producerDone.Add(1)
	go func() {
		defer producerDone.Done()
		producerErr = s.selectRowIDLoop(runCtx, tableRef, batchChan)
	}()

	for batch := range batchChan {
		// A lag pause must not execute this batch until the loop has re-checked
		// the latest throttle signal; keep the batch in hand and retry.
		for s.applyRateLimit(runCtx, params) {
			select {
			case <-runCtx.Done():
				cancel()
				producerDone.Wait()
				s.finish()
				return nil
			default:
			}
		}

		select {
		case <-runCtx.Done():
			cancel()
			producerDone.Wait()
			s.finish()
			return nil
		default:
		}

		if len(batch) == 0 {
			continue
		}

		beginTime := time.Now()
		var affected int64
		retryErr := retry.WithRetry(runCtx, retry.DefaultPolicy(), func(attempt int) error {
			if attempt > 1 {
				log.Logger.Debugf("retry rowid delete batch (attempt %d)", attempt)
			}
			n, err := s.execDeleteRowIDBatch(runCtx, tableRef, batch)
			affected = n
			return err
		})
		if retryErr != nil {
			cancel()
			producerDone.Wait()
			return fmt.Errorf("delete rowid batch failed: %w", retryErr)
		}

		// DELETE committed: update stats, throttle.
		s.writer.AddRowAffects(affected)
		s.writer.SetCostTime(time.Since(beginTime))

		// 行级限流: 按本批次实际删除行数等待。ctx 取消时按 clean-stop 收尾。
		// 注意: sleep/lag 节流已在循环顶部 applyRateLimit 里按 bucketCount 消费,
		// 这里不能再用行数 affected 去 Bucket.Wait(桶 1 token=1ms)。
		if params.RowsLimiter != nil {
			if err := params.RowsLimiter.Wait(runCtx, affected); err != nil {
				cancel()
				producerDone.Wait()
				s.finish()
				return nil
			}
		}

		if stopReached() {
			cancel()
			producerDone.Wait()
			s.finish()
			return nil
		}
	}

	producerDone.Wait()
	if producerErr != nil {
		return producerErr
	}
	s.finish()
	return nil
}

// applyRateLimit consumes the latest bucketNum signal and waits accordingly,
// mirroring Writer.Write / OBCoveringStrategy. It returns true when the caller
// should continue the loop (slave-lag pause), false otherwise.
func (s *TiDBRowIDStrategy) applyRateLimit(ctx context.Context, params RunParams) bool {
	var bucketCount int64
	for i := 0; i < len(params.BucketNum); i++ {
		select {
		case bucketCount = <-params.BucketNum:
		default:
		}
	}
	if bucketCount == vars.LagThreshold {
		log.Logger.Debug("Sleep 1s to let slave eliminate lag")
		params.Bucket.Wait(1000)
		return true
	}
	if bucketCount > 0 {
		params.Bucket.Wait(bucketCount)
	}
	return false
}

func (s *TiDBRowIDStrategy) finish() {
	s.writer.SetFinished()
	log.Logger.Debug("TiDBRowIDStrategy is finished")
}

// ensureApplicable verifies the table exposes `_tidb_rowid`. It prefers the
// authoritative information_schema.tables.TIDB_PK_TYPE column (NONCLUSTERED =
// has rowid; CLUSTERED = none) and falls back to a probe SELECT for TiDB
// versions that don't expose that column.
func (s *TiDBRowIDStrategy) ensureApplicable(ctx context.Context) error {
	var pkType string
	err := s.writer.MysqlClient.QueryRowContext(
		ctx, vars.TiDBPKTypeSQL, s.writer.Database, s.writer.Table,
	).Scan(&pkType)
	switch {
	case err == nil:
		switch strings.ToUpper(strings.TrimSpace(pkType)) {
		case "CLUSTERED":
			return fmt.Errorf(
				"table `%s`.`%s` is a CLUSTERED TiDB table and has no _tidb_rowid; "+
					"remove --tidb-rowid (use a PK-based strategy instead)",
				s.writer.Database, s.writer.Table,
			)
		case "NONCLUSTERED":
			return nil
		}
		// Unknown value: fall through to the probe.
	case err == sql.ErrNoRows:
		// Table not visible via information_schema here; fall through to probe.
	default:
		log.Logger.Debugf("TIDB_PK_TYPE lookup failed, probing _tidb_rowid: %v", err)
	}

	probe := fmt.Sprintf(vars.TiDBRowIDProbeSQL, s.defaultTableRef())
	rows, perr := s.writer.MysqlClient.QueryContext(ctx, probe)
	if perr != nil {
		return fmt.Errorf(
			"table `%s`.`%s` does not expose _tidb_rowid (not a NONCLUSTERED TiDB table?): %w",
			s.writer.Database, s.writer.Table, perr,
		)
	}
	_ = rows.Close()
	return nil
}

// selectRowIDLoop is the producer: each iteration SELECTs up to chunk-size
// candidate `_tidb_rowid` values and sends them downstream. It stops when a
// batch comes back short (the table tail has been reached) or empty.
func (s *TiDBRowIDStrategy) selectRowIDLoop(ctx context.Context, tableRef string, out chan<- []int64) error {
	defer close(out)

	chunkSize := s.writer.ChunkSize

	// pageCursor / started are owned solely by this producer goroutine. started
	// distinguishes "no cursor yet" from "cursor == 0" so a legitimate small or
	// negative first rowid is not skipped.
	var pageCursor int64
	started := false
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		sqlText, args := s.buildSelectSQL(tableRef, pageCursor, started)
		rows, err := s.writer.MysqlClient.QueryContext(ctx, sqlText, args...)
		if err != nil {
			if isContextDoneErr(err) {
				return nil
			}
			return fmt.Errorf("select _tidb_rowid: %w", err)
		}

		batch, scanErr := s.scanRowIDBatch(rows)
		rows.Close()
		if scanErr != nil {
			if isContextDoneErr(scanErr) {
				return nil
			}
			return scanErr
		}

		if len(batch) > 0 {
			pageCursor = batch[len(batch)-1]
			started = true
			select {
			case <-ctx.Done():
				return nil
			case out <- batch:
			}
		}

		// A short page means we've reached the tail. chunkSize is validated > 0
		// by PreCheck for the rowid mode, so it is always > 0 here.
		if int64(len(batch)) < chunkSize {
			return nil
		}
	}
}

// scanRowIDBatch scans the single-column `_tidb_rowid` result into int64 values.
func (s *TiDBRowIDStrategy) scanRowIDBatch(rows *sql.Rows) ([]int64, error) {
	result := make([]int64, 0, s.writer.ChunkSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// buildSelectSQL builds the candidate SELECT, appending the cursor predicate
// when started.
func (s *TiDBRowIDStrategy) buildSelectSQL(tableRef string, cursor int64, started bool) (string, []any) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT _tidb_rowid FROM %s\n WHERE %s", tableRef, s.whereClause())

	var args []any
	if started {
		sb.WriteString("\n   AND _tidb_rowid > ?")
		args = append(args, cursor)
	}

	fmt.Fprintf(&sb, "\n ORDER BY _tidb_rowid LIMIT %d", s.writer.ChunkSize)
	return sb.String(), args
}

// buildDeleteSQL builds "DELETE FROM <t> WHERE <frozen-where> AND _tidb_rowid IN (...)".
// The IN list pins exactly the handles the producer saw, so a row inserted
// between SELECT and DELETE can never be touched.
func (s *TiDBRowIDStrategy) buildDeleteSQL(tableRef string, batch []int64) (string, []any) {
	phs := make([]string, len(batch))
	args := make([]any, len(batch))
	for i, id := range batch {
		phs[i] = "?"
		args[i] = id
	}
	sqlText := fmt.Sprintf(
		"DELETE FROM %s\n WHERE %s\n   AND _tidb_rowid IN (%s)",
		tableRef, s.whereClause(), strings.Join(phs, ","),
	)
	return sqlText, args
}

// execDeleteRowIDBatch runs one short transaction deleting the batch by rowid IN list.
func (s *TiDBRowIDStrategy) execDeleteRowIDBatch(ctx context.Context, tableRef string, batch []int64) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	sqlText, args := s.buildDeleteSQL(tableRef, batch)
	log.Logger.Debugf("rowid delete sql: %s", sqlText)

	tx, err := s.writer.MysqlClient.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	res, err := tx.ExecContext(ctx, sqlText, args...)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("commit: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

// whereClause returns the frozen WHERE predicate, or "1=1" when the DELETE has
// no WHERE (whole-table purge).
func (s *TiDBRowIDStrategy) whereClause() string {
	if strings.TrimSpace(s.writer.OriginWhereClause) == "" {
		return "1=1"
	}
	return s.writer.OriginWhereClause
}

// defaultTableRef returns "`db`.`table`".
func (s *TiDBRowIDStrategy) defaultTableRef() string {
	return fmt.Sprintf("`%s`.`%s`", s.writer.Database, s.writer.Table)
}

// PrintDryRunSample logs a sample SELECT and DELETE without executing them.
func (s *TiDBRowIDStrategy) PrintDryRunSample() error {
	tableRef := s.defaultTableRef()
	selectSQL, selectArgs := s.buildSelectSQL(tableRef, 0, false)
	log.Logger.Infof("[DRY-RUN] sample rowid SELECT:\n%s\nargs: %v", selectSQL, selectArgs)

	deleteSQL, deleteArgs := s.buildDeleteSQL(tableRef, []int64{1, 2, 3})
	log.Logger.Infof("[DRY-RUN] sample rowid DELETE (3 rows):\n%s\nargs: %v", deleteSQL, deleteArgs)
	return nil
}
