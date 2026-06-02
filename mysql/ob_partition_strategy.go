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

// partitionHardMax 限制并发 worker 数的硬上限, 防止配置过大压垮连接池/DB。
const partitionHardMax = 64

// OBPartitionOptions configures the OceanBase partition-parallel covering DELETE.
type OBPartitionOptions struct {
	SelectIndex   string
	SelectOrderBy string
	SelectCursor  bool
	DryRun        bool
	MaxRows       int64
	MaxDuration   time.Duration
	Concurrency   int
}

// OBPartitionStrategy runs the covering-index two-phase DELETE concurrently
// across OceanBase table partitions. Each worker owns exactly one partition
// end-to-end (SELECT candidate PKs + DELETE ... PK IN scoped with
// "PARTITION (<name>)") and its own cursor; the rate limiter (juju Bucket +
// bucketNum lag signal) and the rows-per-sec limiter are shared, so throttling
// is enforced globally across all workers.
type OBPartitionStrategy struct {
	writer *Writer

	selectIndex     string
	selectOrderCols []string
	selectCursor    bool
	dryRun          bool
	maxRows         int64
	maxDuration     time.Duration
	concurrency     int
}

// NewOBPartitionStrategy creates a partition-parallel strategy from a Writer.
func NewOBPartitionStrategy(w *Writer, opts *OBPartitionOptions) *OBPartitionStrategy {
	if opts == nil {
		opts = &OBPartitionOptions{}
	}
	return &OBPartitionStrategy{
		writer:          w,
		selectIndex:     strings.TrimSpace(opts.SelectIndex),
		selectOrderCols: splitColumns(opts.SelectOrderBy),
		selectCursor:    opts.SelectCursor,
		dryRun:          opts.DryRun,
		maxRows:         opts.MaxRows,
		maxDuration:     opts.MaxDuration,
		concurrency:     opts.Concurrency,
	}
}

// Name implements ChunkStrategy.Name.
func (ps *OBPartitionStrategy) Name() string {
	return "OBPartitionStrategy"
}

// newCovering builds a per-call OBCoveringStrategy that shares this strategy's
// writer + options. It is used both for the SQL builders (stateless) and as the
// per-worker covering core. Each worker gets its own instance so its cursor
// never aliases another worker's.
func (ps *OBPartitionStrategy) newCovering() *OBCoveringStrategy {
	return &OBCoveringStrategy{
		writer:          ps.writer,
		selectIndex:     ps.selectIndex,
		selectOrderCols: ps.selectOrderCols,
		selectCursor:    ps.selectCursor,
		maxRows:         ps.maxRows,
		maxDuration:     ps.maxDuration,
	}
}

// partitionTableRef returns "`db`.`table` PARTITION (`name`)".
func (ps *OBPartitionStrategy) partitionTableRef(name string) string {
	q := func(s string) string { return strings.ReplaceAll(s, "`", "``") }
	return fmt.Sprintf("`%s`.`%s` PARTITION (`%s`)", q(ps.writer.Database), q(ps.writer.Table), q(name))
}

// Run implements ChunkStrategy.Run.
func (ps *OBPartitionStrategy) Run(ctx context.Context, params RunParams) error {
	partitions, err := discoverPartitions(ctx, ps.writer.MysqlClient, ps.writer.Database, ps.writer.Table)
	if err != nil {
		return fmt.Errorf("discover partitions: %w", err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf(
			"table `%s`.`%s` has no partitions; drop --partition-concurrency to use the single-worker covering path",
			ps.writer.Database, ps.writer.Table,
		)
	}

	if ps.dryRun {
		// Print a single partition-scoped sample, do not spawn workers.
		cov := ps.newCovering()
		tableRef := ps.partitionTableRef(partitions[0])
		selectSQL, selectArgs := cov.buildSelectSQL(tableRef, nil)
		log.Logger.Infof("[DRY-RUN] sample partition SELECT:\n%s\nargs: %v", selectSQL, selectArgs)
		sample := make([]pkRow, 0, 3)
		pkCols := ps.writer.unqKeys.UniqueKeyColumns
		for i := int64(1); i <= 3; i++ {
			pk := make([]any, len(pkCols))
			for j := range pk {
				pk[j] = i
			}
			sample = append(sample, pkRow{pk: pk})
		}
		deleteSQL, deleteArgs := cov.buildDeleteSQL(tableRef, sample)
		log.Logger.Infof("[DRY-RUN] sample partition DELETE (3 rows):\n%s\nargs: %v", deleteSQL, deleteArgs)
		return nil
	}

	workers := ps.workerCount(len(partitions))
	log.Logger.Infof("OBPartitionStrategy: %d partitions, %d workers", len(partitions), workers)

	state := &partitionRunState{started: time.Now(), stopReason: "drained"}

	// gctx is cancelled by the first worker error (errgroup semantics, without
	// the extra dependency). errOnce guards the single captured error.
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	// partCh feeds partition names to the worker pool; a feeder closes it.
	partCh := make(chan string)
	go func() {
		defer close(partCh)
		for _, p := range partitions {
			select {
			case <-gctx.Done():
				return
			case partCh <- p:
			}
		}
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cov := ps.newCovering()
			for {
				select {
				case <-gctx.Done():
					return
				case name, ok := <-partCh:
					if !ok {
						return
					}
					if state.shouldStop(gctx, ps.maxRows, ps.maxDuration) {
						return
					}
					if err := ps.runPartitionWorker(gctx, params, cov, name, state); err != nil {
						fail(err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		// Context cancellation is a clean stop (SIGINT / guardrail), mirroring
		// the covering strategy.
		if isContextDoneErr(firstErr) {
			ps.finish()
			return nil
		}
		return firstErr
	}

	log.Logger.Infof("OBPartitionStrategy finished (reason=%s)", state.stopReason)
	ps.finish()
	return nil
}

func (ps *OBPartitionStrategy) finish() {
	ps.writer.SetFinished()
	log.Logger.Debug("OBPartitionStrategy is finished")
}

// workerCount clamps the configured concurrency to the partition count and the
// hard max, with a floor of 1.
func (ps *OBPartitionStrategy) workerCount(numPartitions int) int {
	n := ps.concurrency
	if n < 1 {
		n = 1
	}
	if n > numPartitions {
		n = numPartitions
	}
	if n > partitionHardMax {
		n = partitionHardMax
	}
	return n
}

// runPartitionWorker pages one partition end-to-end with its own cursor. It
// reuses the covering builders/scanners with a partition-scoped tableRef. The
// shared state + writer counters guard the global guardrails and stats.
func (ps *OBPartitionStrategy) runPartitionWorker(
	ctx context.Context,
	params RunParams,
	cov *OBCoveringStrategy,
	partition string,
	state *partitionRunState,
) error {
	tableRef := ps.partitionTableRef(partition)
	chunkSize := ps.writer.ChunkSize

	// cursor is owned solely by this worker; it never aliases another worker's.
	var cursor []any
	for {
		if state.shouldStop(ctx, ps.maxRows, ps.maxDuration) {
			return nil
		}

		// Drain the lag signal and throttle (shared bucket = global rate).
		cov.applyRateLimit(ctx, params)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		sqlText, args := cov.buildSelectSQL(tableRef, cursor)
		rows, err := ps.writer.MysqlClient.QueryContext(ctx, sqlText, args...)
		if err != nil {
			if isContextDoneErr(err) {
				return nil
			}
			return fmt.Errorf("select candidate pks (partition %s): %w", partition, err)
		}
		batch, scanErr := cov.scanPKBatch(rows)
		rows.Close()
		if scanErr != nil {
			if isContextDoneErr(scanErr) {
				return nil
			}
			return scanErr
		}

		if len(batch) == 0 {
			return nil
		}

		beginTime := time.Now()
		var affected int64
		retryErr := retry.WithRetry(ctx, retry.DefaultPolicy(), func(attempt int) error {
			if attempt > 1 {
				log.Logger.Debugf("retry covering delete batch (partition %s, attempt %d)", partition, attempt)
			}
			n, derr := cov.execDeletePKBatch(ctx, tableRef, batch)
			affected = n
			return derr
		})
		if retryErr != nil {
			if isContextDoneErr(retryErr) {
				return nil
			}
			return fmt.Errorf("delete pk batch failed (partition %s): %w", partition, retryErr)
		}

		// DELETE committed: advance cursor (copy to avoid aliasing the scan
		// buffer), update stats, throttle.
		if ps.selectCursor {
			cursor = cloneAny(batch[len(batch)-1].full)
		}
		state.addRows(ps.writer, affected)
		ps.writer.SetCostTime(time.Since(beginTime))
		params.Bucket.Wait(affected)

		if params.RowsLimiter != nil {
			if err := params.RowsLimiter.Wait(ctx, affected); err != nil {
				return nil
			}
		}

		// A short page means this partition's tail has been reached.
		if int64(len(batch)) < chunkSize {
			return nil
		}
	}
}

// cloneAny returns a shallow copy of the slice so a stored cursor does not alias
// the producer's scan buffer.
func cloneAny(src []any) []any {
	if src == nil {
		return nil
	}
	dst := make([]any, len(src))
	copy(dst, src)
	return dst
}

// partitionRunState is the mutex-guarded run state shared by all workers.
type partitionRunState struct {
	mu         sync.Mutex
	started    time.Time
	rows       int64 // mirror of writer.GetRowAffects() under lock
	stopReason string
}

// shouldStop reports whether a guardrail (ctx cancel / max-rows / max-duration)
// has been hit. It records the stop reason once.
func (s *partitionRunState) shouldStop(ctx context.Context, maxRows int64, maxDur time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if maxRows > 0 && s.rows >= maxRows {
		s.setStopLocked("max-rows")
		return true
	}
	if maxDur > 0 && time.Since(s.started) >= maxDur {
		s.setStopLocked("max-duration")
		return true
	}
	return false
}

// addRows updates the writer atomic counter and the locked mirror used by
// shouldStop. mu is NOT held while calling the limiter elsewhere.
func (s *partitionRunState) addRows(w *Writer, n int64) {
	w.AddRowAffects(n)
	s.mu.Lock()
	s.rows += n
	s.mu.Unlock()
}

// setStopLocked overwrites the default "drained" reason only once; callers must
// hold s.mu.
func (s *partitionRunState) setStopLocked(reason string) {
	if s.stopReason == "" || s.stopReason == "drained" {
		s.stopReason = reason
	}
}

// discoverPartitions returns the partition names of a table in ordinal order.
// A non-partitioned table returns a zero-length slice (the caller decides
// whether that is an error).
func discoverPartitions(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, vars.PartitionsSQL, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		partitions = append(partitions, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return partitions, nil
}
