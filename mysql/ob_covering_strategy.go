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

// OBCoveringOptions configures the covering-index two-phase fast path.
type OBCoveringOptions struct {
	// SelectIndex is the optional FORCE INDEX name for the candidate SELECT.
	SelectIndex string
	// SelectOrderBy is a comma-separated list of order columns. Non-empty when
	// the fast path is enabled.
	SelectOrderBy string
	// SelectCursor enables cursor advancement (avoids re-scan from the start).
	SelectCursor bool
	// DryRun, when true, makes Run print sample SQL instead of executing.
	DryRun bool
	// MaxRows stops the run once this many rows have been deleted (0=unlimited).
	MaxRows int64
	// MaxDuration stops the run after this wall-clock budget (0=unlimited).
	MaxDuration time.Duration
}

// OBCoveringStrategy implements the covering-index two-phase fast path.
//
// P2 scope: DELETE only, single worker (concurrency is P3). Each chunk is
// processed in two steps:
//  1. SELECT <order-cols>,<pk-cols> FROM <t> [FORCE INDEX(<select-index>)]
//     WHERE <frozen-where> [AND (<order-cols>,<pk>) > cursor]
//     ORDER BY <order-cols>,<pk> LIMIT <chunk-size>
//  2. DELETE FROM <t> WHERE <frozen-where> AND (<pk-cols>) IN (...)
//     in an independent short transaction, only after the SELECT succeeds.
//
// The cursor only advances after the DELETE commits. The frozen WHERE
// (Writer.OriginWhereClause) is always reused so the DELETE can never touch
// rows outside the original predicate.
type OBCoveringStrategy struct {
	writer *Writer

	selectIndex     string
	selectOrderCols []string
	selectCursor    bool
	dryRun          bool
	maxRows         int64
	maxDuration     time.Duration

	// cursorValues holds the full (order-cols + pk-cols) tuple of the last
	// committed row. It only advances after a successful DELETE.
	cursorValues []any
}

// NewOBCoveringStrategy creates a covering-index strategy from a Writer.
func NewOBCoveringStrategy(w *Writer, opts *OBCoveringOptions) *OBCoveringStrategy {
	if opts == nil {
		opts = &OBCoveringOptions{}
	}
	return &OBCoveringStrategy{
		writer:          w,
		selectIndex:     strings.TrimSpace(opts.SelectIndex),
		selectOrderCols: splitColumns(opts.SelectOrderBy),
		selectCursor:    opts.SelectCursor,
		dryRun:          opts.DryRun,
		maxRows:         opts.MaxRows,
		maxDuration:     opts.MaxDuration,
	}
}

// Name implements ChunkStrategy.Name.
func (ocs *OBCoveringStrategy) Name() string {
	return "OBCoveringStrategy"
}

// Run implements ChunkStrategy.Run.
//
// A producer goroutine runs the candidate-PK SELECT loop; the current goroutine
// consumes each batch and runs an independent short-transaction DELETE wrapped
// in retry.WithRetry. Counting/rate-limiting reuse the Writer infrastructure to
// stay consistent with RangeStrategy. The cursor only advances after a DELETE
// commits.
func (ocs *OBCoveringStrategy) Run(ctx context.Context, params RunParams) error {
	if ocs.dryRun {
		return ocs.PrintDryRunSample()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	startedAt := time.Now()

	// stopReached reports whether a max-rows / max-duration guardrail has been
	// hit. Single worker means these stop on an exact chunk boundary.
	stopReached := func() bool {
		if ocs.maxRows > 0 && ocs.writer.GetRowAffects() >= ocs.maxRows {
			log.Logger.Infof("max-rows reached (%d), stopping", ocs.maxRows)
			return true
		}
		if ocs.maxDuration > 0 && time.Since(startedAt) >= ocs.maxDuration {
			log.Logger.Infof("max-duration reached (%s), stopping", ocs.maxDuration)
			return true
		}
		return false
	}

	// The producer owns candidatesChan and closes it when paging finishes; its
	// terminal error is stored in producerErr (read after the channel drains).
	candidatesChan := make(chan []pkRow, 1)
	var producerErr error
	var producerDone sync.WaitGroup
	producerDone.Add(1)
	go func() {
		defer producerDone.Done()
		producerErr = ocs.selectCandidatePKLoop(runCtx, candidatesChan)
	}()

	// Consume every batch the producer emits. Looping over the channel until it
	// is closed guarantees all buffered pages are processed before we observe
	// the producer's terminal status, so no page is dropped on completion.
	for batch := range candidatesChan {
		// Drain the rate-limit signal channel the same way Writer.Write does.
		if ocs.applyRateLimit(runCtx, params) {
			// fall through: still process this batch after the lag pause.
		}

		select {
		case <-runCtx.Done():
			cancel()
			producerDone.Wait()
			ocs.finish()
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
				log.Logger.Debugf("retry covering delete batch (attempt %d)", attempt)
			}
			n, err := ocs.execDeletePKBatch(runCtx, batch)
			affected = n
			return err
		})
		if retryErr != nil {
			cancel()
			producerDone.Wait()
			return fmt.Errorf("delete pk batch failed: %w", retryErr)
		}

		// DELETE committed: advance cursor, update stats, throttle.
		if ocs.selectCursor {
			ocs.cursorValues = batch[len(batch)-1].full
		}
		ocs.writer.AddRowAffects(affected)
		ocs.writer.SetCostTime(time.Since(beginTime))
		params.Bucket.Wait(affected)

		if stopReached() {
			cancel()
			producerDone.Wait()
			ocs.finish()
			return nil
		}
	}

	producerDone.Wait()
	if producerErr != nil {
		return producerErr
	}
	ocs.finish()
	return nil
}

// applyRateLimit consumes the latest bucketNum signal and waits accordingly,
// mirroring Writer.Write. It returns true when the caller should continue the
// loop (slave-lag pause), false otherwise.
func (ocs *OBCoveringStrategy) applyRateLimit(ctx context.Context, params RunParams) bool {
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

func (ocs *OBCoveringStrategy) finish() {
	ocs.writer.SetFinished()
	log.Logger.Debug("OBCoveringStrategy is finished")
}

// pkRow is one candidate row: pk holds the PK column values (for the IN list),
// full holds the complete (order-cols + pk-cols) tuple used as the cursor.
type pkRow struct {
	pk   []any
	full []any
}

// selectCandidatePKLoop is the producer: each iteration SELECTs up to
// chunk-size candidate rows and sends them downstream. It stops when a batch
// comes back short (the table tail has been reached) or empty.
func (ocs *OBCoveringStrategy) selectCandidatePKLoop(ctx context.Context, out chan<- []pkRow) error {
	defer close(out)

	chunkSize := ocs.writer.ChunkSize

	// pageCursor is owned solely by this producer goroutine; it keeps SELECT
	// paging forward. The consumer-owned ocs.cursorValues tracks the committed
	// position separately, so the two goroutines never write the same field.
	var pageCursor []any
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		sqlText, args := ocs.buildSelectSQL(pageCursor)
		rows, err := ocs.writer.MysqlClient.QueryContext(ctx, sqlText, args...)
		if err != nil {
			if isContextDoneErr(err) {
				return nil
			}
			return fmt.Errorf("select candidate pks: %w", err)
		}

		batch, scanErr := ocs.scanPKBatch(rows)
		rows.Close()
		if scanErr != nil {
			if isContextDoneErr(scanErr) {
				return nil
			}
			return scanErr
		}

		if len(batch) > 0 {
			if ocs.selectCursor {
				pageCursor = batch[len(batch)-1].full
			}
			select {
			case <-ctx.Done():
				return nil
			case out <- batch:
			}
		}

		// A short page means we've reached the tail. chunkSize==0 is rejected by
		// PreCheck for the fast path, so it is always > 0 here.
		if int64(len(batch)) < chunkSize {
			return nil
		}
	}
}

// buildSelectSQL builds the candidate SELECT, appending the cursor predicate
// when enabled. Returned columns are ordered [order-cols..., pk-cols...] with
// duplicates removed so the scanned row maps cleanly to (order, pk).
func (ocs *OBCoveringStrategy) buildSelectSQL(cursor []any) (string, []any) {
	var sb strings.Builder

	selectCols := ocs.selectColumns()
	colList := quoteAndJoin(selectCols)
	ns := fmt.Sprintf("`%s`.`%s`", ocs.writer.Database, ocs.writer.Table)

	indexHint := ""
	if ocs.selectIndex != "" {
		indexHint = " FORCE INDEX (`" + ocs.selectIndex + "`)"
	}

	fmt.Fprintf(&sb, "SELECT %s FROM %s%s\n WHERE %s",
		colList, ns, indexHint, ocs.writer.OriginWhereClause)

	var args []any
	if ocs.selectCursor && len(cursor) > 0 {
		fmt.Fprintf(&sb, "\n   AND %s", ocs.buildCursorCondition(selectCols))
		args = append(args, cursor...)
	}

	fmt.Fprintf(&sb, "\n ORDER BY %s LIMIT %d",
		quoteAndJoinOrderBy(selectCols), ocs.writer.ChunkSize)

	return sb.String(), args
}

// selectColumns returns the deduplicated [order-cols..., pk-cols...] list. The
// SELECT/ORDER BY/cursor all use this exact column order.
func (ocs *OBCoveringStrategy) selectColumns() []string {
	cols := make([]string, 0, len(ocs.selectOrderCols)+len(ocs.writer.unqKeys.UniqueKeyColumns))
	seen := make(map[string]struct{})
	add := func(c string) {
		key := strings.ToLower(c)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		cols = append(cols, c)
	}
	for _, c := range ocs.selectOrderCols {
		add(c)
	}
	for _, c := range ocs.writer.unqKeys.UniqueKeyColumns {
		add(c)
	}
	return cols
}

// buildCursorCondition builds the multi-column tuple comparison, e.g.
// "(`created_at`,`id`) > (?,?)" over the given (deduplicated) columns.
func (ocs *OBCoveringStrategy) buildCursorCondition(cols []string) string {
	phs := make([]string, len(cols))
	for i := range phs {
		phs[i] = "?"
	}
	return fmt.Sprintf("(%s) > (%s)", quoteAndJoin(cols), strings.Join(phs, ","))
}

// scanPKBatch scans candidate rows into pkRow values. The full tuple keeps the
// SELECT column order; pk is the subset matching the unique-key columns.
func (ocs *OBCoveringStrategy) scanPKBatch(rows *sql.Rows) ([]pkRow, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	pkIndexes, err := ocs.findPKIndexes(cols)
	if err != nil {
		return nil, err
	}

	result := make([]pkRow, 0, ocs.writer.ChunkSize)
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(dest))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		pk := make([]any, len(pkIndexes))
		for i, idx := range pkIndexes {
			pk[i] = normalizeScanValue(dest[idx])
		}
		full := make([]any, len(dest))
		for i := range dest {
			full[i] = normalizeScanValue(dest[i])
		}
		result = append(result, pkRow{pk: pk, full: full})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// findPKIndexes locates each unique-key column within the SELECT result. It
// errors (rather than letting a -1 index panic later) when a PK column is
// absent, e.g. when the DB returns column names the lookup cannot match.
func (ocs *OBCoveringStrategy) findPKIndexes(cols []string) ([]int, error) {
	indexes := make([]int, len(ocs.writer.unqKeys.UniqueKeyColumns))
	for i, pk := range ocs.writer.unqKeys.UniqueKeyColumns {
		indexes[i] = -1
		for j, col := range cols {
			if strings.EqualFold(pk, col) {
				indexes[i] = j
				break
			}
		}
		if indexes[i] == -1 {
			return nil, fmt.Errorf(
				"primary key column %q not found in candidate SELECT result columns %v",
				pk, cols,
			)
		}
	}
	return indexes, nil
}

// execDeletePKBatch runs one short transaction deleting the batch by PK IN list.
func (ocs *OBCoveringStrategy) execDeletePKBatch(ctx context.Context, batch []pkRow) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	sqlText, args := ocs.buildDeleteSQL(batch)
	log.Logger.Debugf("covering delete sql: %s", sqlText)

	tx, err := ocs.writer.MysqlClient.BeginTx(ctx, nil)
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

// buildDeleteSQL builds "DELETE FROM <t> WHERE <frozen-where> AND (<pk>) IN (...)".
// Single-column PK uses a flat IN list; composite PK uses row-value tuples.
func (ocs *OBCoveringStrategy) buildDeleteSQL(batch []pkRow) (string, []any) {
	var sb strings.Builder

	ns := fmt.Sprintf("`%s`.`%s`", ocs.writer.Database, ocs.writer.Table)
	pkCols := ocs.writer.unqKeys.UniqueKeyColumns
	pkColList := quoteAndJoin(pkCols)

	var inClause string
	if len(pkCols) == 1 {
		phs := make([]string, len(batch))
		for i := range phs {
			phs[i] = "?"
		}
		inClause = "(" + strings.Join(phs, ",") + ")"
	} else {
		tuples := make([]string, len(batch))
		for i := range tuples {
			cols := make([]string, len(pkCols))
			for j := range cols {
				cols[j] = "?"
			}
			tuples[i] = "(" + strings.Join(cols, ",") + ")"
		}
		inClause = "(" + strings.Join(tuples, ",") + ")"
	}

	fmt.Fprintf(&sb, "DELETE FROM %s\n WHERE %s\n   AND (%s) IN %s",
		ns, ocs.writer.OriginWhereClause, pkColList, inClause)

	args := make([]any, 0, len(batch)*len(pkCols))
	for _, row := range batch {
		args = append(args, row.pk...)
	}
	return sb.String(), args
}

// PrintDryRunSample logs a sample SELECT and DELETE without executing them.
func (ocs *OBCoveringStrategy) PrintDryRunSample() error {
	selectSQL, selectArgs := ocs.buildSelectSQL(ocs.cursorValues)
	log.Logger.Infof("[DRY-RUN] sample SELECT:\n%s\nargs: %v", selectSQL, selectArgs)

	pkCols := ocs.writer.unqKeys.UniqueKeyColumns
	sample := make([]pkRow, 0, 3)
	for i := int64(1); i <= 3; i++ {
		pk := make([]any, len(pkCols))
		for j := range pk {
			pk[j] = i
		}
		sample = append(sample, pkRow{pk: pk})
	}
	deleteSQL, deleteArgs := ocs.buildDeleteSQL(sample)
	log.Logger.Infof("[DRY-RUN] sample DELETE (3 rows):\n%s\nargs: %v", deleteSQL, deleteArgs)
	return nil
}

// quoteAndJoin quotes and comma-joins column names, e.g. "`c1`,`c2`".
func quoteAndJoin(cols []string) string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = "`" + col + "`"
	}
	return strings.Join(quoted, ",")
}

// quoteAndJoinOrderBy quotes and joins columns as an ORDER BY clause (all ASC).
func quoteAndJoinOrderBy(cols []string) string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = "`" + col + "` ASC"
	}
	return strings.Join(quoted, ",")
}

// splitColumns splits a comma-separated column list, trimming blanks.
func splitColumns(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cols = append(cols, p)
		}
	}
	return cols
}

// normalizeScanValue converts driver-returned []byte into string so cursor
// values round-trip cleanly as bind parameters on the next SELECT.
func normalizeScanValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}
