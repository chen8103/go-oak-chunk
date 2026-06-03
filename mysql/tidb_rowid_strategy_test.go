package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

func newRowIDStrategyForTest() *TiDBRowIDStrategy {
	return &TiDBRowIDStrategy{
		writer: &Writer{
			Database:          "test_db",
			Table:             "test_tbl",
			OriginWhereClause: "(status = 'active')",
			ChunkSize:         1000,
		},
	}
}

func TestTiDBRowID_Name(t *testing.T) {
	if got := newRowIDStrategyForTest().Name(); got != "TiDBRowIDStrategy" {
		t.Fatalf("Name = %s", got)
	}
}

func TestTiDBRowID_BuildSelectSQL(t *testing.T) {
	s := newRowIDStrategyForTest()
	ref := s.defaultTableRef()

	// no cursor (not started)
	sqlText, args := s.buildSelectSQL(ref, 0, false)
	for _, want := range []string{
		"SELECT _tidb_rowid FROM `test_db`.`test_tbl`",
		"WHERE (status = 'active')",
		"ORDER BY _tidb_rowid LIMIT 1000",
	} {
		if !strings.Contains(sqlText, want) {
			t.Errorf("SQL missing %q\ngot: %s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, "_tidb_rowid > ?") {
		t.Errorf("unstarted SELECT must not have a cursor predicate\ngot: %s", sqlText)
	}
	if len(args) != 0 {
		t.Errorf("args = %d, want 0", len(args))
	}

	// with cursor (started)
	sqlText, args = s.buildSelectSQL(ref, 42, true)
	if !strings.Contains(sqlText, "AND _tidb_rowid > ?") {
		t.Errorf("started SELECT must have a cursor predicate\ngot: %s", sqlText)
	}
	if len(args) != 1 || args[0] != int64(42) {
		t.Errorf("args = %v, want [42]", args)
	}
}

func TestTiDBRowID_BuildDeleteSQL(t *testing.T) {
	s := newRowIDStrategyForTest()
	sqlText, args := s.buildDeleteSQL(s.defaultTableRef(), []int64{1, 2, 3})

	if !strings.Contains(sqlText, "DELETE FROM `test_db`.`test_tbl`") {
		t.Errorf("DELETE target missing\ngot: %s", sqlText)
	}
	// The frozen WHERE must always be re-applied.
	if !strings.Contains(sqlText, "WHERE (status = 'active')\n   AND _tidb_rowid IN (?,?,?)") {
		t.Errorf("frozen WHERE + IN list missing\ngot: %s", sqlText)
	}
	want := []int64{1, 2, 3}
	if len(args) != len(want) {
		t.Fatalf("args count = %d, want %d", len(args), len(want))
	}
	for i := range args {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %v, want %v", i, args[i], want[i])
		}
	}
}

func TestTiDBRowID_WhereClauseEmptyFallback(t *testing.T) {
	s := newRowIDStrategyForTest()
	s.writer.OriginWhereClause = ""
	sqlText, _ := s.buildDeleteSQL(s.defaultTableRef(), []int64{1})
	if !strings.Contains(sqlText, "WHERE 1=1") {
		t.Fatalf("empty WHERE should fall back to 1=1\ngot: %s", sqlText)
	}
}

func TestTiDBRowID_DryRunSampleBuilds(t *testing.T) {
	if err := newRowIDStrategyForTest().PrintDryRunSample(); err != nil {
		t.Fatalf("PrintDryRunSample failed: %v", err)
	}
}

// ---- Run / producer integration via a small fake driver ----

type rowidFakeDriverState struct {
	mu          sync.Mutex
	pkType      string    // value returned for TIDB_PK_TYPE; "" => no row (probe path)
	selectPages [][]int64 // each page is a slice of _tidb_rowid values
	pageIdx     int
	deleteCalls int
	deletedRows int
}

type rowidFakeDriver struct{ state *rowidFakeDriverState }
type rowidFakeConn struct{ state *rowidFakeDriverState }
type rowidFakeTx struct{ state *rowidFakeDriverState }
type rowidFakeRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

var rowidDriverSeq atomic.Int64

func (d *rowidFakeDriver) Open(_ string) (driver.Conn, error) {
	return &rowidFakeConn{state: d.state}, nil
}
func (c *rowidFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not implemented")
}
func (c *rowidFakeConn) Close() error { return nil }
func (c *rowidFakeConn) Begin() (driver.Tx, error) {
	return &rowidFakeTx{state: c.state}, nil
}
func (c *rowidFakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &rowidFakeTx{state: c.state}, nil
}

func (c *rowidFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	switch {
	case strings.Contains(query, "TIDB_PK_TYPE"):
		var rows [][]driver.Value
		if c.state.pkType != "" {
			rows = [][]driver.Value{{c.state.pkType}}
		}
		return &rowidFakeRows{columns: []string{"TIDB_PK_TYPE"}, rows: rows}, nil
	case strings.Contains(query, "ORDER BY _tidb_rowid"):
		var page []int64
		if c.state.pageIdx < len(c.state.selectPages) {
			page = c.state.selectPages[c.state.pageIdx]
			c.state.pageIdx++
		}
		rows := make([][]driver.Value, len(page))
		for i, id := range page {
			rows[i] = []driver.Value{id}
		}
		return &rowidFakeRows{columns: []string{"_tidb_rowid"}, rows: rows}, nil
	case strings.Contains(query, "_tidb_rowid"):
		// probe SELECT _tidb_rowid ... LIMIT 1
		return &rowidFakeRows{columns: []string{"_tidb_rowid"}, rows: nil}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (c *rowidFakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.Contains(strings.ToUpper(query), "DELETE") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.deleteCalls++
	c.state.deletedRows += len(args)
	return driver.RowsAffected(int64(len(args))), nil
}

func (tx *rowidFakeTx) Commit() error   { return nil }
func (tx *rowidFakeTx) Rollback() error { return nil }

func (r *rowidFakeRows) Columns() []string { return r.columns }
func (r *rowidFakeRows) Close() error      { return nil }
func (r *rowidFakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.idx]
	r.idx++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		} else {
			dest[i] = nil
		}
	}
	return nil
}

func newRowIDTestDB(t *testing.T, state *rowidFakeDriverState) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("rowid_fake_driver_%d", rowidDriverSeq.Add(1))
	sql.Register(name, &rowidFakeDriver{state: state})
	db, err := sql.Open(name, "rowid-test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newRunnableRowIDStrategy(t *testing.T, state *rowidFakeDriverState, chunkSize, maxRows int64) *TiDBRowIDStrategy {
	t.Helper()
	return &TiDBRowIDStrategy{
		writer: &Writer{
			MysqlClient:       newRowIDTestDB(t, state),
			Database:          "test_db",
			Table:             "test_tbl",
			OriginWhereClause: "(status = 'active')",
			ChunkSize:         chunkSize,
		},
		maxRows: maxRows,
	}
}

func runRowIDStrategy(t *testing.T, s *TiDBRowIDStrategy, params RunParams) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(context.Background(), params) }()
	select {
	case err := <-errCh:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out")
		return nil
	}
}

func bucketNumZero() <-chan int64 {
	ch := make(chan int64, 1)
	ch <- 0
	return ch
}

func rowIDDeletes(state *rowidFakeDriverState) int {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.deleteCalls
}

func TestTiDBRowID_Run_LagSignalRecheckedBeforeDelete(t *testing.T) {
	state := &rowidFakeDriverState{
		pkType:      "NONCLUSTERED",
		selectPages: [][]int64{{1, 2}},
	}
	s := newRunnableRowIDStrategy(t, state, 3, 0)

	bucket := newGateBucket()
	bucketNum := make(chan int64, 2)
	bucketNum <- vars.LagThreshold

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Run(context.Background(), RunParams{Bucket: bucket, BucketNum: bucketNum})
	}()

	if got := bucket.waitForWait(t); got != 1000 {
		t.Fatalf("first lag wait = %d, want 1000", got)
	}
	if got := rowIDDeletes(state); got != 0 {
		t.Fatalf("delete calls during first lag wait = %d, want 0", got)
	}

	bucketNum <- vars.LagThreshold
	bucket.release()
	if got := bucket.waitForWait(t); got != 1000 {
		t.Fatalf("second lag wait = %d, want 1000", got)
	}
	if got := rowIDDeletes(state); got != 0 {
		t.Fatalf("delete calls before lag recheck completed = %d, want 0", got)
	}

	bucket.release()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out")
	}
	if got := rowIDDeletes(state); got != 1 {
		t.Fatalf("delete calls after lag clears = %d, want 1", got)
	}
}

func TestTiDBRowID_Run_PagesAndStop(t *testing.T) {
	state := &rowidFakeDriverState{pkType: "NONCLUSTERED", selectPages: [][]int64{{1, 2}, {3}}}
	s := newRunnableRowIDStrategy(t, state, 2, 0)

	if err := runRowIDStrategy(t, s, RunParams{Bucket: noopBucket{}, BucketNum: bucketNumZero()}); err != nil {
		t.Fatalf("Run err: %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 2 {
		t.Errorf("delete calls = %d, want 2", state.deleteCalls)
	}
	if state.deletedRows != 3 {
		t.Errorf("deleted rows = %d, want 3", state.deletedRows)
	}
	if got := s.writer.GetRowAffects(); got != 3 {
		t.Errorf("rowAffects = %d, want 3", got)
	}
}

func TestTiDBRowID_Run_ClusteredErrors(t *testing.T) {
	state := &rowidFakeDriverState{pkType: "CLUSTERED", selectPages: [][]int64{{1}}}
	s := newRunnableRowIDStrategy(t, state, 2, 0)

	err := runRowIDStrategy(t, s, RunParams{Bucket: noopBucket{}, BucketNum: bucketNumZero()})
	if err == nil || !strings.Contains(err.Error(), "CLUSTERED") {
		t.Fatalf("want CLUSTERED error, got: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 0 {
		t.Errorf("clustered table must not delete, calls = %d", state.deleteCalls)
	}
}

func TestTiDBRowID_Run_ProbeFallback(t *testing.T) {
	// pkType "" => TIDB_PK_TYPE returns no row => probe path => usable.
	state := &rowidFakeDriverState{pkType: "", selectPages: [][]int64{{1, 2}, {3}}}
	s := newRunnableRowIDStrategy(t, state, 2, 0)

	if err := runRowIDStrategy(t, s, RunParams{Bucket: noopBucket{}, BucketNum: bucketNumZero()}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deletedRows != 3 {
		t.Errorf("deleted rows = %d, want 3 (probe path)", state.deletedRows)
	}
}

func TestTiDBRowID_Run_MaxRowsStops(t *testing.T) {
	state := &rowidFakeDriverState{pkType: "NONCLUSTERED", selectPages: [][]int64{{1, 2}, {3, 4}, {5, 6}}}
	s := newRunnableRowIDStrategy(t, state, 2, 2) // maxRows=2

	if err := runRowIDStrategy(t, s, RunParams{Bucket: noopBucket{}, BucketNum: bucketNumZero()}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 1 {
		t.Errorf("delete calls = %d, want 1 (max-rows stop after first chunk)", state.deleteCalls)
	}
	if got := s.writer.GetRowAffects(); got != 2 {
		t.Errorf("rowAffects = %d, want 2", got)
	}
}

func TestTiDBRowID_Run_CursorAdvances(t *testing.T) {
	// Capture the cursor arg the producer binds on each SELECT page.
	state := &rowidFakeDriverState{pkType: "NONCLUSTERED", selectPages: [][]int64{{10, 20}, {30}}}
	s := newRunnableRowIDStrategy(t, state, 2, 0)
	if err := runRowIDStrategy(t, s, RunParams{Bucket: noopBucket{}, BucketNum: bucketNumZero()}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	// Two SELECT pages consumed: page1 full (2 rows == chunkSize), page2 short.
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pageIdx != 2 {
		t.Errorf("pageIdx = %d, want 2 (full page then short page terminates)", state.pageIdx)
	}
}

func TestTiDBRowID_Run_DryRunNoDelete(t *testing.T) {
	state := &rowidFakeDriverState{pkType: "NONCLUSTERED", selectPages: [][]int64{{1, 2}}}
	s := newRunnableRowIDStrategy(t, state, 2, 0)
	s.dryRun = true

	if err := runRowIDStrategy(t, s, RunParams{Bucket: noopBucket{}, BucketNum: bucketNumZero()}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 0 {
		t.Errorf("dry-run must not delete, calls = %d", state.deleteCalls)
	}
}

func TestTiDBRowID_Run_NoRowCountThrottle(t *testing.T) {
	// sleep/rows-per-sec off: the strategy must never feed the row count into
	// Bucket.Wait (guards the covering/partition throughput-bug regression).
	state := &rowidFakeDriverState{pkType: "NONCLUSTERED", selectPages: [][]int64{{1, 2}, {3}}}
	s := newRunnableRowIDStrategy(t, state, 2, 0)

	bucket := &countingBucket{}
	if err := runRowIDStrategy(t, s, RunParams{Bucket: bucket, BucketNum: bucketNumZero()}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if bucket.total != 0 {
		t.Errorf("bucket waited %d tokens, want 0", bucket.total)
	}
}
