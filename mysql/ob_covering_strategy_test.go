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

	"github.com/juju/ratelimit"

	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

func newCoveringStrategyForTest(pkCols, orderCols []string, cursor bool, cursorVals []any) *OBCoveringStrategy {
	return &OBCoveringStrategy{
		writer: &Writer{
			Database:          "test_db",
			Table:             "test_tbl",
			OriginWhereClause: "(status = 'active')",
			ChunkSize:         1000,
			unqKeys:           &UnqKeys{UniqueKeyColumns: pkCols},
		},
		selectOrderCols: orderCols,
		selectCursor:    cursor,
		cursorValues:    cursorVals,
	}
}

func TestOBCoveringStrategy_BuildSelectSQL(t *testing.T) {
	tests := []struct {
		name         string
		orderCols    []string
		pkCols       []string
		cursor       bool
		cursorValues []any
		wantContains []string
		wantArgs     int
	}{
		{
			name:         "single order col single pk no cursor",
			orderCols:    []string{"created_at"},
			pkCols:       []string{"id"},
			cursor:       false,
			wantContains: []string{"SELECT `created_at`,`id` FROM `test_db`.`test_tbl`", "WHERE (status = 'active')", "ORDER BY `created_at` ASC,`id` ASC LIMIT 1000"},
			wantArgs:     0,
		},
		{
			name:         "composite pk with cursor",
			orderCols:    []string{"created_at"},
			pkCols:       []string{"id", "tenant_id"},
			cursor:       true,
			cursorValues: []any{"2025-01-01", 100, 200},
			wantContains: []string{"AND (`created_at`,`id`,`tenant_id`) > (?,?,?)"},
			wantArgs:     3,
		},
		{
			name:         "composite order cols with cursor",
			orderCols:    []string{"created_at", "updated_at"},
			pkCols:       []string{"id"},
			cursor:       true,
			cursorValues: []any{"2025-01-01", "2025-01-02", 300},
			wantContains: []string{"AND (`created_at`,`updated_at`,`id`) > (?,?,?)"},
			wantArgs:     3,
		},
		{
			name:         "order col overlaps pk is deduped",
			orderCols:    []string{"id", "created_at"},
			pkCols:       []string{"id"},
			cursor:       true,
			cursorValues: []any{1, "2025-01-01"},
			wantContains: []string{"SELECT `id`,`created_at` FROM", "AND (`id`,`created_at`) > (?,?)"},
			wantArgs:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ocs := newCoveringStrategyForTest(tt.pkCols, tt.orderCols, tt.cursor, tt.cursorValues)
			sql, args := ocs.buildSelectSQL(ocs.defaultTableRef(), tt.cursorValues)
			for _, want := range tt.wantContains {
				if !strings.Contains(sql, want) {
					t.Errorf("SQL missing %q\ngot: %s", want, sql)
				}
			}
			if len(args) != tt.wantArgs {
				t.Errorf("args count = %d, want %d (sql=%s)", len(args), tt.wantArgs, sql)
			}
		})
	}
}

func TestOBCoveringStrategy_BuildSelectSQL_ForceIndex(t *testing.T) {
	ocs := newCoveringStrategyForTest([]string{"id"}, []string{"created_at"}, false, nil)
	ocs.selectIndex = "idx_created_at"
	sql, _ := ocs.buildSelectSQL(ocs.defaultTableRef(), nil)
	if !strings.Contains(sql, "FORCE INDEX (`idx_created_at`)") {
		t.Errorf("expected FORCE INDEX hint, got: %s", sql)
	}
}

func TestOBCoveringStrategy_BuildDeleteSQL(t *testing.T) {
	tests := []struct {
		name         string
		pkCols       []string
		batch        []pkRow
		wantContains string
		wantArgs     []any
	}{
		{
			name:         "single pk 3 rows",
			pkCols:       []string{"id"},
			batch:        []pkRow{{pk: []any{1}}, {pk: []any{2}}, {pk: []any{3}}},
			wantContains: "WHERE (status = 'active')\n   AND (`id`) IN (?,?,?)",
			wantArgs:     []any{1, 2, 3},
		},
		{
			name:         "composite pk 2 rows",
			pkCols:       []string{"id", "tenant_id"},
			batch:        []pkRow{{pk: []any{100, 1}}, {pk: []any{200, 2}}},
			wantContains: "AND (`id`,`tenant_id`) IN ((?,?),(?,?))",
			wantArgs:     []any{100, 1, 200, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ocs := newCoveringStrategyForTest(tt.pkCols, nil, false, nil)
			sql, args := ocs.buildDeleteSQL(ocs.defaultTableRef(), tt.batch)
			if !strings.Contains(sql, tt.wantContains) {
				t.Errorf("SQL missing %q\ngot: %s", tt.wantContains, sql)
			}
			if !strings.Contains(sql, "DELETE FROM `test_db`.`test_tbl`") {
				t.Errorf("DELETE target missing\ngot: %s", sql)
			}
			if len(args) != len(tt.wantArgs) {
				t.Fatalf("args count = %d, want %d", len(args), len(tt.wantArgs))
			}
			for i := range args {
				if args[i] != tt.wantArgs[i] {
					t.Errorf("args[%d] = %v, want %v", i, args[i], tt.wantArgs[i])
				}
			}
		})
	}
}

// Frozen WHERE must always be preserved in the DELETE to avoid touching rows
// outside the original predicate.
func TestOBCoveringStrategy_DeleteAlwaysKeepsFrozenWhere(t *testing.T) {
	ocs := newCoveringStrategyForTest([]string{"id"}, nil, false, nil)
	sql, _ := ocs.buildDeleteSQL(ocs.defaultTableRef(), []pkRow{{pk: []any{1}}})
	if !strings.Contains(sql, "(status = 'active')") {
		t.Fatalf("frozen where dropped from DELETE: %s", sql)
	}
}

func TestOBCoveringStrategy_CursorAdvancesFromBatchTail(t *testing.T) {
	ocs := newCoveringStrategyForTest([]string{"id"}, []string{"created_at"}, true, nil)
	batch := []pkRow{
		{pk: []any{1}, full: []any{"2025-01-01", 1}},
		{pk: []any{2}, full: []any{"2025-01-02", 2}},
	}
	// Simulate the Run consumer advancing the cursor after a successful DELETE.
	ocs.cursorValues = batch[len(batch)-1].full

	if len(ocs.cursorValues) != 2 {
		t.Fatalf("cursor len = %d, want 2", len(ocs.cursorValues))
	}
	want := []any{"2025-01-02", 2}
	for i, v := range ocs.cursorValues {
		if v != want[i] {
			t.Errorf("cursor[%d] = %v, want %v", i, v, want[i])
		}
	}

	// The next SELECT must bind the advanced cursor values.
	_, args := ocs.buildSelectSQL(ocs.defaultTableRef(), ocs.cursorValues)
	if len(args) != 2 || args[0] != "2025-01-02" || args[1] != 2 {
		t.Errorf("next-page args = %v, want [2025-01-02 2]", args)
	}
}

func TestOBCoveringStrategy_FindPKIndexes(t *testing.T) {
	ocs := newCoveringStrategyForTest([]string{"id", "tenant_id"}, []string{"created_at"}, false, nil)
	idx, err := ocs.findPKIndexes([]string{"created_at", "id", "tenant_id"})
	if err != nil {
		t.Fatalf("findPKIndexes error: %v", err)
	}
	if len(idx) != 2 || idx[0] != 1 || idx[1] != 2 {
		t.Fatalf("pk indexes = %v, want [1 2]", idx)
	}

	if _, err := ocs.findPKIndexes([]string{"created_at", "id"}); err == nil {
		t.Fatal("expected error when a pk column is missing from result columns")
	}
}

func TestOBCoveringStrategy_DryRunSampleBuilds(t *testing.T) {
	ocs := newCoveringStrategyForTest([]string{"id"}, []string{"created_at"}, false, nil)
	ocs.selectIndex = "idx_created_at"
	if err := ocs.PrintDryRunSample(); err != nil {
		t.Fatalf("PrintDryRunSample failed: %v", err)
	}
}

func TestOBCoveringStrategy_Name(t *testing.T) {
	ocs := newCoveringStrategyForTest([]string{"id"}, []string{"created_at"}, false, nil)
	if ocs.Name() != "OBCoveringStrategy" {
		t.Fatalf("Name = %s", ocs.Name())
	}
}

// ---- Run / producer integration via a small fake driver ----

// coveringFakeDriver answers the candidate SELECT with scripted pages and
// records DELETE executions so tests can assert paging, empty-set stop, and
// that DELETE happens once per non-empty page.
type coveringFakeDriverState struct {
	mu          sync.Mutex
	selectPages [][][]driver.Value // each page is a slice of rows; rows are [created_at, id]
	pageIdx     int
	deleteCalls int
	deletedRows int
}

type coveringFakeDriver struct{ state *coveringFakeDriverState }
type coveringFakeConn struct{ state *coveringFakeDriverState }
type coveringFakeTx struct{ state *coveringFakeDriverState }
type coveringFakeRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

var coveringDriverSeq atomic.Int64

func (d *coveringFakeDriver) Open(_ string) (driver.Conn, error) {
	return &coveringFakeConn{state: d.state}, nil
}
func (c *coveringFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not implemented")
}
func (c *coveringFakeConn) Close() error { return nil }
func (c *coveringFakeConn) Begin() (driver.Tx, error) {
	return &coveringFakeTx{state: c.state}, nil
}
func (c *coveringFakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &coveringFakeTx{state: c.state}, nil
}

func (c *coveringFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(strings.ToUpper(query), "SELECT") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	var page [][]driver.Value
	if c.state.pageIdx < len(c.state.selectPages) {
		page = c.state.selectPages[c.state.pageIdx]
		c.state.pageIdx++
	}
	return &coveringFakeRows{columns: []string{"created_at", "id"}, rows: page}, nil
}

func (c *coveringFakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.Contains(strings.ToUpper(query), "DELETE") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.deleteCalls++
	c.state.deletedRows += len(args)
	return driver.RowsAffected(int64(len(args))), nil
}

func (tx *coveringFakeTx) Commit() error   { return nil }
func (tx *coveringFakeTx) Rollback() error { return nil }

func (r *coveringFakeRows) Columns() []string { return r.columns }
func (r *coveringFakeRows) Close() error      { return nil }
func (r *coveringFakeRows) Next(dest []driver.Value) error {
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

func newCoveringTestDB(t *testing.T, pages [][][]driver.Value) (*sql.DB, *coveringFakeDriverState) {
	t.Helper()
	state := &coveringFakeDriverState{selectPages: pages}
	name := fmt.Sprintf("covering_fake_driver_%d", coveringDriverSeq.Add(1))
	sql.Register(name, &coveringFakeDriver{state: state})
	db, err := sql.Open(name, "covering-test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, state
}

type noopBucket struct{}

func (noopBucket) Wait(int64) {}

type gateBucket struct {
	waits    chan int64
	releases chan struct{}
}

func newGateBucket() *gateBucket {
	return &gateBucket{
		waits:    make(chan int64, 8),
		releases: make(chan struct{}, 8),
	}
}

func (b *gateBucket) Wait(n int64) {
	b.waits <- n
	<-b.releases
}

func (b *gateBucket) release() {
	b.releases <- struct{}{}
}

func (b *gateBucket) waitForWait(t *testing.T) int64 {
	t.Helper()
	select {
	case n := <-b.waits:
		return n
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Bucket.Wait")
		return 0
	}
}

// countingBucket records every token count passed to Wait so a test can assert
// the strategy never throttles by row count when sleep/rows-per-sec are off.
type countingBucket struct {
	mu      sync.Mutex
	total   int64
	maxOnce int64
}

func (b *countingBucket) Wait(n int64) {
	b.mu.Lock()
	b.total += n
	if n > b.maxOnce {
		b.maxOnce = n
	}
	b.mu.Unlock()
}

func coveringDeletes(state *coveringFakeDriverState) int {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.deleteCalls
}

func TestOBCoveringStrategy_Run_LagSignalRecheckedBeforeDelete(t *testing.T) {
	pages := [][][]driver.Value{
		{{[]byte("2025-01-01"), []byte("1")}},
	}
	db, state := newCoveringTestDB(t, pages)
	ocs := &OBCoveringStrategy{
		writer: &Writer{
			MysqlClient:       db,
			Database:          "d",
			Table:             "t",
			OriginWhereClause: "(id > 0)",
			ChunkSize:         2,
			unqKeys:           &UnqKeys{UniqueKeyColumns: []string{"id"}},
		},
		selectOrderCols: []string{"created_at"},
	}

	bucket := newGateBucket()
	bucketNum := make(chan int64, 2)
	bucketNum <- vars.LagThreshold

	errCh := make(chan error, 1)
	go func() {
		errCh <- ocs.Run(context.Background(), RunParams{Bucket: bucket, BucketNum: bucketNum})
	}()

	if got := bucket.waitForWait(t); got != 1000 {
		t.Fatalf("first lag wait = %d, want 1000", got)
	}
	if got := coveringDeletes(state); got != 0 {
		t.Fatalf("delete calls during first lag wait = %d, want 0", got)
	}

	bucketNum <- vars.LagThreshold
	bucket.release()
	if got := bucket.waitForWait(t); got != 1000 {
		t.Fatalf("second lag wait = %d, want 1000", got)
	}
	if got := coveringDeletes(state); got != 0 {
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
	if got := coveringDeletes(state); got != 1 {
		t.Fatalf("delete calls after lag clears = %d, want 1", got)
	}
}

func TestOBCoveringStrategy_Run_NoRowCountThrottle(t *testing.T) {
	// sleep=0 (bucketNum=0) and no RowsLimiter: the strategy must not feed the
	// row count into Bucket.Wait (the bucket is a 1-token-per-ms SLEEP bucket;
	// Wait(affected) would sleep ~affected ms per chunk). Regression for the
	// covering/partition throughput bug.
	pages := [][][]driver.Value{
		{{[]byte("2025-01-01"), []byte("1")}, {[]byte("2025-01-02"), []byte("2")}},
		{{[]byte("2025-01-03"), []byte("3")}},
	}
	db, _ := newCoveringTestDB(t, pages)

	ocs := &OBCoveringStrategy{
		writer: &Writer{
			MysqlClient:       db,
			Database:          "d",
			Table:             "t",
			OriginWhereClause: "(id > 0)",
			ChunkSize:         2,
			unqKeys:           &UnqKeys{UniqueKeyColumns: []string{"id"}},
		},
		selectOrderCols: []string{"created_at"},
		selectCursor:    true,
	}

	bucket := &countingBucket{}
	bucketNum := make(chan int64, 1)
	bucketNum <- 0
	errCh := make(chan error, 1)
	go func() {
		errCh <- ocs.Run(context.Background(), RunParams{Bucket: bucket, BucketNum: bucketNum})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out")
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if bucket.total != 0 {
		t.Errorf("bucket waited %d tokens total, want 0 (no row-count throttle when sleep=0)", bucket.total)
	}
}

func TestOBCoveringStrategy_Run_PagesAndEmptyStop(t *testing.T) {
	// chunk-size 2: a full page (2 rows) then a short page (1 row) stops paging.
	pages := [][][]driver.Value{
		{{[]byte("2025-01-01"), []byte("1")}, {[]byte("2025-01-02"), []byte("2")}},
		{{[]byte("2025-01-03"), []byte("3")}},
	}
	db, state := newCoveringTestDB(t, pages)

	ocs := &OBCoveringStrategy{
		writer: &Writer{
			MysqlClient:       db,
			Database:          "d",
			Table:             "t",
			OriginWhereClause: "(id > 0)",
			ChunkSize:         2,
			unqKeys:           &UnqKeys{UniqueKeyColumns: []string{"id"}},
		},
		selectOrderCols: []string{"created_at"},
		selectCursor:    true,
	}

	bucketNum := make(chan int64, 1)
	bucketNum <- 0
	errCh := make(chan error, 1)
	go func() {
		errCh <- ocs.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: bucketNum})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 2 {
		t.Errorf("delete calls = %d, want 2 (one per non-empty page)", state.deleteCalls)
	}
	if state.deletedRows != 3 {
		t.Errorf("deleted rows = %d, want 3", state.deletedRows)
	}
	if ocs.writer.GetRowAffects() != 3 {
		t.Errorf("rowAffects = %d, want 3", ocs.writer.GetRowAffects())
	}
	if !ocs.writer.IsFinished() {
		t.Errorf("expected writer finished after empty-set stop")
	}
}

func TestOBCoveringStrategy_Run_EmptyFirstPageNoDelete(t *testing.T) {
	db, state := newCoveringTestDB(t, [][][]driver.Value{{}})
	ocs := &OBCoveringStrategy{
		writer: &Writer{
			MysqlClient:       db,
			Database:          "d",
			Table:             "t",
			OriginWhereClause: "(id > 0)",
			ChunkSize:         10,
			unqKeys:           &UnqKeys{UniqueKeyColumns: []string{"id"}},
		},
		selectOrderCols: []string{"created_at"},
	}

	bn := make(chan int64, 1)
	bn <- 0
	errCh := make(chan error, 1)
	go func() {
		errCh <- ocs.Run(context.Background(), RunParams{Bucket: ratelimit.NewBucketWithQuantum(time.Millisecond, 1, 1), BucketNum: bn})
	}()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 0 {
		t.Errorf("delete calls = %d, want 0 on empty set", state.deleteCalls)
	}
}
