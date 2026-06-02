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

func newPartitionStrategyForTest(pkCols, orderCols []string) *OBPartitionStrategy {
	return &OBPartitionStrategy{
		writer: &Writer{
			Database:          "test_db",
			Table:             "test_tbl",
			OriginWhereClause: "(status = 'active')",
			ChunkSize:         1000,
			unqKeys:           &UnqKeys{UniqueKeyColumns: pkCols},
		},
		selectOrderCols: orderCols,
	}
}

func TestWriter_DataSourceAccessor(t *testing.T) {
	w := &Writer{}
	if w.DataSource() != "" {
		t.Errorf("DataSource before detection = %q, want empty", w.DataSource())
	}
	w.dataSource = dataSourceOceanBase
	if w.DataSource() != "oceanbase" {
		t.Errorf("DataSource = %q, want oceanbase", w.DataSource())
	}
}

func TestOBPartitionStrategy_Name(t *testing.T) {
	ps := newPartitionStrategyForTest([]string{"id"}, []string{"created_at"})
	if ps.Name() != "OBPartitionStrategy" {
		t.Fatalf("Name = %s", ps.Name())
	}
}

func TestOBPartitionStrategy_PartitionTableRefInjection(t *testing.T) {
	ps := newPartitionStrategyForTest([]string{"id"}, []string{"created_at"})
	cov := ps.newCovering()
	tableRef := ps.partitionTableRef("p0")

	selSQL, _ := cov.buildSelectSQL(tableRef, nil)
	if !strings.Contains(selSQL, "FROM `test_db`.`test_tbl` PARTITION (`p0`)") {
		t.Errorf("SELECT missing partition scope, got: %s", selSQL)
	}

	delSQL, args := cov.buildDeleteSQL(tableRef, []pkRow{{pk: []any{1}}, {pk: []any{2}}})
	if !strings.Contains(delSQL, "DELETE FROM `test_db`.`test_tbl` PARTITION (`p0`)") {
		t.Errorf("DELETE missing partition scope, got: %s", delSQL)
	}
	// Bind-arg ordering is unchanged vs the non-partition path.
	if len(args) != 2 || args[0] != 1 || args[1] != 2 {
		t.Errorf("delete args = %v, want [1 2]", args)
	}
}

// Bind-arg ordering for the SELECT cursor must match the non-partition path.
func TestOBPartitionStrategy_SelectArgsUnchangedVsNonPartition(t *testing.T) {
	ps := newPartitionStrategyForTest([]string{"id"}, []string{"created_at"})
	ps.selectCursor = true
	cov := ps.newCovering()

	cursor := []any{"2025-01-01", 5}
	_, partArgs := cov.buildSelectSQL(ps.partitionTableRef("p0"), cursor)
	_, plainArgs := cov.buildSelectSQL(cov.defaultTableRef(), cursor)
	if len(partArgs) != len(plainArgs) {
		t.Fatalf("arg count differs: partition=%d plain=%d", len(partArgs), len(plainArgs))
	}
	for i := range partArgs {
		if partArgs[i] != plainArgs[i] {
			t.Errorf("arg[%d] differs: partition=%v plain=%v", i, partArgs[i], plainArgs[i])
		}
	}
}

func TestOBPartitionStrategy_WorkerCount(t *testing.T) {
	tests := []struct {
		concurrency   int
		numPartitions int
		want          int
	}{
		{4, 8, 4},
		{8, 4, 4},  // clamp to partitions
		{0, 4, 1},  // floor 1
		{-3, 4, 1}, // defensive floor
		{1, 1, 1},
		{1000, 200, partitionHardMax}, // hard max
	}
	for _, tt := range tests {
		ps := &OBPartitionStrategy{concurrency: tt.concurrency}
		if got := ps.workerCount(tt.numPartitions); got != tt.want {
			t.Errorf("workerCount(conc=%d, parts=%d) = %d, want %d",
				tt.concurrency, tt.numPartitions, got, tt.want)
		}
	}
}

// ---- partitionRunState ----

func TestPartitionRunState_ShouldStop(t *testing.T) {
	ctx := context.Background()
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		rows    int64
		maxRows int64
		maxDur  time.Duration
		started time.Time
		want    bool
	}{
		{"normal drain", ctx, 10, 100, 0, time.Now(), false},
		{"max-rows hit", ctx, 100, 100, 0, time.Now(), true},
		{"max-duration hit", ctx, 0, 0, time.Millisecond, time.Now().Add(-time.Second), true},
		{"ctx cancelled", cancelledCtx, 0, 0, 0, time.Now(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &partitionRunState{started: tt.started, rows: tt.rows, stopReason: "drained"}
			if got := s.shouldStop(tt.ctx, tt.maxRows, tt.maxDur); got != tt.want {
				t.Errorf("shouldStop = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPartitionRunState_AddRowsConcurrent(t *testing.T) {
	w := &Writer{}
	s := &partitionRunState{stopReason: "drained"}

	const goroutines = 16
	const perG = 100
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				s.addRows(w, 1)
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perG)
	if got := w.GetRowAffects(); got != want {
		t.Errorf("writer rowAffects = %d, want %d", got, want)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows != want {
		t.Errorf("state.rows = %d, want %d", s.rows, want)
	}
}

func TestPartitionRunState_SetStopOnce(t *testing.T) {
	s := &partitionRunState{stopReason: "drained"}
	s.mu.Lock()
	s.setStopLocked("max-rows")
	s.setStopLocked("max-duration")
	got := s.stopReason
	s.mu.Unlock()
	if got != "max-rows" {
		t.Errorf("stopReason = %q, want %q (first reason wins)", got, "max-rows")
	}
}

// ---- fake driver covering discovery + partition-scoped SELECT/DELETE ----

type partFakeState struct {
	mu          sync.Mutex
	partitions  []string                      // returned by the PARTITIONS query
	pages       map[string][][][]driver.Value // partition name -> pages of [created_at,id] rows
	pageIdx     map[string]int
	deleteCalls int
	deletedRows int
	failDelete  string // partition name whose DELETE returns an error
}

type partFakeDriver struct{ state *partFakeState }
type partFakeConn struct{ state *partFakeState }
type partFakeTx struct{ state *partFakeState }
type partFakeRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

var partDriverSeq atomic.Int64

func (d *partFakeDriver) Open(_ string) (driver.Conn, error) {
	return &partFakeConn{state: d.state}, nil
}
func (c *partFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not implemented")
}
func (c *partFakeConn) Close() error { return nil }
func (c *partFakeConn) Begin() (driver.Tx, error) {
	return &partFakeTx{state: c.state}, nil
}
func (c *partFakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &partFakeTx{state: c.state}, nil
}

// partitionNameFromQuery extracts `name` from "... PARTITION (`name`) ...".
func partitionNameFromQuery(query string) string {
	const marker = "PARTITION (`"
	i := strings.Index(query, marker)
	if i < 0 {
		return ""
	}
	rest := query[i+len(marker):]
	j := strings.Index(rest, "`")
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func (c *partFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	up := strings.ToUpper(query)
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	if strings.Contains(up, "INFORMATION_SCHEMA.PARTITIONS") {
		rows := make([][]driver.Value, 0, len(c.state.partitions))
		for _, p := range c.state.partitions {
			rows = append(rows, []driver.Value{[]byte(p)})
		}
		return &partFakeRows{columns: []string{"PARTITION_NAME"}, rows: rows}, nil
	}

	if strings.Contains(up, "SELECT") {
		name := partitionNameFromQuery(query)
		var page [][]driver.Value
		if pages, ok := c.state.pages[name]; ok {
			idx := c.state.pageIdx[name]
			if idx < len(pages) {
				page = pages[idx]
				c.state.pageIdx[name] = idx + 1
			}
		}
		return &partFakeRows{columns: []string{"created_at", "id"}, rows: page}, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}

func (c *partFakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.Contains(strings.ToUpper(query), "DELETE") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	name := partitionNameFromQuery(query)
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.failDelete != "" && name == c.state.failDelete {
		return nil, fmt.Errorf("injected delete failure on partition %s", name)
	}
	c.state.deleteCalls++
	c.state.deletedRows += len(args)
	return driver.RowsAffected(int64(len(args))), nil
}

func (tx *partFakeTx) Commit() error   { return nil }
func (tx *partFakeTx) Rollback() error { return nil }

func (r *partFakeRows) Columns() []string { return r.columns }
func (r *partFakeRows) Close() error      { return nil }
func (r *partFakeRows) Next(dest []driver.Value) error {
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

func newPartTestDB(t *testing.T, state *partFakeState) *sql.DB {
	t.Helper()
	if state.pageIdx == nil {
		state.pageIdx = make(map[string]int)
	}
	name := fmt.Sprintf("part_fake_driver_%d", partDriverSeq.Add(1))
	sql.Register(name, &partFakeDriver{state: state})
	db, err := sql.Open(name, "part-test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestDiscoverPartitions(t *testing.T) {
	state := &partFakeState{partitions: []string{"p0", "p1", "p2"}}
	db := newPartTestDB(t, state)

	got, err := discoverPartitions(context.Background(), db, "test_db", "test_tbl")
	if err != nil {
		t.Fatalf("discoverPartitions err: %v", err)
	}
	want := []string{"p0", "p1", "p2"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("partition[%d] = %s, want %s (ordinal order)", i, got[i], want[i])
		}
	}
}

func TestDiscoverPartitions_NonPartitioned(t *testing.T) {
	state := &partFakeState{partitions: nil}
	db := newPartTestDB(t, state)
	got, err := discoverPartitions(context.Background(), db, "test_db", "test_tbl")
	if err != nil {
		t.Fatalf("discoverPartitions err: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 partitions, got %v", got)
	}
}

func newPartitionStrategyWithDB(db *sql.DB, pkCols, orderCols []string, concurrency int) *OBPartitionStrategy {
	return &OBPartitionStrategy{
		writer: &Writer{
			MysqlClient:       db,
			Database:          "test_db",
			Table:             "test_tbl",
			OriginWhereClause: "(id > 0)",
			ChunkSize:         2,
			unqKeys:           &UnqKeys{UniqueKeyColumns: pkCols},
		},
		selectOrderCols: orderCols,
		selectCursor:    true,
		concurrency:     concurrency,
	}
}

func TestOBPartitionStrategy_Run_NonPartitionedErrors(t *testing.T) {
	state := &partFakeState{partitions: nil}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, 2)

	err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: make(chan int64, 1)})
	if err == nil || !strings.Contains(err.Error(), "no partitions") {
		t.Fatalf("expected no-partitions error, got: %v", err)
	}
}

func TestOBPartitionStrategy_Run_DrainsAllPartitions(t *testing.T) {
	// Three partitions; each has a full page (2 rows) then a short page (1 row).
	state := &partFakeState{
		partitions: []string{"p0", "p1", "p2"},
		pages: map[string][][][]driver.Value{
			"p0": {{{[]byte("2025-01-01"), []byte("1")}, {[]byte("2025-01-02"), []byte("2")}}, {{[]byte("2025-01-03"), []byte("3")}}},
			"p1": {{{[]byte("2025-01-01"), []byte("11")}, {[]byte("2025-01-02"), []byte("12")}}, {{[]byte("2025-01-03"), []byte("13")}}},
			"p2": {{{[]byte("2025-01-01"), []byte("21")}, {[]byte("2025-01-02"), []byte("22")}}, {{[]byte("2025-01-03"), []byte("23")}}},
		},
	}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, 3)

	bucketNum := make(chan int64, 1)
	bucketNum <- 0
	if err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: bucketNum}); err != nil {
		t.Fatalf("Run err: %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	// 3 partitions * 2 non-empty pages each = 6 DELETE calls, 9 rows total.
	if state.deleteCalls != 6 {
		t.Errorf("delete calls = %d, want 6", state.deleteCalls)
	}
	if state.deletedRows != 9 {
		t.Errorf("deleted rows = %d, want 9", state.deletedRows)
	}
	if ps.writer.GetRowAffects() != 9 {
		t.Errorf("rowAffects = %d, want 9", ps.writer.GetRowAffects())
	}
	if !ps.writer.IsFinished() {
		t.Error("expected writer finished")
	}
}

func TestOBPartitionStrategy_Run_MaxRowsStops(t *testing.T) {
	// Many rows per partition; maxRows should halt before draining everything.
	pages := map[string][][][]driver.Value{}
	for _, p := range []string{"p0", "p1"} {
		// 100 full pages -> effectively unbounded for this test.
		full := make([][][]driver.Value, 0, 100)
		for i := 0; i < 100; i++ {
			full = append(full, [][]driver.Value{
				{[]byte("2025-01-01"), []byte(fmt.Sprintf("%d", i*2+1))},
				{[]byte("2025-01-02"), []byte(fmt.Sprintf("%d", i*2+2))},
			})
		}
		pages[p] = full
	}
	state := &partFakeState{partitions: []string{"p0", "p1"}, pages: pages}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, 2)
	ps.maxRows = 10

	if err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: make(chan int64, 1)}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	got := ps.writer.GetRowAffects()
	// Exact: budget reservation under the shared lock trims each batch to the
	// remaining max-rows budget, so the total deleted equals maxRows exactly
	// (the fake DELETE reports affected == len(batch)).
	if got != 10 {
		t.Errorf("rowAffects = %d, want exactly 10 (maxRows, exact trim)", got)
	}
	// The DB-side deleted count must also equal maxRows (no over-DELETE).
	state.mu.Lock()
	deleted := state.deletedRows
	state.mu.Unlock()
	if deleted != 10 {
		t.Errorf("deletedRows = %d, want exactly 10 (no over-delete)", deleted)
	}
}

// maxRows that is not a multiple of chunk-size must still be hit exactly: a
// batch straddling the budget boundary is trimmed to the remaining budget.
func TestOBPartitionStrategy_Run_MaxRowsExactNonMultiple(t *testing.T) {
	pages := map[string][][][]driver.Value{}
	for _, p := range []string{"p0", "p1", "p2"} {
		full := make([][][]driver.Value, 0, 100)
		for i := 0; i < 100; i++ {
			full = append(full, [][]driver.Value{
				{[]byte("2025-01-01"), []byte(fmt.Sprintf("%d", i*2+1))},
				{[]byte("2025-01-02"), []byte(fmt.Sprintf("%d", i*2+2))},
			})
		}
		pages[p] = full
	}
	state := &partFakeState{partitions: []string{"p0", "p1", "p2"}, pages: pages}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, 3)
	ps.maxRows = 7 // odd, not a multiple of chunkSize=2

	if err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: make(chan int64, 1)}); err != nil {
		t.Fatalf("Run err: %v", err)
	}
	if got := ps.writer.GetRowAffects(); got != 7 {
		t.Errorf("rowAffects = %d, want exactly 7", got)
	}
	state.mu.Lock()
	deleted := state.deletedRows
	state.mu.Unlock()
	if deleted != 7 {
		t.Errorf("deletedRows = %d, want exactly 7", deleted)
	}
}

func TestOBPartitionStrategy_Run_FirstErrorPropagates(t *testing.T) {
	state := &partFakeState{
		partitions: []string{"p0", "p1"},
		pages: map[string][][][]driver.Value{
			"p0": {{{[]byte("2025-01-01"), []byte("1")}, {[]byte("2025-01-02"), []byte("2")}}},
			"p1": {{{[]byte("2025-01-01"), []byte("11")}, {[]byte("2025-01-02"), []byte("12")}}},
		},
		failDelete: "p1",
	}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, 2)

	err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: make(chan int64, 1)})
	if err == nil || !strings.Contains(err.Error(), "delete pk batch failed") {
		t.Fatalf("expected delete failure propagation, got: %v", err)
	}
}

// TestOBPartitionStrategy_Run_ConcurrentRace spins up multiple workers over
// many partitions so concurrent SELECT/DELETE and shared-state updates
// (addRows, reserveBudget, lag gate) actually overlap. Run under -race to
// detect data races on the shared partitionRunState / writer counters.
func TestOBPartitionStrategy_Run_ConcurrentRace(t *testing.T) {
	const (
		numParts = 8
		pagesPer = 25 // full pages per partition
	)
	pages := map[string][][][]driver.Value{}
	parts := make([]string, 0, numParts)
	rowID := 0
	wantRows := 0
	for pi := 0; pi < numParts; pi++ {
		name := fmt.Sprintf("p%d", pi)
		parts = append(parts, name)
		full := make([][][]driver.Value, 0, pagesPer+1)
		for j := 0; j < pagesPer; j++ {
			rowID++
			a := rowID
			rowID++
			b := rowID
			full = append(full, [][]driver.Value{
				{[]byte("2025-01-01"), []byte(fmt.Sprintf("%d", a))},
				{[]byte("2025-01-02"), []byte(fmt.Sprintf("%d", b))},
			})
			wantRows += 2
		}
		// Trailing short page (1 row) marks this partition's tail.
		rowID++
		full = append(full, [][]driver.Value{
			{[]byte("2025-01-03"), []byte(fmt.Sprintf("%d", rowID))},
		})
		wantRows++
		pages[name] = full
	}

	state := &partFakeState{partitions: parts, pages: pages}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, numParts)

	// Shrink the shared lag pause so the test exercises the gate without a
	// 1s real-time stall; restore after.
	orig := lagPauseDuration
	lagPauseDuration = 5 * time.Millisecond
	t.Cleanup(func() { lagPauseDuration = orig })

	// A non-blocking lag signal channel; feed a couple of LagThreshold sentinels
	// to exercise the shared lag gate under contention.
	bucketNum := make(chan int64, 4)
	bucketNum <- vars.LagThreshold
	bucketNum <- 0

	if err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: bucketNum}); err != nil {
		t.Fatalf("Run err: %v", err)
	}

	state.mu.Lock()
	deleted := state.deletedRows
	state.mu.Unlock()
	if deleted != wantRows {
		t.Errorf("deletedRows = %d, want %d (all partitions drained)", deleted, wantRows)
	}
	if got := ps.writer.GetRowAffects(); int(got) != wantRows {
		t.Errorf("rowAffects = %d, want %d", got, wantRows)
	}
	if !ps.writer.IsFinished() {
		t.Error("expected writer finished")
	}
}

func TestOBPartitionStrategy_Run_DryRunNoWorkers(t *testing.T) {
	state := &partFakeState{partitions: []string{"p0", "p1"}}
	db := newPartTestDB(t, state)
	ps := newPartitionStrategyWithDB(db, []string{"id"}, []string{"created_at"}, 2)
	ps.dryRun = true

	if err := ps.Run(context.Background(), RunParams{Bucket: noopBucket{}, BucketNum: make(chan int64, 1)}); err != nil {
		t.Fatalf("dry-run Run err: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteCalls != 0 {
		t.Errorf("dry-run should not execute DELETE, got %d calls", state.deleteCalls)
	}
}
