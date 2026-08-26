package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juju/ratelimit"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

type writerExecPlan struct {
	execErrors     []error
	blockExecCalls map[int]bool
	rowsAffected   int64
}

type writerTxState struct {
	id            int
	commitCalls   int
	rollbackCalls int
}

type writerDriverState struct {
	mu sync.Mutex

	plan writerExecPlan

	beginTxCalls int
	execCalls    int
	txs          []*writerTxState

	beginCallCh chan struct{}
	execCallCh  chan int
}

type writerFakeDriver struct {
	state *writerDriverState
}

type writerFakeConn struct {
	state *writerDriverState
}

type writerFakeTx struct {
	state   *writerDriverState
	txState *writerTxState
}

var writerDriverSeq atomic.Int64

func (d *writerFakeDriver) Open(_ string) (driver.Conn, error) {
	return &writerFakeConn{state: d.state}, nil
}

func (c *writerFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in writer fake conn")
}

func (c *writerFakeConn) Close() error {
	return nil
}

func (c *writerFakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *writerFakeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.state.mu.Lock()
	c.state.beginTxCalls++
	txState := &writerTxState{id: len(c.state.txs) + 1}
	c.state.txs = append(c.state.txs, txState)
	c.state.mu.Unlock()

	select {
	case c.state.beginCallCh <- struct{}{}:
	default:
	}
	return &writerFakeTx{state: c.state, txState: txState}, nil
}

func (c *writerFakeConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	callIndex := c.state.execCalls
	c.state.execCalls++
	plan := c.state.plan
	c.state.mu.Unlock()

	select {
	case c.state.execCallCh <- callIndex:
	default:
	}

	if plan.blockExecCalls != nil && plan.blockExecCalls[callIndex] {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	if callIndex < len(plan.execErrors) && plan.execErrors[callIndex] != nil {
		return nil, plan.execErrors[callIndex]
	}

	affected := plan.rowsAffected
	if affected == 0 {
		affected = 1
	}
	return driver.RowsAffected(affected), nil
}

func (c *writerFakeConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	named := make([]driver.NamedValue, 0, len(args))
	for i, v := range args {
		named = append(named, driver.NamedValue{Ordinal: i + 1, Value: v})
	}
	return c.ExecContext(context.Background(), query, named)
}

func (tx *writerFakeTx) Commit() error {
	tx.state.mu.Lock()
	tx.txState.commitCalls++
	tx.state.mu.Unlock()
	return nil
}

func (tx *writerFakeTx) Rollback() error {
	tx.state.mu.Lock()
	tx.txState.rollbackCalls++
	tx.state.mu.Unlock()
	return nil
}

func newWriterTestDB(t *testing.T, plan writerExecPlan) (*sql.DB, *writerDriverState) {
	t.Helper()

	state := &writerDriverState{
		plan:        plan,
		beginCallCh: make(chan struct{}, 16),
		execCallCh:  make(chan int, 32),
	}
	driverName := fmt.Sprintf("writer_fake_driver_%d", writerDriverSeq.Add(1))
	sql.Register(driverName, &writerFakeDriver{state: state})

	db, err := sql.Open(driverName, "writer-test")
	if err != nil {
		t.Fatalf("open fake db failed: %v", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func newWriterForTest(db *sql.DB) *Writer {
	return &Writer{
		MysqlClient:   db,
		ExecuteSQL:    "UPDATE `t` SET `c` = 1 WHERE ",
		ChunkSize:     1,
		TxnSize:       100,
		ProducerQueue: make(chan *Producer, 8),
	}
}

func TestNewWriterPreservesExecuteQuery(t *testing.T) {
	query := "UPDATE sales.orders SET note = 'a;b' WHERE id > 0;"
	w := newWriter(&conf.Config{ExecuteQuery: query})

	if w.ExecuteSQL != query {
		t.Fatalf("ExecuteSQL = %q, want original query %q", w.ExecuteSQL, query)
	}
}

func enqueueOneBatchAndFinish(w *Writer) {
	w.ProducerQueue <- &Producer{
		WhereClause: "`id` = ?",
		CurrentKeyValues: []*KeyValue{
			{
				ColumnName:  "id",
				ColumnValue: int64(1),
			},
		},
	}
	w.ProducerQueue <- &Producer{IsFinished: true}
}

// enqueueNBatches 在后台 goroutine 里持续投递批次, 避免 ProducerQueue
// 缓冲满时阻塞测试主协程(消费者在护栏触发后会停止消费)。
func enqueueNBatches(w *Writer, n int) {
	go func() {
		for i := 0; i < n; i++ {
			select {
			case w.ProducerQueue <- &Producer{
				WhereClause: "`id` = ?",
				CurrentKeyValues: []*KeyValue{
					{ColumnName: "id", ColumnValue: int64(i + 1)},
				},
			}:
			case <-time.After(3 * time.Second):
				return
			}
		}
	}()
}

func TestWriterWrite_MaxRowsStops(t *testing.T) {
	db, state := newWriterTestDB(t, writerExecPlan{rowsAffected: 1})
	w := newWriterForTest(db)
	w.TxnSize = 1 // one row per tx -> guardrail evaluated每个事务后
	w.MaxRows = 3
	// 排入远多于 MaxRows 的批次, 且不投递 IsFinished, 确保停止来自护栏。
	enqueueNBatches(w, 50)

	bucketNum := make(chan int64, 1)
	bucketNum <- 0

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Write(context.Background(),
			ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1), bucketNum, nil)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error on max-rows stop, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting writer.Write to stop on max-rows")
	}

	if !w.IsFinished() {
		t.Fatalf("expected writer to be finished after max-rows")
	}
	if got := w.GetRowAffects(); got < w.MaxRows {
		t.Fatalf("expected rowAffects >= MaxRows(%d), got %d", w.MaxRows, got)
	}
	// 事务边界停止: 每事务 1 行, 不应溢出超过一个 txn-size。
	if got := w.GetRowAffects(); got > w.MaxRows {
		t.Fatalf("expected rowAffects == MaxRows(%d) with txn-size=1, got %d", w.MaxRows, got)
	}
	if _, execCalls := snapshotDriverCounters(state); execCalls != int(w.MaxRows) {
		t.Fatalf("expected exactly %d exec calls, got %d", w.MaxRows, execCalls)
	}
}

func TestWriterWrite_MaxDurationStops(t *testing.T) {
	db, _ := newWriterTestDB(t, writerExecPlan{rowsAffected: 1})
	w := newWriterForTest(db)
	w.TxnSize = 1
	w.MaxDuration = 50 * time.Millisecond
	enqueueNBatches(w, 100000)

	bucketNum := make(chan int64, 1)
	bucketNum <- 0

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Write(context.Background(),
			ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1), bucketNum, nil)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error on max-duration stop, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting writer.Write to stop on max-duration")
	}

	if !w.IsFinished() {
		t.Fatalf("expected writer to be finished after max-duration")
	}
}

func snapshotTxState(state *writerDriverState) []*writerTxState {
	state.mu.Lock()
	defer state.mu.Unlock()
	res := make([]*writerTxState, 0, len(state.txs))
	for _, tx := range state.txs {
		cp := *tx
		res = append(res, &cp)
	}
	return res
}

func snapshotDriverCounters(state *writerDriverState) (beginTxCalls, execCalls int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.beginTxCalls, state.execCalls
}

func TestWriterWrite_RetryAndCancellationRisks(t *testing.T) {
	tests := []struct {
		name             string
		plan             writerExecPlan
		cancelOnExecCall int
		wantErr          error
		assertFn         func(t *testing.T, txs []*writerTxState)
	}{
		{
			name: "retry_success_rolls_back_failed_tx_before_retry",
			plan: writerExecPlan{
				execErrors:   []error{errors.New("first exec failed"), nil},
				rowsAffected: 3,
			},
			cancelOnExecCall: -1,
			wantErr:          nil,
			assertFn: func(t *testing.T, txs []*writerTxState) {
				t.Helper()
				if len(txs) < 2 {
					t.Fatalf("expected >=2 tx objects, got %d", len(txs))
				}
				if txs[0].commitCalls != 0 || txs[0].rollbackCalls == 0 {
					t.Fatalf("expected first tx to be rolled back, got commit=%d rollback=%d",
						txs[0].commitCalls, txs[0].rollbackCalls)
				}
				if txs[1].commitCalls == 0 {
					t.Fatalf("expected retry tx to be committed at least once")
				}
			},
		},
		{
			name: "cancel_interrupts_retry_exec_context",
			plan: writerExecPlan{
				execErrors:     []error{errors.New("first exec failed"), nil},
				blockExecCalls: map[int]bool{1: true},
				rowsAffected:   1,
			},
			cancelOnExecCall: 1,
			wantErr:          context.Canceled,
			assertFn: func(t *testing.T, txs []*writerTxState) {
				t.Helper()
				if len(txs) < 2 {
					t.Fatalf("expected >=2 tx objects in cancel case, got %d", len(txs))
				}
				if txs[0].rollbackCalls == 0 {
					t.Fatalf("expected first tx rollback in cancel case")
				}
				if txs[1].commitCalls > 0 {
					t.Fatalf("retry tx should not be committed after cancel")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, state := newWriterTestDB(t, tt.plan)
			w := newWriterForTest(db)
			enqueueOneBatchAndFinish(w)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			bucketNum := make(chan int64, 1)
			bucketNum <- 0

			errCh := make(chan error, 1)
			go func() {
				errCh <- w.Write(ctx, ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1), bucketNum, nil)
			}()

			if tt.cancelOnExecCall >= 0 {
				timeout := time.After(2 * time.Second)
				waiting := true
				for waiting {
					select {
					case idx := <-state.execCallCh:
						if idx == tt.cancelOnExecCall {
							cancel()
							waiting = false
						}
					case <-timeout:
						beginCalls, execCalls := snapshotDriverCounters(state)
						t.Fatalf("timeout waiting exec call %d (beginTxCalls=%d execCalls=%d txs=%d)",
							tt.cancelOnExecCall, beginCalls, execCalls, len(snapshotTxState(state)))
					}
				}
			}

			select {
			case err := <-errCh:
				if tt.wantErr == nil {
					if err != nil {
						t.Fatalf("expected nil error, got %v", err)
					}
				} else if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
			case <-time.After(3 * time.Second):
				beginCalls, execCalls := snapshotDriverCounters(state)
				t.Fatalf("timeout waiting writer.Write (beginTxCalls=%d execCalls=%d txs=%d queueLen=%d)",
					beginCalls, execCalls, len(snapshotTxState(state)), len(w.ProducerQueue))
			}

			tt.assertFn(t, snapshotTxState(state))
		})
	}
}

func TestWriterWrite_CancelWhileWaitingProducerRollsBack(t *testing.T) {
	db, state := newWriterTestDB(t, writerExecPlan{})
	w := newWriterForTest(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Write(ctx, ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1), make(chan int64, 1), nil)
	}()

	select {
	case <-state.beginCallCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting first BeginTx")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil error after cancel while waiting producer, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting writer.Write after cancel")
	}

	txs := snapshotTxState(state)
	if len(txs) == 0 {
		t.Fatalf("expected at least one tx")
	}
	if txs[0].rollbackCalls == 0 {
		t.Fatalf("expected first tx rollback on cancel while waiting producer")
	}
}

// ---- getInfoFromTable tests ----

const defaultCreateTableDDL = "CREATE TABLE `t1` (\n" +
	"  `id` bigint NOT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

const compositePrimaryCreateTableDDL = "CREATE TABLE `t1` (\n" +
	"  `id` bigint NOT NULL,\n" +
	"  `user_id` bigint NOT NULL,\n" +
	"  PRIMARY KEY (`id`, `user_id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

const oceanBaseCompatCreateTableDDL = "CREATE TABLE `t1` (\n" +
	"  `id` bigint NOT NULL,\n" +
	"  `user_id` bigint NOT NULL,\n" +
	"  `order_code` varchar(255) NOT NULL,\n" +
	"  PRIMARY KEY (`id`, `user_id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

type getInfoCall struct {
	op     string
	connID int
}

type getInfoPlan struct {
	version        string
	versionErr     error
	dbNowLiteral   string
	dbNowErr       error
	setSessionErr  error
	createTableDDL string
	showCreateErr  error
	showIndexRows  [][]driver.Value
	showIndexErr   error
}

type getInfoDriverState struct {
	mu sync.Mutex

	plan       getInfoPlan
	nextConnID int
	calls      []getInfoCall
}

type getInfoFakeDriver struct {
	state *getInfoDriverState
}

type getInfoFakeConn struct {
	state  *getInfoDriverState
	connID int
}

type getInfoFakeRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

var getInfoDriverSeq atomic.Int64

func (d *getInfoFakeDriver) Open(_ string) (driver.Conn, error) {
	d.state.mu.Lock()
	defer d.state.mu.Unlock()

	d.state.nextConnID++
	return &getInfoFakeConn{
		state:  d.state,
		connID: d.state.nextConnID,
	}, nil
}

func (c *getInfoFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in get info fake conn")
}

func (c *getInfoFakeConn) Close() error {
	return nil
}

func (c *getInfoFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not implemented in get info fake conn")
}

func (c *getInfoFakeConn) recordCall(op string) getInfoPlan {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.calls = append(c.state.calls, getInfoCall{
		op:     op,
		connID: c.connID,
	})
	return c.state.plan
}

func (c *getInfoFakeConn) QueryContext(
	_ context.Context, query string, _ []driver.NamedValue,
) (driver.Rows, error) {
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	switch {
	case strings.Contains(lowerQuery, "select version()"):
		plan := c.recordCall("version")
		if plan.versionErr != nil {
			return nil, plan.versionErr
		}
		version := plan.version
		if version == "" {
			version = "8.0.36"
		}
		return &getInfoFakeRows{
			columns: []string{"version()"},
			rows: [][]driver.Value{
				{[]byte(version)},
			},
		}, nil
	case strings.Contains(lowerQuery, "date_format(now()"):
		plan := c.recordCall("db_now")
		if plan.dbNowErr != nil {
			return nil, plan.dbNowErr
		}
		literal := plan.dbNowLiteral
		if literal == "" {
			literal = "2026-06-03 12:00:00"
		}
		return &getInfoFakeRows{
			columns: []string{"DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')"},
			rows: [][]driver.Value{
				{[]byte(literal)},
			},
		}, nil
	case strings.Contains(lowerQuery, "show create table"):
		plan := c.recordCall("show_create")
		if plan.showCreateErr != nil {
			return nil, plan.showCreateErr
		}
		createTableDDL := plan.createTableDDL
		if createTableDDL == "" {
			createTableDDL = defaultCreateTableDDL
		}
		return &getInfoFakeRows{
			columns: []string{"Table", "Create Table"},
			rows: [][]driver.Value{
				{[]byte("t1"), []byte(createTableDDL)},
			},
		}, nil
	case strings.Contains(lowerQuery, "show index from"):
		plan := c.recordCall("show_index")
		if plan.showIndexErr != nil {
			return nil, plan.showIndexErr
		}
		rows := plan.showIndexRows
		if rows == nil {
			rows = [][]driver.Value{
				{
					[]byte("t1"),
					[]byte("0"),
					[]byte("PRIMARY"),
					[]byte("1"),
					[]byte("id"),
				},
			}
		}
		return &getInfoFakeRows{
			columns: []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name"},
			rows:    rows,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (c *getInfoFakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	_ = args
	return c.QueryContext(context.Background(), query, nil)
}

func (c *getInfoFakeConn) ExecContext(
	_ context.Context, query string, _ []driver.NamedValue,
) (driver.Result, error) {
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	if !strings.Contains(lowerQuery, "_show_ddl_in_compat_mode") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}

	plan := c.recordCall("set_ob_compat")
	if plan.setSessionErr != nil {
		return nil, plan.setSessionErr
	}
	return driver.RowsAffected(0), nil
}

func (c *getInfoFakeConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	_ = args
	return c.ExecContext(context.Background(), query, nil)
}

func (r *getInfoFakeRows) Columns() []string {
	return r.columns
}

func (r *getInfoFakeRows) Close() error {
	return nil
}

func (r *getInfoFakeRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	r.index++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
			continue
		}
		dest[i] = nil
	}
	return nil
}

func newGetInfoTestDB(t *testing.T, plan getInfoPlan) (*sql.DB, *getInfoDriverState) {
	t.Helper()

	state := &getInfoDriverState{plan: plan}
	driverName := fmt.Sprintf("writer_get_info_driver_%d", getInfoDriverSeq.Add(1))
	sql.Register(driverName, &getInfoFakeDriver{state: state})
	db, err := sql.Open(driverName, "writer-get-info-test")
	if err != nil {
		t.Fatalf("open get info fake db failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func newWriterGetInfoForTest(db *sql.DB) *Writer {
	return &Writer{
		MysqlClient: db,
		ExecuteSQL:  "DELETE FROM `t1` WHERE `id` > 0",
		Database:    "test_db",
		Table:       "t1",
	}
}

func snapshotGetInfoCalls(state *getInfoDriverState) []getInfoCall {
	state.mu.Lock()
	defer state.mu.Unlock()
	calls := make([]getInfoCall, 0, len(state.calls))
	calls = append(calls, state.calls...)
	return calls
}

func assertGetInfoCalls(t *testing.T, calls []getInfoCall, wantOps []string) {
	t.Helper()
	if len(calls) != len(wantOps) {
		t.Fatalf("call count mismatch, got %d want %d, calls=%+v", len(calls), len(wantOps), calls)
	}
	for i := range wantOps {
		if calls[i].op != wantOps[i] {
			t.Fatalf("call[%d] op mismatch, got %s want %s", i, calls[i].op, wantOps[i])
		}
	}
}

func assertSingleConnUsed(t *testing.T, calls []getInfoCall) {
	t.Helper()
	if len(calls) == 0 {
		t.Fatalf("calls should not be empty")
	}
	connID := calls[0].connID
	if connID == 0 {
		t.Fatalf("invalid connID in first call")
	}
	for i := range calls {
		if calls[i].connID != connID {
			t.Fatalf("calls use different conn id, got %+v", calls)
		}
	}
}

func TestWriterGetInfoFromTable_DataSourceFlow(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantOps []string
	}{
		{
			name:    "mysql_version_no_set_session",
			version: "8.0.36",
			wantOps: []string{"version", "show_create"},
		},
		{
			name:    "tidb_version_no_set_session",
			version: "8.0.11-TiDB-v8.1.1",
			wantOps: []string{"version", "show_create"},
		},
		{
			name:    "oceanbase_version_set_session_before_show_create",
			version: "5.7.25-OceanBase-v4.2.5.4",
			wantOps: []string{"version", "set_ob_compat", "show_create", "show_index"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, state := newGetInfoTestDB(t, getInfoPlan{version: tt.version})
			w := newWriterGetInfoForTest(db)
			err := w.getInfoFromTable(&conf.Config{
				ExecuteQuery: w.ExecuteSQL,
			})
			if err != nil {
				t.Fatalf("getInfoFromTable returned err: %v", err)
			}
			if w.unqKeys == nil {
				t.Fatalf("expected unqKeys to be initialized")
			}

			calls := snapshotGetInfoCalls(state)
			assertGetInfoCalls(t, calls, tt.wantOps)
			assertSingleConnUsed(t, calls)
		})
	}
}

func TestWriterGetInfoFromTable_QualifiesExecuteTable(t *testing.T) {
	tests := []struct {
		name      string
		database  string
		table     string
		query     string
		wantStart string
	}{
		{
			name:      "delete",
			database:  "sales",
			table:     "orders",
			query:     "DELETE FROM sales.orders WHERE id > 0",
			wantStart: "DELETE FROM `sales`.`orders` WHERE ",
		},
		{
			name:      "update",
			database:  "sales",
			table:     "orders",
			query:     "UPDATE sales.orders SET status = 1 WHERE id > 0",
			wantStart: "UPDATE `sales`.`orders` SET status = 1 WHERE ",
		},
		{
			name:      "update schema contains set",
			database:  "asset",
			table:     "orders",
			query:     "UPDATE asset.orders SET status = 1 WHERE id > 0",
			wantStart: "UPDATE `asset`.`orders` SET status = 1 WHERE ",
		},
		{
			name:      "update preserves operator spelling",
			database:  "sales",
			table:     "orders",
			query:     "UPDATE sales.orders SET status = left_part || right_part WHERE id > 0",
			wantStart: "UPDATE `sales`.`orders` SET status = left_part || right_part WHERE ",
		},
		{
			name:      "update preserves semicolon in string literal",
			database:  "sales",
			table:     "orders",
			query:     "UPDATE sales.orders SET note = 'a;b' WHERE id > 0;",
			wantStart: "UPDATE `sales`.`orders` SET note = 'a;b' WHERE ",
		},
		{
			name:      "update ignores where in nested expression",
			database:  "sales",
			table:     "orders",
			query:     "UPDATE sales.orders SET status = (SELECT max(status) FROM archive WHERE archive.id = orders.id) WHERE id > 0",
			wantStart: "UPDATE `sales`.`orders` SET status = (SELECT max(status) FROM archive WHERE archive.id = orders.id) WHERE ",
		},
		{
			name:      "update ignores keywords in literals and comments",
			database:  "sales",
			table:     "orders",
			query:     "UPDATE sales.orders /* SET fake */ SET note = 'WHERE and SET' /* WHERE fake */ WHERE id > 0",
			wantStart: "UPDATE `sales`.`orders` SET note = 'WHERE and SET' /* WHERE fake */ WHERE ",
		},
		{
			name:      "escaped identifiers",
			database:  "sales`archive",
			table:     "order`items",
			query:     "DELETE FROM `sales``archive`.`order``items` WHERE id > 0",
			wantStart: "DELETE FROM `sales``archive`.`order``items` WHERE ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := newGetInfoTestDB(t, getInfoPlan{version: "8.0.36"})
			w := &Writer{
				MysqlClient: db,
				ExecuteSQL:  tt.query,
				Database:    tt.database,
				Table:       tt.table,
			}

			if err := w.getInfoFromTable(&conf.Config{ExecuteQuery: tt.query}); err != nil {
				t.Fatalf("getInfoFromTable returned err: %v", err)
			}
			if !strings.HasPrefix(w.ExecuteSQL, tt.wantStart) {
				t.Fatalf("ExecuteSQL = %q, want prefix %q", w.ExecuteSQL, tt.wantStart)
			}
		})
	}
}

func TestWriterGetInfoFromTable_FreezeNowUsesDatabaseTime(t *testing.T) {
	db, state := newGetInfoTestDB(t, getInfoPlan{
		version:      "8.0.36",
		dbNowLiteral: "2026-06-03 18:19:20",
	})
	w := newWriterGetInfoForTest(db)
	w.ExecuteSQL = "DELETE FROM `t1` WHERE `create_time` <= date_sub(now(), interval 15 day)"

	err := w.getInfoFromTable(&conf.Config{
		ExecuteQuery: w.ExecuteSQL,
	})
	if err != nil {
		t.Fatalf("getInfoFromTable returned err: %v", err)
	}
	if !strings.Contains(w.OriginWhereClause, "2026-06-03 18:19:20") {
		t.Fatalf("OriginWhereClause should contain DB current time literal, got: %s", w.OriginWhereClause)
	}
	if strings.Contains(strings.ToLower(w.OriginWhereClause), "now(") {
		t.Fatalf("OriginWhereClause should not keep NOW(), got: %s", w.OriginWhereClause)
	}
	assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{"version", "db_now", "show_create"})
}

func TestWriterGetInfoFromTable_ErrorPaths(t *testing.T) {
	t.Run("version_query_failed", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			versionErr: errors.New("version query failed"),
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery: w.ExecuteSQL,
		})
		if err == nil || !strings.Contains(err.Error(), "query version failed") {
			t.Fatalf("expected query version failed error, got: %v", err)
		}
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{"version"})
	})

	t.Run("db_now_query_failed", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			version:  "8.0.36",
			dbNowErr: errors.New("db now failed"),
		})
		w := newWriterGetInfoForTest(db)
		w.ExecuteSQL = "DELETE FROM `t1` WHERE `create_time` <= now()"
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery: w.ExecuteSQL,
		})
		if err == nil || !strings.Contains(err.Error(), "query database current time failed") {
			t.Fatalf("expected query database current time failed error, got: %v", err)
		}
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{"version", "db_now"})
	})

	t.Run("oceanbase_set_session_failed", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			version:       "5.7.25-OceanBase-v4.2.5.4",
			setSessionErr: errors.New("set session failed"),
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery: w.ExecuteSQL,
		})
		if err == nil || !strings.Contains(err.Error(), "set oceanbase ddl compat mode failed") {
			t.Fatalf("expected set oceanbase ddl compat mode failed error, got: %v", err)
		}
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{"version", "set_ob_compat"})
	})

	t.Run("show_create_failed", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			version:       "8.0.36",
			showCreateErr: errors.New("show create failed"),
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery: w.ExecuteSQL,
		})
		if err == nil || !strings.Contains(err.Error(), "show create table `test_db`.`t1` failed") {
			t.Fatalf("expected show create table failed error, got: %v", err)
		}
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{"version", "show_create"})
	})

	t.Run("oceanbase_show_index_failed", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			version:      "5.7.25-OceanBase-v4.2.5.4",
			showIndexErr: errors.New("show index failed"),
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery:        w.ExecuteSQL,
			ForceChunkingColumn: "id",
		})
		if err == nil || !strings.Contains(err.Error(), "show index `test_db`.`t1` failed") {
			t.Fatalf("expected show index failure, got: %v", err)
		}
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{
			"version", "set_ob_compat", "show_create", "show_index",
		})
	})
}

func assertUniqueKeyColumns(t *testing.T, uk *UnqKeys, tp int, wantColumns []string) {
	t.Helper()
	if uk == nil {
		t.Fatalf("expected unique key to be selected")
	}
	if uk.Tp != tp {
		t.Fatalf("unexpected key type, got %d want %d", uk.Tp, tp)
	}
	if len(uk.UniqueKeyColumns) != len(wantColumns) {
		t.Fatalf("unexpected key column count, got %v want %v", uk.UniqueKeyColumns, wantColumns)
	}
	for i := range wantColumns {
		if uk.UniqueKeyColumns[i] != wantColumns[i] {
			t.Fatalf("unexpected key columns, got %v want %v", uk.UniqueKeyColumns, wantColumns)
		}
	}
}

func TestWriterGetInfoFromTable_OceanBaseUniqueKeyDiscovery(t *testing.T) {
	showIndexRows := [][]driver.Value{
		{
			[]byte("t1"),
			[]byte("0"),
			[]byte("PRIMARY"),
			[]byte("1"),
			[]byte("id"),
		},
		{
			[]byte("t1"),
			[]byte("0"),
			[]byte("PRIMARY"),
			[]byte("2"),
			[]byte("user_id"),
		},
		{
			[]byte("t1"),
			[]byte("0"),
			[]byte("uk_order_code"),
			[]byte("1"),
			[]byte("order_code"),
		},
	}

	t.Run("force_chunking_column_can_use_ob_unique_key", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			version:        "5.7.25-OceanBase-v4.2.5.4",
			createTableDDL: oceanBaseCompatCreateTableDDL,
			showIndexRows:  showIndexRows,
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery:        w.ExecuteSQL,
			ForceChunkingColumn: "order_code",
		})
		if err != nil {
			t.Fatalf("getInfoFromTable returned err: %v", err)
		}

		assertUniqueKeyColumns(t, w.unqKeys, vars.ConstraintUniqKey, []string{"order_code"})
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{
			"version", "set_ob_compat", "show_create", "show_index",
		})
	})

	t.Run("default_selection_still_prefers_primary_key", func(t *testing.T) {
		db, _ := newGetInfoTestDB(t, getInfoPlan{
			version:        "5.7.25-OceanBase-v4.2.5.4",
			createTableDDL: oceanBaseCompatCreateTableDDL,
			showIndexRows:  showIndexRows,
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery: w.ExecuteSQL,
		})
		if err != nil {
			t.Fatalf("getInfoFromTable returned err: %v", err)
		}

		assertUniqueKeyColumns(t, w.unqKeys, vars.ConstraintPrimaryKey, []string{"id", "user_id"})
	})

	t.Run("show_index_failure_without_force_falls_back_to_ddl", func(t *testing.T) {
		db, state := newGetInfoTestDB(t, getInfoPlan{
			version:        "5.7.25-OceanBase-v4.2.5.4",
			createTableDDL: oceanBaseCompatCreateTableDDL,
			showIndexErr:   errors.New("show index failed"),
		})
		w := newWriterGetInfoForTest(db)
		err := w.getInfoFromTable(&conf.Config{
			ExecuteQuery: w.ExecuteSQL,
		})
		if err != nil {
			t.Fatalf("getInfoFromTable returned err: %v", err)
		}

		assertUniqueKeyColumns(t, w.unqKeys, vars.ConstraintPrimaryKey, []string{"id", "user_id"})
		assertGetInfoCalls(t, snapshotGetInfoCalls(state), []string{
			"version", "set_ob_compat", "show_create", "show_index",
		})
	})
}

func TestWriterGetInfoFromTable_ForceChunkingColumnNormalization(t *testing.T) {
	db, _ := newGetInfoTestDB(t, getInfoPlan{
		version:        "8.0.36",
		createTableDDL: compositePrimaryCreateTableDDL,
	})
	w := newWriterGetInfoForTest(db)
	err := w.getInfoFromTable(&conf.Config{
		ExecuteQuery:        w.ExecuteSQL,
		ForceChunkingColumn: "id, user_id",
	})
	if err != nil {
		t.Fatalf("getInfoFromTable returned err: %v", err)
	}

	assertUniqueKeyColumns(t, w.unqKeys, vars.ConstraintPrimaryKey, []string{"id", "user_id"})
}
