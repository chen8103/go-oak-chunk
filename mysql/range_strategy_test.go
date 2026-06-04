package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tidbmysql "github.com/pingcap/tidb/parser/mysql"
)

type rangeStrategyFakeState struct {
	mu sync.Mutex

	rows       int
	queryCalls int
	execCalls  int

	blockQueryCall    int
	blockQueryStarted chan struct{}
	blockQueryErr     error
	blockQueryOnce    sync.Once
}

type rangeStrategyFakeDriver struct {
	state *rangeStrategyFakeState
}

type rangeStrategyFakeConn struct {
	state *rangeStrategyFakeState
}

type rangeStrategyFakeTx struct {
	state *rangeStrategyFakeState
}

type rangeStrategyFakeRows struct {
	rows [][]driver.Value
	idx  int
}

var rangeStrategyDriverSeq atomic.Int64

func (d *rangeStrategyFakeDriver) Open(_ string) (driver.Conn, error) {
	return &rangeStrategyFakeConn{state: d.state}, nil
}

func (c *rangeStrategyFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not implemented")
}

func (c *rangeStrategyFakeConn) Close() error {
	return nil
}

func (c *rangeStrategyFakeConn) Begin() (driver.Tx, error) {
	return &rangeStrategyFakeTx{state: c.state}, nil
}

func (c *rangeStrategyFakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &rangeStrategyFakeTx{state: c.state}, nil
}

func (c *rangeStrategyFakeConn) QueryContext(
	ctx context.Context, query string, _ []driver.NamedValue,
) (driver.Rows, error) {
	if !strings.Contains(strings.ToUpper(query), "SELECT") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}

	c.state.mu.Lock()
	c.state.queryCalls++
	queryCall := c.state.queryCalls
	rowCount := c.state.rows
	blockQueryCall := c.state.blockQueryCall
	blockQueryStarted := c.state.blockQueryStarted
	blockQueryErr := c.state.blockQueryErr
	c.state.mu.Unlock()

	if blockQueryCall > 0 && queryCall == blockQueryCall {
		if blockQueryStarted != nil {
			c.state.blockQueryOnce.Do(func() {
				close(blockQueryStarted)
			})
		}
		<-ctx.Done()
		if blockQueryErr != nil {
			return nil, blockQueryErr
		}
		return nil, ctx.Err()
	}

	rows := make([][]driver.Value, 0, rowCount)
	for i := 1; i <= rowCount; i++ {
		rows = append(rows, []driver.Value{[]byte(strconv.Itoa(i))})
	}
	return &rangeStrategyFakeRows{rows: rows}, nil
}

func (c *rangeStrategyFakeConn) ExecContext(
	_ context.Context, query string, _ []driver.NamedValue,
) (driver.Result, error) {
	if !strings.Contains(strings.ToUpper(query), "DELETE") {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}

	c.state.mu.Lock()
	c.state.execCalls++
	c.state.mu.Unlock()
	return driver.RowsAffected(1), nil
}

func (tx *rangeStrategyFakeTx) Commit() error {
	return nil
}

func (tx *rangeStrategyFakeTx) Rollback() error {
	return nil
}

func (r *rangeStrategyFakeRows) Columns() []string {
	return []string{"id"}
}

func (r *rangeStrategyFakeRows) Close() error {
	return nil
}

func (r *rangeStrategyFakeRows) Next(dest []driver.Value) error {
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

func newRangeStrategyTestDB(t *testing.T, rows int) (*sql.DB, *rangeStrategyFakeState) {
	t.Helper()

	state := &rangeStrategyFakeState{rows: rows}
	name := fmt.Sprintf("range_strategy_fake_driver_%d", rangeStrategyDriverSeq.Add(1))
	sql.Register(name, &rangeStrategyFakeDriver{state: state})
	db, err := sql.Open(name, "range-strategy-test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, state
}

func TestRangeStrategyRun_CancelsProducerWhenWriterStopsOnGuardrail(t *testing.T) {
	db, state := newRangeStrategyTestDB(t, 100)
	w := &Writer{
		MysqlClient:       db,
		ExecuteSQL:        "DELETE FROM `t` WHERE (id > 0)",
		OriginWhereClause: "(id > 0)",
		ChunkSize:         1,
		TxnSize:           1,
		Database:          "d",
		Table:             "t",
		unqKeys: &UnqKeys{
			UniqueKeyColumns: []string{"id"},
			UniqueKeyTypes:   []byte{tidbmysql.TypeLonglong},
			IsNull:           []bool{false},
		},
		ProducerQueue: make(chan *Producer, 2),
		MaxRows:       1,
	}

	bucketNum := make(chan int64, 1)
	bucketNum <- 0

	errCh := make(chan error, 1)
	go func() {
		errCh <- NewRangeStrategy(w).Run(context.Background(), RunParams{
			Bucket:    noopBucket{},
			BucketNum: bucketNum,
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out after writer stopped on max-rows")
	}

	if got := w.GetRowAffects(); got != 1 {
		t.Fatalf("rowAffects = %d, want 1", got)
	}
	state.mu.Lock()
	queryCalls := state.queryCalls
	execCalls := state.execCalls
	state.mu.Unlock()
	if queryCalls == 0 {
		t.Fatal("expected producer to query candidate rows")
	}
	if execCalls != 1 {
		t.Fatalf("execCalls = %d, want 1", execCalls)
	}
}

func TestRangeStrategyRun_IgnoresProducerErrorAfterGuardrailCancelMidQuery(t *testing.T) {
	db, state := newRangeStrategyTestDB(t, 1)
	state.blockQueryCall = 2
	state.blockQueryStarted = make(chan struct{})
	state.blockQueryErr = fmt.Errorf("driver-specific query cancel")

	w := &Writer{
		MysqlClient:       db,
		ExecuteSQL:        "DELETE FROM `t` WHERE (id > 0)",
		OriginWhereClause: "(id > 0)",
		ChunkSize:         1,
		TxnSize:           1,
		Database:          "d",
		Table:             "t",
		unqKeys: &UnqKeys{
			UniqueKeyColumns: []string{"id"},
			UniqueKeyTypes:   []byte{tidbmysql.TypeLonglong},
			IsNull:           []bool{false},
		},
		ProducerQueue: make(chan *Producer, 8),
		MaxRows:       1,
	}

	bucketNum := make(chan int64, 1)
	bucketNum <- 0

	errCh := make(chan error, 1)
	go func() {
		errCh <- NewRangeStrategy(w).Run(context.Background(), RunParams{
			Bucket:    noopBucket{},
			BucketNum: bucketNum,
		})
	}()

	select {
	case <-state.blockQueryStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not enter the mid-query cancel path")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run timed out after canceling producer mid-query")
	}

	if got := w.GetRowAffects(); got != 1 {
		t.Fatalf("rowAffects = %d, want 1", got)
	}
	state.mu.Lock()
	queryCalls := state.queryCalls
	execCalls := state.execCalls
	state.mu.Unlock()
	if queryCalls != 2 {
		t.Fatalf("queryCalls = %d, want 2", queryCalls)
	}
	if execCalls != 1 {
		t.Fatalf("execCalls = %d, want 1", execCalls)
	}
}
