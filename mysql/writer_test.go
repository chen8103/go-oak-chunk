package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juju/ratelimit"
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
				errCh <- w.Write(ctx, ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1), bucketNum)
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
		errCh <- w.Write(ctx, ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1), make(chan int64, 1))
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
