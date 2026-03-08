package oak

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

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
	"github.com/SisyphusSQ/go-oak-chunk/v3/task"
)

type executorDBPlan struct {
	blockBeginUntilCancel bool
}

type executorTxState struct {
	commitCalls   int
	rollbackCalls int
}

type executorDriverState struct {
	mu sync.Mutex

	plan executorDBPlan

	beginCalls int
	txs        []*executorTxState
	beginCh    chan struct{}
}

type executorFakeDriver struct {
	state *executorDriverState
}

type executorFakeConn struct {
	state *executorDriverState
}

type executorFakeTx struct {
	state   *executorDriverState
	txState *executorTxState
}

var executorDriverSeq atomic.Int64

func (d *executorFakeDriver) Open(_ string) (driver.Conn, error) {
	return &executorFakeConn{state: d.state}, nil
}

func (c *executorFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in executor fake conn")
}

func (c *executorFakeConn) Close() error {
	return nil
}

func (c *executorFakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *executorFakeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.state.mu.Lock()
	c.state.beginCalls++
	c.state.mu.Unlock()

	select {
	case c.state.beginCh <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if c.state.plan.blockBeginUntilCancel {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	txState := &executorTxState{}
	c.state.mu.Lock()
	c.state.txs = append(c.state.txs, txState)
	c.state.mu.Unlock()
	return &executorFakeTx{state: c.state, txState: txState}, nil
}

func (tx *executorFakeTx) Commit() error {
	tx.state.mu.Lock()
	tx.txState.commitCalls++
	tx.state.mu.Unlock()
	return nil
}

func (tx *executorFakeTx) Rollback() error {
	tx.state.mu.Lock()
	tx.txState.rollbackCalls++
	tx.state.mu.Unlock()
	return nil
}

func newExecutorTestDB(t *testing.T, plan executorDBPlan) (*sql.DB, *executorDriverState) {
	t.Helper()

	state := &executorDriverState{
		plan:    plan,
		beginCh: make(chan struct{}, 16),
	}
	driverName := fmt.Sprintf("executor_fake_driver_%d", executorDriverSeq.Add(1))
	sql.Register(driverName, &executorFakeDriver{state: state})

	db, err := sql.Open(driverName, "executor-test")
	if err != nil {
		t.Fatalf("open fake db failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func newExecutorForTest(db *sql.DB) *Executor {
	cfg := &conf.Config{
		ChunkSize:    0,
		ExecuteQuery: "DELETE FROM t WHERE id > 0",
		Database:     "test_db",
		NoSlaves:     true,
	}

	w := &mysql.Writer{
		MysqlClient:   db,
		ExecuteSQL:    "DELETE FROM `t` WHERE ",
		ChunkSize:     0,
		TxnSize:       10,
		ProducerQueue: make(chan *mysql.Producer, 16),
		Database:      "test_db",
		Table:         "t",
	}
	w.SetCostTime(time.Millisecond)

	return &Executor{
		config:           cfg,
		rateLimiter:      task.NewRateLimiter(0, 0, 0, false),
		writer:           w,
		progressInterval: 20 * time.Millisecond,
	}
}

func TestExecutor_RunReuseBehavior(t *testing.T) {
	db, _ := newExecutorTestDB(t, executorDBPlan{})
	executor := newExecutorForTest(db)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	if err := executor.Run(ctx1); err != nil {
		t.Fatalf("first run should succeed, got err: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	err := executor.Run(ctx2)
	if err == nil {
		t.Fatal("second run should be rejected, got nil error")
	}
	if !errors.Is(err, ErrExecutorAlreadyRun) {
		t.Fatalf("second run should return ErrExecutorAlreadyRun, got: %v", err)
	}
}

func TestExecutor_RunReuseBehavior_FirstRunCanceledStillOneShot(t *testing.T) {
	db, state := newExecutorTestDB(t, executorDBPlan{blockBeginUntilCancel: true})
	executor := newExecutorForTest(db)

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- executor.Run(runCtx)
	}()

	select {
	case <-state.beginCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting first BeginTx")
	}
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, task.ErrExecutionStopped) {
			t.Fatalf("first run should return task.ErrExecutionStopped after cancel, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting first run cancel result")
	}

	err := executor.Run(context.Background())
	if !errors.Is(err, ErrExecutorAlreadyRun) {
		t.Fatalf("second run after canceled first run should return ErrExecutorAlreadyRun, got: %v", err)
	}
}

func TestExecutor_StopAndCancelSemantics(t *testing.T) {
	tests := []struct {
		name  string
		runFn func(t *testing.T, e *Executor, s *executorDriverState)
	}{
		{
			name: "stop_without_active_run_is_idempotent",
			runFn: func(t *testing.T, e *Executor, _ *executorDriverState) {
				t.Helper()
				e.Stop()
				e.Stop()
			},
		},
		{
			name: "parent_context_canceled_returns_stopped_error",
			runFn: func(t *testing.T, e *Executor, _ *executorDriverState) {
				t.Helper()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := e.Run(ctx)
				if !errors.Is(err, task.ErrExecutionStopped) {
					t.Fatalf("expected task.ErrExecutionStopped, got %v", err)
				}
			},
		},
		{
			name: "stop_during_run_returns_stopped_error",
			runFn: func(t *testing.T, e *Executor, s *executorDriverState) {
				t.Helper()
				errCh := make(chan error, 1)
				go func() {
					errCh <- e.Run(context.Background())
				}()

				select {
				case <-s.beginCh:
				case <-time.After(2 * time.Second):
					t.Fatal("timeout waiting writer BeginTx")
				}

				e.Stop()

				select {
				case err := <-errCh:
					if !errors.Is(err, task.ErrExecutionStopped) {
						t.Fatalf("expected task.ErrExecutionStopped after Stop, got %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("timeout waiting run result after Stop")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := executorDBPlan{}
			if tt.name == "stop_during_run_returns_stopped_error" {
				plan.blockBeginUntilCancel = true
			}
			db, state := newExecutorTestDB(t, plan)
			tt.runFn(t, newExecutorForTest(db), state)
		})
	}
}
