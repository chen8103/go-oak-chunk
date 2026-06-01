package task

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	parser_mysql "github.com/pingcap/tidb/parser/mysql"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
)

type executeDriverPlan struct {
	queryErr   error
	beginTxErr error
}

type executeDriverState struct {
	plan executeDriverPlan
}

type executeFakeDriver struct {
	state *executeDriverState
}

type executeFakeConn struct {
	state *executeDriverState
}

type executeFakeTx struct{}

type executeFakeRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

var executeDriverSeq atomic.Int64

func (d *executeFakeDriver) Open(_ string) (driver.Conn, error) {
	return &executeFakeConn{state: d.state}, nil
}

func (c *executeFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in execute fake conn")
}

func (c *executeFakeConn) Close() error {
	return nil
}

func (c *executeFakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *executeFakeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if c.state.plan.beginTxErr != nil {
		return nil, c.state.plan.beginTxErr
	}
	return &executeFakeTx{}, nil
}

func (c *executeFakeConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return driver.RowsAffected(1), nil
}

func (c *executeFakeConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	_ = query
	_ = args
	return c.ExecContext(context.Background(), "", nil)
}

func (c *executeFakeConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if c.state.plan.queryErr != nil {
		return nil, c.state.plan.queryErr
	}

	return &executeFakeRows{
		columns: []string{"id"},
		rows: [][]driver.Value{
			{[]byte("1")},
		},
	}, nil
}

func (c *executeFakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	_ = query
	_ = args
	return c.QueryContext(context.Background(), "", nil)
}

func (tx *executeFakeTx) Commit() error {
	return nil
}

func (tx *executeFakeTx) Rollback() error {
	return nil
}

func (r *executeFakeRows) Columns() []string {
	return r.columns
}

func (r *executeFakeRows) Close() error {
	return nil
}

func (r *executeFakeRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.index]
	r.index++
	for i := range dest {
		if i < len(row) {
			dest[i] = row[i]
		} else {
			dest[i] = nil
		}
	}
	return nil
}

func newExecuteTestDB(t *testing.T, plan executeDriverPlan) *sql.DB {
	t.Helper()
	state := &executeDriverState{plan: plan}
	driverName := fmt.Sprintf("execute_fake_driver_%d", executeDriverSeq.Add(1))
	sql.Register(driverName, &executeFakeDriver{state: state})

	db, err := sql.Open(driverName, "execute-test")
	if err != nil {
		t.Fatalf("open execute fake db failed: %v", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func newExecuteTestWriter(db *sql.DB, chunkSize int64) *mysql.Writer {
	w := &mysql.Writer{
		MysqlClient:   db,
		ExecuteSQL:    "UPDATE `t` SET `c` = 1 WHERE ",
		ChunkSize:     chunkSize,
		TxnSize:       100,
		ProducerQueue: make(chan *mysql.Producer, 16),
		Database:      "test_db",
		Table:         "t",
	}
	w.SetCostTime(time.Millisecond)
	return w
}

func setWriterUniqueKeysForExecuteTest(t *testing.T, w *mysql.Writer, keys *mysql.UnqKeys) {
	t.Helper()
	field := reflect.ValueOf(w).Elem().FieldByName("unqKeys")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(keys))
}

func TestNormalizeStopError_ContextCanceledIsUnified(t *testing.T) {
	tests := []struct {
		name string
		in   error
		want error
	}{
		{
			name: "nil_error",
			in:   nil,
			want: nil,
		},
		{
			name: "plain_context_canceled",
			in:   context.Canceled,
			want: ErrExecutionStopped,
		},
		{
			name: "wrapped_context_canceled",
			in:   fmt.Errorf("wrapped: %w", context.Canceled),
			want: ErrExecutionStopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeStopError(tt.in)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("normalizeStopError() = %v, want nil", got)
				}
				return
			}
			if !errors.Is(got, tt.want) {
				t.Fatalf("normalizeStopError() = %v, want %v", got, tt.want)
			}
			if got != tt.want {
				t.Fatalf("normalizeStopError() should return sentinel %v, got %v", tt.want, got)
			}
		})
	}

	t.Run("non_stop_error_passthrough", func(t *testing.T) {
		origin := errors.New("some other error")
		got := normalizeStopError(origin)
		if got != origin {
			t.Fatalf("normalizeStopError() changed non-stop error, got %v, want %v", got, origin)
		}
	})
}

func TestExecute_ReadErrBranch_ReturnsProcedureError(t *testing.T) {
	readErr := errors.New("build sql failed in procedure")
	db := newExecuteTestDB(t, executeDriverPlan{
		queryErr: readErr,
	})
	writer := newExecuteTestWriter(db, 1)
	setWriterUniqueKeysForExecuteTest(t, writer, &mysql.UnqKeys{
		UniqueKeyColumns: []string{"id"},
		CountColumns:     1,
		UniqueKeyTypes:   []byte{parser_mysql.TypeLonglong},
		IsNull:           []bool{false},
	})

	err := Execute(context.Background(), &conf.Config{NoSlaves: true}, writer, RunOptions{
		RateLimiter:      NewRateLimiter(0, 0, 0, false),
		ProgressInterval: 5 * time.Millisecond,
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("Execute() = %v, want %v", err, readErr)
	}
}

func TestExecute_WriteErrBranch_CanceledIsNormalized(t *testing.T) {
	db := newExecuteTestDB(t, executeDriverPlan{
		beginTxErr: context.Canceled,
	})
	writer := newExecuteTestWriter(db, 0)

	err := Execute(context.Background(), &conf.Config{NoSlaves: true}, writer, RunOptions{
		RateLimiter:      NewRateLimiter(0, 0, 0, false),
		ProgressInterval: 5 * time.Millisecond,
	})
	if !errors.Is(err, ErrExecutionStopped) {
		t.Fatalf("Execute() should normalize context.Canceled to ErrExecutionStopped, got %v", err)
	}
}
