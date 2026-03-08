package lag_checker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

type initDriverPlan struct {
	blockVersion   bool
	blockHostQuery bool
}

type initDriverState struct {
	plan initDriverPlan

	versionQueryCalls atomic.Int64
	hostQueryCalls    atomic.Int64
}

type initFakeDriver struct {
	state *initDriverState
}

type initFakeConn struct {
	state *initDriverState
}

type initFakeRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

var initDriverSeq atomic.Int64

func (d *initFakeDriver) Open(_ string) (driver.Conn, error) {
	return &initFakeConn{state: d.state}, nil
}

func (c *initFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in init fake conn")
}

func (c *initFakeConn) Close() error {
	return nil
}

func (c *initFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not implemented in init fake conn")
}

func (c *initFakeConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	lower := strings.ToLower(query)
	switch {
	case strings.Contains(lower, "@@version"):
		c.state.versionQueryCalls.Add(1)
		if c.state.plan.blockVersion {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &initFakeRows{
			columns: []string{"@@version"},
			rows:    [][]driver.Value{{[]byte("8.0.36")}},
		}, nil
	case strings.Contains(lower, "show slave hosts"), strings.Contains(lower, "show replica hosts"):
		c.state.hostQueryCalls.Add(1)
		if c.state.plan.blockHostQuery {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &initFakeRows{
			columns: []string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"},
			rows:    [][]driver.Value{},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
}

func (c *initFakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	_ = args
	return c.QueryContext(context.Background(), query, nil)
}

func (r *initFakeRows) Columns() []string {
	return r.columns
}

func (r *initFakeRows) Close() error {
	return nil
}

func (r *initFakeRows) Next(dest []driver.Value) error {
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

func newInitTestDB(t *testing.T, plan initDriverPlan) (*sql.DB, *initDriverState) {
	t.Helper()
	state := &initDriverState{plan: plan}
	driverName := fmt.Sprintf("lag_init_driver_%d", initDriverSeq.Add(1))
	sql.Register(driverName, &initFakeDriver{state: state})
	db, err := sql.Open(driverName, "lag-init-test")
	if err != nil {
		t.Fatalf("open init fake db failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func TestNewSlaveCheckerWithContext_RespectsCancelDuringVersionCheck(t *testing.T) {
	db, state := newInitTestDB(t, initDriverPlan{blockVersion: true})

	cfg := &conf.Config{
		NoSlaves: false,
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := NewSlaveCheckerWithContext(ctx, db, cfg)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if got := state.versionQueryCalls.Load(); got != 1 {
			t.Fatalf("version query calls = %d, want 1", got)
		}
		if got := state.hostQueryCalls.Load(); got != 0 {
			t.Fatalf("host query calls = %d, want 0 when version check canceled", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting NewSlaveCheckerWithContext return")
	}
}

func TestNewSlaveCheckerWithContext_RespectsCancelDuringHostQuery(t *testing.T) {
	db, state := newInitTestDB(t, initDriverPlan{blockHostQuery: true})

	cfg := &conf.Config{
		NoSlaves: false,
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := NewSlaveCheckerWithContext(ctx, db, cfg)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if got := state.versionQueryCalls.Load(); got != 1 {
			t.Fatalf("version query calls = %d, want 1", got)
		}
		if got := state.hostQueryCalls.Load(); got != 1 {
			t.Fatalf("host query calls = %d, want 1", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting NewSlaveCheckerWithContext return")
	}
}
