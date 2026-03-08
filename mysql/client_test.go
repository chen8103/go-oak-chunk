package mysql

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
)

type versionRowsPlan struct {
	version      string
	blockVersion bool
}

type versionDriverState struct {
	plan versionRowsPlan
}

type versionFakeDriver struct {
	state *versionDriverState
}

type versionFakeConn struct {
	state *versionDriverState
}

type versionFakeRows struct {
	version string
	done    bool
}

var versionDriverSeq atomic.Int64

func (d *versionFakeDriver) Open(_ string) (driver.Conn, error) {
	return &versionFakeConn{state: d.state}, nil
}

func (c *versionFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in version fake conn")
}

func (c *versionFakeConn) Close() error {
	return nil
}

func (c *versionFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not implemented in version fake conn")
}

func (c *versionFakeConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(strings.ToLower(query), "@@version") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	if c.state.plan.blockVersion {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &versionFakeRows{version: c.state.plan.version}, nil
}

func (c *versionFakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	_ = args
	return c.QueryContext(context.Background(), query, nil)
}

func (r *versionFakeRows) Columns() []string {
	return []string{"@@version"}
}

func (r *versionFakeRows) Close() error {
	return nil
}

func (r *versionFakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = []byte(r.version)
	return nil
}

func newVersionTestDB(t *testing.T, plan versionRowsPlan) *sql.DB {
	t.Helper()
	state := &versionDriverState{plan: plan}
	driverName := fmt.Sprintf("version_fake_driver_%d", versionDriverSeq.Add(1))
	sql.Register(driverName, &versionFakeDriver{state: state})

	db, err := sql.Open(driverName, "version-test")
	if err != nil {
		t.Fatalf("open version fake db failed: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestCheckVersionContext(t *testing.T) {
	t.Run("returns version normally", func(t *testing.T) {
		db := newVersionTestDB(t, versionRowsPlan{version: "8.0.36"})
		got, err := CheckVersionContext(context.Background(), db)
		if err != nil {
			t.Fatalf("CheckVersionContext returned err: %v", err)
		}
		if got != "8.0.36" {
			t.Fatalf("version = %q, want %q", got, "8.0.36")
		}
	})

	t.Run("respects context cancel", func(t *testing.T) {
		db := newVersionTestDB(t, versionRowsPlan{blockVersion: true})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			_, err := CheckVersionContext(ctx, db)
			errCh <- err
		}()

		time.Sleep(20 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout waiting CheckVersionContext cancel result")
		}
	})
}
