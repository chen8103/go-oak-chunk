package lag_checker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
)

type lagRowsPlan struct {
	columns   []string
	rows      [][]driver.Value
	nextErrAt int
	nextErr   error
}

type lagDriverState struct {
	plan lagRowsPlan

	closeCalls atomic.Int64
}

type lagFakeDriver struct {
	state *lagDriverState
}

type lagFakeConn struct {
	state *lagDriverState
}

type lagFakeRows struct {
	state *lagDriverState
	plan  lagRowsPlan
	index int
}

var lagDriverSeq atomic.Int64

func (d *lagFakeDriver) Open(_ string) (driver.Conn, error) {
	return &lagFakeConn{state: d.state}, nil
}

func (c *lagFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not implemented in lag fake conn")
}

func (c *lagFakeConn) Close() error {
	return nil
}

func (c *lagFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not implemented in lag fake conn")
}

func (c *lagFakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return &lagFakeRows{
		state: c.state,
		plan:  c.state.plan,
	}, nil
}

func (c *lagFakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	_ = query
	_ = args
	return c.QueryContext(context.Background(), "", nil)
}

func (r *lagFakeRows) Columns() []string {
	return r.plan.columns
}

func (r *lagFakeRows) Close() error {
	r.state.closeCalls.Add(1)
	return nil
}

func (r *lagFakeRows) Next(dest []driver.Value) error {
	if r.plan.nextErr != nil && r.index == r.plan.nextErrAt {
		r.index++
		return r.plan.nextErr
	}

	if r.index >= len(r.plan.rows) {
		return io.EOF
	}

	row := r.plan.rows[r.index]
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

func newLagTestDB(t *testing.T, plan lagRowsPlan) (*sql.DB, *lagDriverState) {
	t.Helper()

	state := &lagDriverState{plan: plan}
	driverName := fmt.Sprintf("lag_fake_driver_%d", lagDriverSeq.Add(1))
	sql.Register(driverName, &lagFakeDriver{state: state})

	db, err := sql.Open(driverName, "lag-test")
	if err != nil {
		t.Fatalf("open lag fake db failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db, state
}

func TestSlaveChecker_GetSlaveLag_RowsFinalizationRisks(t *testing.T) {
	tests := []struct {
		name           string
		plan           lagRowsPlan
		wantErr        bool
		wantLag        int64
		wantCloseCalls *int64
	}{
		{
			name: "parse_error_still_closes_rows",
			plan: lagRowsPlan{
				columns: []string{"Seconds_Behind_Master"},
				rows: [][]driver.Value{
					{[]byte("not-a-number")},
				},
				nextErrAt: -1,
			},
			wantErr:        true,
			wantLag:        0,
			wantCloseCalls: int64Ptr(1),
		},
		{
			name: "rows_next_error_is_propagated",
			plan: lagRowsPlan{
				columns:   []string{"Seconds_Behind_Master"},
				rows:      [][]driver.Value{},
				nextErrAt: 0,
				nextErr:   errors.New("rows next failed"),
			},
			wantErr:        true,
			wantLag:        0,
			wantCloseCalls: int64Ptr(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, state := newLagTestDB(t, tt.plan)
			sc := &SlaveChecker{}

			lag, err := sc.getSlaveLag(context.Background(), db, "SHOW REPLICA STATUS")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
			} else if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}

			if lag != tt.wantLag {
				t.Fatalf("lag = %d, want %d", lag, tt.wantLag)
			}

			if tt.wantCloseCalls != nil {
				if got := state.closeCalls.Load(); got != *tt.wantCloseCalls {
					t.Fatalf("rows close calls = %d, want %d", got, *tt.wantCloseCalls)
				}
			}
		})
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}
