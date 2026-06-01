package preflight

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNeedsConfirmation(t *testing.T) {
	tests := []struct {
		name        string
		r           Result
		autoConfirm bool
		want        bool
	}{
		{"below threshold", Result{EstimatedRows: 100, Threshold: 1000}, false, false},
		{"at threshold", Result{EstimatedRows: 1000, Threshold: 1000}, false, true},
		{"above threshold", Result{EstimatedRows: 5000, Threshold: 1000}, false, true},
		{"auto confirm skips", Result{EstimatedRows: 5000, Threshold: 1000}, true, false},
		{"zero threshold disables", Result{EstimatedRows: 5000, Threshold: 0}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NeedsConfirmation(tt.r, tt.autoConfirm); got != tt.want {
				t.Errorf("NeedsConfirmation = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfirmLargeDelete(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"yes\n", true},
		{"YES\n", true},
		{" yes \n", true},
		{"no\n", false},
		{"\n", false},
		{"", false},
	}
	for _, tt := range tests {
		var out strings.Builder
		got, err := ConfirmLargeDelete(strings.NewReader(tt.input), &out, Result{EstimatedRows: 1, Threshold: 1})
		if err != nil {
			t.Fatalf("ConfirmLargeDelete(%q) err: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ConfirmLargeDelete(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// ---- EstimateRows over a fake EXPLAIN result ----

type explainFakeDriverState struct {
	columns  []string
	rows     [][]driver.Value
	queryErr error
}

type explainFakeDriver struct{ state *explainFakeDriverState }
type explainFakeConn struct{ state *explainFakeDriverState }
type explainFakeRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

var explainDriverSeq atomic.Int64

func (d *explainFakeDriver) Open(_ string) (driver.Conn, error) {
	return &explainFakeConn{state: d.state}, nil
}
func (c *explainFakeConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not implemented")
}
func (c *explainFakeConn) Close() error              { return nil }
func (c *explainFakeConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("no tx") }
func (c *explainFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.state.queryErr != nil {
		return nil, c.state.queryErr
	}
	if !strings.Contains(strings.ToUpper(query), "EXPLAIN") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	return &explainFakeRows{columns: c.state.columns, rows: c.state.rows}, nil
}
func (r *explainFakeRows) Columns() []string { return r.columns }
func (r *explainFakeRows) Close() error      { return nil }
func (r *explainFakeRows) Next(dest []driver.Value) error {
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

func newExplainDB(t *testing.T, state *explainFakeDriverState) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("explain_fake_driver_%d", explainDriverSeq.Add(1))
	sql.Register(name, &explainFakeDriver{state: state})
	db, err := sql.Open(name, "explain-test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestEstimateRows_MySQLTabular(t *testing.T) {
	db := newExplainDB(t, &explainFakeDriverState{
		columns: []string{"id", "select_type", "table", "type", "rows", "Extra"},
		rows: [][]driver.Value{
			{[]byte("1"), []byte("DELETE"), []byte("t"), []byte("range"), []byte("42000"), []byte("Using where")},
		},
	})
	n, err := EstimateRows(context.Background(), db, "`d`.`t`", "(id > 0)")
	if err != nil {
		t.Fatalf("EstimateRows err: %v", err)
	}
	if n != 42000 {
		t.Fatalf("estimated = %d, want 42000", n)
	}
}

func TestEstimateRows_OceanBasePlanText(t *testing.T) {
	plan := "===========================================\n" +
		"|ID|OPERATOR        |NAME|EST.ROWS|COST |\n" +
		"-------------------------------------------\n" +
		"|0 |TABLE FULL SCAN |t   |123456  |1000 |\n" +
		"==========================================="
	db := newExplainDB(t, &explainFakeDriverState{
		columns: []string{"Query Plan"},
		rows:    [][]driver.Value{{[]byte(plan)}},
	})
	n, err := EstimateRows(context.Background(), db, "`d`.`t`", "(id > 0)")
	if err != nil {
		t.Fatalf("EstimateRows err: %v", err)
	}
	if n != 123456 {
		t.Fatalf("estimated = %d, want 123456", n)
	}
}

func TestEstimateRows_NoEstimateReturnsZero(t *testing.T) {
	db := newExplainDB(t, &explainFakeDriverState{
		columns: []string{"Query Plan"},
		rows:    [][]driver.Value{{[]byte("no numbers here")}},
	})
	n, err := EstimateRows(context.Background(), db, "`d`.`t`", "")
	if err != nil {
		t.Fatalf("EstimateRows err: %v", err)
	}
	if n != 0 {
		t.Fatalf("estimated = %d, want 0", n)
	}
}

func TestEstimateRows_QueryErrorPropagates(t *testing.T) {
	db := newExplainDB(t, &explainFakeDriverState{queryErr: fmt.Errorf("explain boom")})
	_, err := EstimateRows(context.Background(), db, "`d`.`t`", "")
	if err == nil || !strings.Contains(err.Error(), "explain") {
		t.Fatalf("expected explain error, got: %v", err)
	}
}
