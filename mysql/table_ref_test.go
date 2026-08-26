package mysql

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

func TestTargetTableMeta(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantSchema string
		wantTable  string
	}{
		{
			name:      "unqualified delete",
			sql:       "DELETE FROM orders WHERE id > 0",
			wantTable: "orders",
		},
		{
			name:       "qualified delete",
			sql:        "DELETE FROM sales.orders WHERE id > 0",
			wantSchema: "sales",
			wantTable:  "orders",
		},
		{
			name:       "quoted qualified delete",
			sql:        "DELETE FROM `sales-archive`.`order.items` WHERE id > 0",
			wantSchema: "sales-archive",
			wantTable:  "order.items",
		},
		{
			name:       "qualified update",
			sql:        "UPDATE sales.orders SET status = 1 WHERE id > 0",
			wantSchema: "sales",
			wantTable:  "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TargetTableMeta(tt.sql)
			if err != nil {
				t.Fatalf("TargetTableMeta() error = %v", err)
			}
			if got.Schema != tt.wantSchema || got.Table != tt.wantTable {
				t.Fatalf(
					"TargetTableMeta() = schema %q table %q, want schema %q table %q",
					got.Schema, got.Table, tt.wantSchema, tt.wantTable,
				)
			}
		})
	}
}

func TestWriterResolveTargetFromQualifiedSQL(t *testing.T) {
	c := &conf.Config{ExecuteQuery: "DELETE FROM sales.orders WHERE id > 0"}
	w := &Writer{ExecuteSQL: c.ExecuteQuery}

	if err := w.resolveTarget(c); err != nil {
		t.Fatalf("resolveTarget() error = %v", err)
	}
	if w.Database != "sales" || w.Table != "orders" {
		t.Fatalf("resolved writer target = %q.%q, want sales.orders", w.Database, w.Table)
	}
	if c.Database != "sales" {
		t.Fatalf("resolved config database = %q, want sales", c.Database)
	}
}

func TestWriterResolveTargetDoesNotRewriteConfiguredDatabase(t *testing.T) {
	c := &conf.Config{
		Database:     "sales",
		ExecuteQuery: "DELETE FROM sales.orders WHERE id > 0",
	}
	w := &Writer{ExecuteSQL: c.ExecuteQuery}

	ready := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	var readyOnce sync.Once
	var changed atomic.Bool
	go func() {
		defer close(done)
		for {
			configured := c.Database
			readyOnce.Do(func() { close(ready) })
			if configured != "sales" {
				changed.Store(true)
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	<-ready
	for range 10_000 {
		if err := w.resolveTarget(c); err != nil {
			close(stop)
			<-done
			t.Fatalf("resolveTarget() error = %v", err)
		}
	}
	close(stop)
	<-done

	if changed.Load() {
		t.Fatalf("configured database changed to %q", c.Database)
	}
}

func TestWriterPreCheckRejectsDatabaseMismatchBeforeConnection(t *testing.T) {
	c := &conf.Config{
		Database:     "sales",
		ExecuteQuery: "DELETE FROM archive.orders WHERE id > 0",
		Host:         "127.0.0.1",
		Port:         1,
	}
	w := &Writer{ExecuteSQL: c.ExecuteQuery}

	err := w.preCheck(c)
	if err == nil || !strings.Contains(err.Error(), `database mismatch: --database="sales", SQL schema="archive"`) {
		t.Fatalf("preCheck() error = %v, want database mismatch", err)
	}
}

func TestResolveTargetDatabase(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		sqlSchema  string
		want       string
		wantErr    string
	}{
		{name: "configured database", configured: "sales", want: "sales"},
		{name: "SQL schema", sqlSchema: "sales", want: "sales"},
		{name: "matching values", configured: "sales", sqlSchema: "sales", want: "sales"},
		{
			name:       "mismatched values",
			configured: "sales",
			sqlSchema:  "archive",
			wantErr:    `database mismatch: --database="sales", SQL schema="archive"`,
		},
		{
			name:    "missing values",
			wantErr: "no database specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTargetDatabase(tt.configured, tt.sqlSchema)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveTargetDatabase() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTargetDatabase() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveTargetDatabase() = %q, want %q", got, tt.want)
			}
		})
	}
}
