package mysql

import (
	"strings"
	"testing"
	"time"

	soar "github.com/XiaoMi/soar/ast"
	"github.com/pingcap/parser/ast"
)

func TestFreezeNow_BasicFunctions(t *testing.T) {
	const frozen = "2026-05-13 14:23:10"
	tests := []struct {
		name    string
		sql     string
		want    []string
		notWant string
	}{
		{
			name:    "now()",
			sql:     "DELETE FROM orders WHERE ts < now()",
			want:    []string{frozen},
			notWant: "now(",
		},
		{
			name:    "current_timestamp()",
			sql:     "UPDATE orders SET status=1 WHERE updated_at >= current_timestamp()",
			want:    []string{frozen},
			notWant: "current_timestamp",
		},
		{
			name:    "sysdate()",
			sql:     "DELETE FROM logs WHERE create_time <= sysdate()",
			want:    []string{frozen},
			notWant: "sysdate",
		},
		{
			name:    "localtime()",
			sql:     "DELETE FROM events WHERE event_ts > localtime()",
			want:    []string{frozen},
			notWant: "localtime(",
		},
		{
			name:    "localtimestamp()",
			sql:     "DELETE FROM data WHERE ts <= localtimestamp()",
			want:    []string{frozen},
			notWant: "localtimestamp",
		},
		{
			name:    "mixed_with_AND",
			sql:     "DELETE FROM t WHERE a = 1 AND ts < now() AND b = 2",
			want:    []string{"=1", frozen, "=2"},
			notWant: "now(",
		},
		{
			name:    "nested_in_function",
			sql:     "DELETE FROM t WHERE date_sub(now(), interval 10 day) > ts",
			want:    []string{"DATE_SUB", frozen},
			notWant: "now(",
		},
		{
			name:    "case_insensitive_NOW",
			sql:     "DELETE FROM t WHERE ts < NOW()",
			want:    []string{frozen},
			notWant: "now(",
		},
		{
			name:    "multiple_now_calls",
			sql:     "DELETE FROM t WHERE ts1 > now() AND ts2 < now()",
			want:    []string{frozen},
			notWant: "now(",
		},
		{
			name:    "where_without_now",
			sql:     "DELETE FROM t WHERE id > 0",
			want:    []string{">0"},
			notWant: "2026-05-13",
		},
	}

	freezeTime := time.Date(2026, 5, 13, 14, 23, 10, 0, time.Local)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := soar.TiParse(tt.sql, "", "")
			if err != nil {
				t.Fatalf("TiParse failed: %v", err)
			}
			if len(stmts) == 0 {
				t.Fatalf("empty parse result")
			}

			var whereNode ast.ExprNode
			switch stmt := stmts[0].(type) {
			case *ast.DeleteStmt:
				whereNode = stmt.Where
			case *ast.UpdateStmt:
				whereNode = stmt.Where
			default:
				t.Fatalf("unsupported statement type")
			}

			result, err := FreezeNow(whereNode, freezeTime)
			if err != nil {
				t.Fatalf("FreezeNow failed: %v", err)
			}

			compact := strings.ReplaceAll(strings.ToLower(result), " ", "")
			for _, s := range tt.want {
				if !strings.Contains(strings.ToLower(result), strings.ToLower(s)) &&
					!strings.Contains(compact, strings.ToLower(s)) {
					t.Errorf("result missing %q, got: %q", s, result)
				}
			}

			if tt.notWant != "" && strings.Contains(strings.ToLower(result), strings.ToLower(tt.notWant)) {
				t.Errorf("result should not contain %q, got: %q", tt.notWant, result)
			}
		})
	}
}

func TestFreezeNow_NilWhere(t *testing.T) {
	result, err := FreezeNow(nil, time.Now())
	if err != nil {
		t.Fatalf("FreezeNow(nil) failed: %v", err)
	}
	if result != "" {
		t.Errorf("FreezeNow(nil) = %q, want empty", result)
	}
}
