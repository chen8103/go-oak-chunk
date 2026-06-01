package conf

import (
	"strings"
	"testing"
)

func baseConfig() *Config {
	return &Config{
		ChunkSize:    1000,
		ExecuteQuery: "DELETE FROM t WHERE created_at < '2025-01-01'",
	}
}

func TestParseSQLType(t *testing.T) {
	tests := []struct {
		query   string
		want    string
		wantErr bool
	}{
		{"DELETE FROM t WHERE id > 0", SQLTypeDelete, false},
		{"  delete from t where id>0", SQLTypeDelete, false},
		{"UPDATE t SET c=1 WHERE id>0", SQLTypeUpdate, false},
		{"  update t set c=1", SQLTypeUpdate, false},
		{"SELECT * FROM t", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		got, err := ParseSQLType(tt.query)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSQLType(%q) expected error", tt.query)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseSQLType(%q) unexpected error: %v", tt.query, err)
		}
		if got != tt.want {
			t.Errorf("ParseSQLType(%q) = %s, want %s", tt.query, got, tt.want)
		}
	}
}

func TestPreCheck_FastPathRequiresDelete(t *testing.T) {
	c := baseConfig()
	c.ExecuteQuery = "UPDATE t SET c=1 WHERE id>0"
	c.SelectOrderBy = "created_at"
	err := c.PreCheck()
	if err == nil || !strings.Contains(err.Error(), "requires DELETE") {
		t.Fatalf("expected DELETE-only error, got: %v", err)
	}
}

func TestPreCheck_FastPathDeleteOK(t *testing.T) {
	c := baseConfig()
	c.SelectOrderBy = "created_at"
	c.SelectIndex = "idx_created_at"
	c.SelectCursor = true
	if err := c.PreCheck(); err != nil {
		t.Fatalf("expected fast-path DELETE to pass, got: %v", err)
	}
}

func TestPreCheck_SelectCursorRequiresOrderBy(t *testing.T) {
	c := baseConfig()
	c.SelectCursor = true
	err := c.PreCheck()
	if err == nil || !strings.Contains(err.Error(), "--select-cursor requires --select-order-by") {
		t.Fatalf("expected select-cursor dependency error, got: %v", err)
	}
}

func TestPreCheck_SelectIndexRequiresOrderBy(t *testing.T) {
	c := baseConfig()
	c.SelectIndex = "idx_x"
	err := c.PreCheck()
	if err == nil || !strings.Contains(err.Error(), "--select-index requires --select-order-by") {
		t.Fatalf("expected select-index dependency error, got: %v", err)
	}
}

func TestPreCheck_GuardrailNegatives(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(c *Config)
		want string
	}{
		{"max-rows", func(c *Config) { c.MaxRows = -1 }, "--max-rows"},
		{"max-duration", func(c *Config) { c.MaxDuration = -1 }, "--max-duration-ms"},
		{"preflight-threshold", func(c *Config) { c.PreflightThreshold = -1 }, "--preflight-threshold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.set(c)
			err := c.PreCheck()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %s error, got: %v", tc.want, err)
			}
		})
	}
}

// A plain DELETE with no fast-path flags must still pass unchanged.
func TestPreCheck_NoFastPathStillPasses(t *testing.T) {
	if err := baseConfig().PreCheck(); err != nil {
		t.Fatalf("baseline DELETE PreCheck failed: %v", err)
	}
}

// max-rows / max-duration are only enforced on the fast-path in P2; setting them
// on the default range path must be rejected, not silently ignored.
func TestPreCheck_GuardrailsWithoutFastPathRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(c *Config)
	}{
		{"max-rows", func(c *Config) { c.MaxRows = 500 }},
		{"max-duration", func(c *Config) { c.MaxDuration = 1000 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.set(c)
			err := c.PreCheck()
			if err == nil || !strings.Contains(err.Error(), "--select-order-by fast-path") {
				t.Fatalf("expected fast-path-required error, got: %v", err)
			}
		})
	}
}

// The same guardrails are accepted once the fast-path is enabled.
func TestPreCheck_GuardrailsWithFastPathAccepted(t *testing.T) {
	c := baseConfig()
	c.SelectOrderBy = "created_at"
	c.MaxRows = 500
	c.MaxDuration = 1000
	if err := c.PreCheck(); err != nil {
		t.Fatalf("fast-path guardrails PreCheck failed: %v", err)
	}
}
