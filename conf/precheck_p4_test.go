package conf

import (
	"strings"
	"testing"
)

func TestPreCheck_TiDBRowIDDeleteOK(t *testing.T) {
	c := baseConfig()
	c.TiDBRowID = true
	if err := c.PreCheck(); err != nil {
		t.Fatalf("expected tidb-rowid DELETE to pass, got: %v", err)
	}
}

func TestPreCheck_TiDBRowIDRequiresDelete(t *testing.T) {
	c := baseConfig()
	c.ExecuteQuery = "UPDATE t SET c=1 WHERE id>0"
	c.TiDBRowID = true
	err := c.PreCheck()
	if err == nil || !strings.Contains(err.Error(), "requires DELETE") {
		t.Fatalf("expected DELETE-only error, got: %v", err)
	}
}

func TestPreCheck_TiDBRowIDMutualExclusions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"with order-by", func(c *Config) { c.SelectOrderBy = "created_at" }, "covering fast-path"},
		{"with select-cursor", func(c *Config) { c.SelectCursor = true }, "select-cursor"},
		{"with select-index", func(c *Config) { c.SelectIndex = "idx" }, "select-index"},
		{"with partition-concurrency", func(c *Config) { c.PartitionConcurrency = 4 }, "partition-concurrency"},
		{"with force-chunking-column", func(c *Config) { c.ForceChunkingColumn = "id" }, "force-chunking-column"},
		{"chunk-size zero", func(c *Config) { c.ChunkSize = 0 }, "chunk-size > 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := baseConfig()
			c.TiDBRowID = true
			tt.mutate(c)
			err := c.PreCheck()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got: %v", tt.want, err)
			}
		})
	}
}
