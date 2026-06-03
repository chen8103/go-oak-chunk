package task

import (
	"testing"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
)

func TestSelectStrategy(t *testing.T) {
	w := &mysql.Writer{}

	tests := []struct {
		name     string
		config   *conf.Config
		wantName string
	}{
		{
			name:     "default range strategy",
			config:   &conf.Config{},
			wantName: "RangeStrategy",
		},
		{
			name:     "fast-path covering strategy when order-by set",
			config:   &conf.Config{SelectOrderBy: "created_at"},
			wantName: "OBCoveringStrategy",
		},
		{
			name:     "blank order-by stays range",
			config:   &conf.Config{SelectOrderBy: "  "},
			wantName: "RangeStrategy",
		},
		{
			// PartitionConcurrency>1 + fast-path but non-OceanBase data source
			// (empty DataSource() on the bare Writer) must fall back to covering.
			name:     "partition concurrency without oceanbase falls back to covering",
			config:   &conf.Config{SelectOrderBy: "created_at", PartitionConcurrency: 4},
			wantName: "OBCoveringStrategy",
		},
		{
			name:     "partition concurrency <=1 stays covering",
			config:   &conf.Config{SelectOrderBy: "created_at", PartitionConcurrency: 1},
			wantName: "OBCoveringStrategy",
		},
		{
			name:     "tidb-rowid takes top priority",
			config:   &conf.Config{TiDBRowID: true},
			wantName: "TiDBRowIDStrategy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectStrategy(tt.config, w).Name()
			if got != tt.wantName {
				t.Fatalf("selectStrategy = %s, want %s", got, tt.wantName)
			}
		})
	}
}
