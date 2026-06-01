package cmd

import (
	"sync"
	"testing"
)

var initRunOnce sync.Once

func ensureRunInit() {
	initRunOnce.Do(initRun)
}

// TestRunFlags_P2Mapping verifies the P2 CLI flags are registered on runCmd and
// parse into their bound package-level variables (which RunE maps into Config).
func TestRunFlags_P2Mapping(t *testing.T) {
	ensureRunInit()

	args := []string{
		"--execute", "DELETE FROM t WHERE id > 0",
		"--select-index", "idx_created_at",
		"--select-order-by", "created_at,id",
		"--select-cursor",
		"--max-rows", "500",
		"--max-duration-ms", "1000",
		"--dry-run",
		"--preflight-threshold", "200000",
		"--yes",
	}
	if err := runCmd.ParseFlags(args); err != nil {
		t.Fatalf("ParseFlags failed: %v", err)
	}

	if selectIndex != "idx_created_at" {
		t.Errorf("selectIndex = %q", selectIndex)
	}
	if selectOrderBy != "created_at,id" {
		t.Errorf("selectOrderBy = %q", selectOrderBy)
	}
	if !selectCursor {
		t.Errorf("selectCursor = false, want true")
	}
	if maxRows != 500 {
		t.Errorf("maxRows = %d", maxRows)
	}
	if maxDurationMs != 1000 {
		t.Errorf("maxDurationMs = %d", maxDurationMs)
	}
	if !dryRun {
		t.Errorf("dryRun = false, want true")
	}
	if preflightThreshold != 200000 {
		t.Errorf("preflightThreshold = %d", preflightThreshold)
	}
	if !autoConfirm {
		t.Errorf("autoConfirm = false, want true")
	}
}

// Guard that every P2 flag exists on the run command.
func TestRunFlags_P2Registered(t *testing.T) {
	ensureRunInit()
	for _, name := range []string{
		"select-index", "select-order-by", "select-cursor",
		"max-rows", "max-duration-ms", "dry-run", "preflight-threshold", "yes",
	} {
		if runCmd.Flags().Lookup(name) == nil {
			t.Errorf("flag --%s not registered", name)
		}
	}
}
