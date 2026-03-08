package cmd

import (
	"os"
	"regexp"
	"testing"
)

func TestRunCommand_StoppedErrorHandledAsSuccess(t *testing.T) {
	src, err := os.ReadFile("run.go")
	if err != nil {
		t.Fatalf("read run.go failed: %v", err)
	}

	// Guard CLI contract: ErrExecutionStopped should be treated as graceful stop.
	re := regexp.MustCompile(`(?s)errors\.Is\s*\(\s*err\s*,\s*task\.ErrExecutionStopped\s*\)\s*\{\s*log\.Logger\.Info\("task stopped by signal"\)\s*return nil\s*\}`)
	if !re.Match(src) {
		t.Fatalf("run command must treat task.ErrExecutionStopped as success (log + return nil)")
	}
}
