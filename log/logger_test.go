package log

import (
	"testing"
)

func Test_log(t *testing.T) {
	Logger.Error("this is err msg")
	Logger.Debug("this is debg msg")
	Logger.Info("this is a %s msg", "INFO")
}
