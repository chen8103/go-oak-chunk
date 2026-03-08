package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type OutputMode string

const (
	OutputStdio  OutputMode = "stdio"
	OutputStderr OutputMode = "stderr"
	OutputFile   OutputMode = "file"
)

var Logger = NewZapLogger(zap.NewNop().Sugar())
var loggerConfigured atomic.Bool

// 保留兼容别名，避免一次性改动全部调用点。

func New(isDebug bool, mode OutputMode, filePath ...string) error {
	logLevel := zapcore.InfoLevel
	if isDebug {
		logLevel = zapcore.DebugLevel
	}

	writeSyncer, err := buildWriteSyncer(mode, filePath...)
	if err != nil {
		return err
	}

	encoder := zapcore.NewConsoleEncoder(buildEncoderConfig())
	core := zapcore.NewCore(encoder, writeSyncer, logLevel)
	sugar := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
	Logger.setSugaredLogger(sugar)
	loggerConfigured.Store(true)
	return nil
}

func NewFromSugaredLogger(sugar *zap.SugaredLogger) {
	if sugar == nil {
		return
	}
	Logger.setSugaredLogger(sugar)
	loggerConfigured.Store(true)
}

func IsConfigured() bool {
	return loggerConfigured.Load()
}

type ZapLogger struct {
	mu     sync.RWMutex
	logger *zap.SugaredLogger
}

func NewZapLogger(logger *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{logger: logger}
}

func (l *ZapLogger) setSugaredLogger(logger *zap.SugaredLogger) {
	l.mu.Lock()
	l.logger = logger
	l.mu.Unlock()
}

func (l *ZapLogger) getLogger() *zap.SugaredLogger {
	l.mu.RLock()
	logger := l.logger
	l.mu.RUnlock()
	if logger == nil {
		return zap.NewNop().Sugar()
	}
	return logger
}

func (l *ZapLogger) Debug(format string, args ...any) {
	logger := l.getLogger()
	if len(args) == 0 {
		logger.Debug(format)
		return
	}
	logger.Debugf(format, args...)
}

func (l *ZapLogger) Info(format string, args ...any) {
	logger := l.getLogger()
	if len(args) == 0 {
		logger.Info(format)
		return
	}
	logger.Infof(format, args...)
}

func (l *ZapLogger) Warn(format string, args ...any) {
	logger := l.getLogger()
	if len(args) == 0 {
		logger.Warn(format)
		return
	}
	logger.Warnf(format, args...)
}

func (l *ZapLogger) Error(format string, args ...any) {
	logger := l.getLogger()
	if len(args) == 0 {
		logger.Error(format)
		return
	}
	logger.Errorf(format, args...)
}

// Fatal 为 SDK 兼容保留，不再触发 os.Exit。
func (l *ZapLogger) Fatal(format string, args ...any) {
	l.Error(format, args...)
}

func (l *ZapLogger) Debugf(format string, args ...any) {
	l.getLogger().Debugf(format, args...)
}

func (l *ZapLogger) Infof(format string, args ...any) {
	l.getLogger().Infof(format, args...)
}

func (l *ZapLogger) Warnf(format string, args ...any) {
	l.getLogger().Warnf(format, args...)
}

func (l *ZapLogger) Errorf(format string, args ...any) {
	l.getLogger().Errorf(format, args...)
}

func (l *ZapLogger) Sync() {
	_ = l.getLogger().Sync()
}

func buildWriteSyncer(mode OutputMode, filePath ...string) (zapcore.WriteSyncer, error) {
	switch mode {
	case OutputStdio:
		return zapcore.AddSync(os.Stdout), nil
	case OutputStderr:
		return zapcore.AddSync(os.Stderr), nil
	case OutputFile:
		logPath := defaultLogPath(filePath...)
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			return nil, fmt.Errorf("create log directory: %w", err)
		}

		rotating := &lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     28,
			Compress:   true,
		}
		return zapcore.AddSync(rotating), nil
	default:
		return nil, fmt.Errorf("unsupported log output mode: %s", mode)
	}
}

func defaultLogPath(filePath ...string) string {
	if len(filePath) == 0 {
		return "./logs/app.log"
	}

	candidate := strings.TrimSpace(filePath[0])
	if candidate == "" {
		return "./logs/app.log"
	}

	return candidate
}

func buildEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    customLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout(time.DateTime),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   customCallerEncoder,
	}
}

func customLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

func customCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	if caller.Defined {
		enc.AppendString("[" + caller.TrimmedPath() + "]")
		return
	}
	enc.AppendString("[undefined]")
}
