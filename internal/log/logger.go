package log

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
)

type Log struct {
	Logger logr.Logger
}

func NewLogger() (l Log, err error) {
	var cfg zap.Config

	devMode := os.Getenv("DEV_MODE")
	if strings.EqualFold(devMode, "true") {
		cfg = zap.NewDevelopmentConfig()
		cfg.Development = true
		cfg.Encoding = "console"
		cfg.EncoderConfig.StacktraceKey = "trace"
		cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	} else {
		cfg = zap.NewProductionConfig()
		cfg.Development = false
		cfg.Encoding = "json"
		cfg.EncoderConfig.StacktraceKey = "trace"
		cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	}

	logLvl := os.Getenv("LOG_LEVEL")
	if strings.EqualFold(logLvl, "DEBUG") {
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else if strings.EqualFold(logLvl, "INFO") {
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	} else {
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	}

	zapLogger, err := cfg.Build()
	if err != nil {
		return
	}
	l.Logger = zapr.NewLogger(zapLogger)
	return
}

func (l Log) Debug(message string, keysAndValues ...interface{}) {
	l.Logger.V(1).Info(message, keysAndValues...)
}

func (l Log) Info(message string, keysAndValues ...interface{}) {
	l.Logger.Info(message, keysAndValues...)
}

func (l Log) Warn(message string, keysAndValues ...interface{}) {
	l.Logger.V(-1).Info(message, keysAndValues...)
}

func (l Log) Error(err error, message string, keysAndValues ...interface{}) {
	l.Logger.Error(err, message, keysAndValues...)
}
