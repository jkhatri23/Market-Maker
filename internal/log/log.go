package log

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(level, format string) (*zap.Logger, error) {
	lvl := zap.NewAtomicLevel()
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("parse log level %q: %w", level, err)
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var enc zapcore.Encoder
	switch format {
	case "", "json":
		enc = zapcore.NewJSONEncoder(encCfg)
	case "console":
		enc = zapcore.NewConsoleEncoder(encCfg)
	default:
		return nil, fmt.Errorf("unknown log format %q (want json|console)", format)
	}

	core := zapcore.NewCore(enc, zapcore.Lock(os.Stderr), lvl)
	return zap.New(core, zap.AddCaller()), nil
}
