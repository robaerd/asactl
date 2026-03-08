package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/lmittmann/tint"
)

type Options struct {
	JSON    bool
	Verbose bool
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func New(writer io.Writer, options Options) *slog.Logger {
	level := slog.LevelInfo
	if options.Verbose {
		level = slog.LevelDebug
	}

	handlerOptions := &slog.HandlerOptions{
		Level:     level,
		AddSource: options.Verbose,
	}
	if options.JSON {
		return slog.New(slog.NewJSONHandler(writer, handlerOptions))
	}

	return slog.New(tint.NewHandler(writer, &tint.Options{
		Level:      level,
		AddSource:  options.Verbose,
		NoColor:    !ColorEnabled(writer),
		TimeFormat: "15:04:05",
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return attr
			}

			if attr.Key == slog.LevelKey {
				if levelValue, ok := attr.Value.Any().(slog.Level); ok {
					return slog.String(slog.LevelKey, shortLevel(levelValue))
				}
			}

			if attr.Key == slog.SourceKey {
				if source, ok := attr.Value.Any().(*slog.Source); ok && source != nil {
					return slog.String(slog.SourceKey, filepath.Base(source.File)+":"+strconv.Itoa(source.Line))
				}
			}

			if attr.Value.Kind() == slog.KindAny {
				if err, ok := attr.Value.Any().(error); ok {
					return tint.Err(err)
				}
			}

			return attr
		},
	}))
}

type statWriter interface {
	io.Writer
	Stat() (os.FileInfo, error)
}

func ColorEnabled(writer io.Writer) bool {
	return colorEnabledWithLookup(writer, os.LookupEnv)
}

func colorEnabledWithLookup(writer io.Writer, lookupEnv func(string) (string, bool)) bool {
	if _, disabled := lookupEnv("NO_COLOR"); disabled {
		return false
	}

	statter, ok := writer.(statWriter)
	if !ok {
		return false
	}

	info, err := statter.Stat()
	if err != nil {
		return false
	}

	return info.Mode()&os.ModeCharDevice != 0
}

func shortLevel(level slog.Level) string {
	switch {
	case level <= slog.LevelDebug:
		return "DBG"
	case level < slog.LevelWarn:
		return "INF"
	case level < slog.LevelError:
		return "WRN"
	default:
		return "ERR"
	}
}

func Ensure(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return discardLogger
	}
	return logger
}

func Component(logger *slog.Logger, name string) *slog.Logger {
	return Ensure(logger).With("component", name)
}
