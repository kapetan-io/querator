package querator_test

import (
	"flag"
	"github.com/kapetan-io/tackle/color"
	"io"
	"log/slog"
	"testing"
)

var log *slog.Logger

func TestMain(m *testing.M) {
	logFlag := flag.String("logging", "", "indicates the type of logging during tests. "+
		"If unset tests run with debug level colored text log output. "+
		"If set to 'ci' discards logs during tests which greatly reduces logs during CI runs")
	flag.Parse()

	switch *logFlag {
	case "":
		log = slog.New(color.NewLog(&color.LogOptions{
			HandlerOptions: slog.HandlerOptions{
				ReplaceAttr: color.SuppressAttrs(slog.TimeKey),
				Level:       slog.LevelDebug,
			},
		}))
	case "ci":
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	m.Run()
}
