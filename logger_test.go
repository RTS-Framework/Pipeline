package pipeline

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogger(t *testing.T) {
	t.Run("common", func(t *testing.T) {
		lg := NewLogger(nil)

		testPrintLog(lg)
	})

	t.Run("debug", func(t *testing.T) {
		lg := NewLogger(nil)
		lg.SetLevel(LevelDebug)

		testPrintLog(lg)
	})

	t.Run("info", func(t *testing.T) {
		lg := NewLogger(nil)
		lg.SetLevel(LevelInfo)

		testPrintLog(lg)
	})

	t.Run("warning", func(t *testing.T) {
		lg := NewLogger(nil)
		lg.SetLevel(LevelWarning)

		testPrintLog(lg)
	})

	t.Run("error", func(t *testing.T) {
		lg := NewLogger(nil)
		lg.SetLevel(LevelError)

		testPrintLog(lg)
	})

	t.Run("fatal", func(t *testing.T) {
		lg := NewLogger(nil)
		lg.SetLevel(LevelFatal)

		testPrintLog(lg)
	})

	t.Run("discard", func(t *testing.T) {
		lg := NewLogger(nil)
		lg.SetLevel(LevelDiscard)

		testPrintLog(lg)
	})

	t.Run("multi writer", func(t *testing.T) {
		buf := bytes.NewBuffer(make([]byte, 0, 512))

		lg := NewLogger(buf)
		lg.SetLevel(LevelDebug)

		testPrintLog(lg)

		require.NotEmpty(t, buf.String())
	})
}

func testPrintLog(logger Logger) {
	logger.Debug("debug log")
	logger.Debugf("%s", "debugf log")

	logger.Info("info log")
	logger.Infof("%s", "infof log")

	logger.Warning("warning log")
	logger.Warningf("%s", "warningf log")

	logger.Error("error log")
	logger.Errorf("%s", "errorf log")

	logger.Fatal("test func", "fatal log")
	logger.Fatalf("test func", "%s", "fatalf log")
}
