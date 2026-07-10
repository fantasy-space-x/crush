package log

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsoleHandlerUsesCompactFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(newConsoleHandler(&buf, slog.LevelInfo))
	logger.Info("Starting Crush server", "host", "tcp://127.0.0.1:8080")

	out := buf.String()
	require.Regexp(t, `time=\d{2}:\d{2}:\d{2}\b`, out)
	require.Contains(t, out, "level=INFO")
	require.Contains(t, out, `msg="Starting Crush server"`)
	require.Contains(t, out, "host=tcp://127.0.0.1:8080")
	require.NotContains(t, out, "source=")
	require.NotRegexp(t, `\d{4}-\d{2}-\d{2}`, out)
	require.NotRegexp(t, `^\{`, out)
}
