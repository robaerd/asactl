package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/logging"
)

func TestNewSuppressesDebugWithoutVerbose(t *testing.T) {
	var output bytes.Buffer
	logger := logging.New(&output, logging.Options{})
	logger.Debug("debug_message")
	logger.Info("info_message")

	text := output.String()
	if strings.Contains(text, "debug_message") {
		t.Fatalf("expected debug log to be suppressed, got %s", text)
	}
	if !strings.Contains(text, "info_message") {
		t.Fatalf("expected info log to be present, got %s", text)
	}
	if strings.Contains(text, "\x1b[") {
		t.Fatalf("expected no ANSI escapes for non-TTY writer, got %q", text)
	}
}

func TestNewEmitsDebugWithVerbose(t *testing.T) {
	var output bytes.Buffer
	logger := logging.New(&output, logging.Options{Verbose: true})
	logger.Debug("debug_message", "component", "test")

	text := output.String()
	if !strings.Contains(text, "debug_message") {
		t.Fatalf("expected debug log to be present, got %s", text)
	}
	if !strings.Contains(text, "component=test") {
		t.Fatalf("expected structured attrs in output, got %s", text)
	}
	if !strings.Contains(text, "DBG") {
		t.Fatalf("expected shortened debug level in output, got %s", text)
	}
}

func TestNewEmitsJSONWhenRequested(t *testing.T) {
	var output bytes.Buffer
	logger := logging.New(&output, logging.Options{JSON: true, Verbose: true})
	logger.Debug("debug_message", "component", "test", "count", 2)

	var payload map[string]any
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("decode json log: %v output=%s", err, output.String())
	}
	if payload[slog.MessageKey] != "debug_message" {
		t.Fatalf("expected debug message, got %v", payload[slog.MessageKey])
	}
	if payload["component"] != "test" {
		t.Fatalf("expected component attr, got %v", payload["component"])
	}
}

func TestEnsureReturnsUsableLoggerForNil(t *testing.T) {
	logger := logging.Ensure(nil)
	logger.Info("ignored")
}
