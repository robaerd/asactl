package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunPrintsUnknownCommandError(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := run(context.Background(), stdout, stderr, []string{"unknown-command"})
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), `unknown command "unknown-command" for "asactl"`) {
		t.Fatalf("expected unknown command on stderr, got %q", stderr.String())
	}
}

func TestRunDoesNotDoublePrintRenderedErrors(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := run(context.Background(), stdout, stderr, []string{"plan"})
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
	if count := strings.Count(stderr.String(), "missing required argument: <config.yaml>"); count != 1 {
		t.Fatalf("expected missing-argument error once, got %d in %q", count, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Run 'asactl plan --help' for usage.") {
		t.Fatalf("expected help hint in stderr, got %q", stderr.String())
	}
}
