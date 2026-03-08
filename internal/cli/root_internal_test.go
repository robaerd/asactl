package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/diff"
	syncpkg "github.com/robaerd/asactl/internal/sync"
)

func TestConfirmationPromptForManagedRecreateMentionsFullManagedRebuild(t *testing.T) {
	result := syncpkg.Result{
		Plan: diff.Plan{
			Summary: diff.Summary{Delete: 1, Create: 1, Total: 2},
		},
		ScopeSummary: appleadsapi.ScopeSummary{
			ManagedCampaignCount:  1,
			OtherAppCampaignCount: 0,
		},
	}

	prompt := confirmationPrompt(result, diff.RecreateScopeManaged)
	if !strings.Contains(prompt, "before recreating them from YAML") {
		t.Fatalf("expected recreate wording, got %q", prompt)
	}
	if !strings.Contains(prompt, "campaign_group.id + app.app_id scope") {
		t.Fatalf("expected campaign_group scope wording, got %q", prompt)
	}
}

func TestStdioPrompterAcceptsNonClosableReader(t *testing.T) {
	output := &bytes.Buffer{}
	confirmed, err := stdioPrompter{}.Confirm(context.Background(), strings.NewReader("yes\n"), output, "Apply changes?")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if !confirmed {
		t.Fatal("expected confirmation to succeed")
	}
	if !strings.Contains(output.String(), "Apply changes? [y/N]: ") {
		t.Fatalf("unexpected prompt output: %q", output.String())
	}
}

func TestStdioPrompterReturnsOnContextCancellation(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()

	ctx, cancel := context.WithCancelCause(context.Background())
	output := &promptSignalWriter{
		buffer: &bytes.Buffer{},
		signal: "Apply changes? [y/N]: ",
		ready:  make(chan struct{}, 1),
	}

	type confirmResult struct {
		confirmed bool
		err       error
	}
	resultCh := make(chan confirmResult, 1)
	go func() {
		confirmed, err := stdioPrompter{}.Confirm(ctx, reader, output, "Apply changes?")
		resultCh <- confirmResult{confirmed: confirmed, err: err}
	}()

	select {
	case <-output.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("expected confirmation prompt before cancellation")
	}
	cancel(context.Canceled)

	select {
	case result := <-resultCh:
		if result.confirmed {
			t.Fatal("expected confirmation to be false on cancellation")
		}
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("confirm did not return after context cancellation")
	}
}

type promptSignalWriter struct {
	buffer *bytes.Buffer
	signal string
	ready  chan struct{}
}

func (writer *promptSignalWriter) Write(data []byte) (int, error) {
	n, err := writer.buffer.Write(data)
	if strings.Contains(writer.buffer.String(), writer.signal) {
		select {
		case writer.ready <- struct{}{}:
		default:
		}
	}
	return n, err
}
