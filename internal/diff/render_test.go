package diff_test

import (
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/diff"
)

func TestRenderTextIncludesSummaryAndChanges(t *testing.T) {
	plan := diff.Plan{
		Actions: []diff.Action{{
			Operation:    diff.OperationUpdate,
			Kind:         diff.ResourceCampaign,
			Description:  `"US - Brand - Exact"`,
			CampaignName: "US - Brand - Exact",
			Changes: []diff.FieldChange{{
				Field:  "daily_budget",
				Before: mustDecimal(t, "1.00"),
				After:  mustDecimal(t, "1.50"),
			}},
		}},
		Summary: diff.Summary{Update: 1, Total: 1},
	}

	rendered := diff.RenderText(plan)
	if !strings.Contains(rendered, "Summary: delete=0 create=0 update=1 pause=0 activate=0 noop=0 total=1") {
		t.Fatalf("expected summary line, got:\n%s", rendered)
	}
	if firstLine := strings.SplitN(rendered, "\n", 2)[0]; firstLine != "Summary: delete=0 create=0 update=1 pause=0 activate=0 noop=0 total=1" {
		t.Fatalf("expected summary on first line, got %q in:\n%s", firstLine, rendered)
	}
	if !strings.Contains(rendered, `Campaign: US - Brand - Exact`) {
		t.Fatalf("expected campaign section, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, `UPDATE campaign "US - Brand - Exact"`) {
		t.Fatalf("expected action line, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "daily_budget=1.00->1.50") {
		t.Fatalf("expected change details, got:\n%s", rendered)
	}
}

func TestRenderTextWithNoActionsStillRendersSummary(t *testing.T) {
	rendered := diff.RenderText(diff.Plan{})
	if strings.TrimSpace(rendered) != "Summary: delete=0 create=0 update=0 pause=0 activate=0 noop=0 total=0" {
		t.Fatalf("unexpected render output: %q", rendered)
	}
}

func TestRenderStyledTextAddsANSIColors(t *testing.T) {
	plan := diff.Plan{
		Actions: []diff.Action{{
			Operation:    diff.OperationCreate,
			Kind:         diff.ResourceCampaign,
			Description:  `"US - Brand - Exact"`,
			CampaignName: "US - Brand - Exact",
		}},
		Summary: diff.Summary{Create: 1, Total: 1},
	}

	rendered := diff.RenderStyledText(plan, diff.RenderOptions{Color: true})
	if !strings.Contains(rendered, "\x1b[32mCREATE\x1b[0m") {
		t.Fatalf("expected CREATE operation to be colorized, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "create=\x1b[32m1\x1b[0m") {
		t.Fatalf("expected create count to be colorized, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "total=\x1b[1m1\x1b[0m") {
		t.Fatalf("expected total count to be colorized, got:\n%s", rendered)
	}
}
