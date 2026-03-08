package diff

import (
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
)

func TestBuildPlanMarksRemoteOnlyActionsExplicitly(t *testing.T) {
	desired := State{
		Campaigns: []Campaign{{
			Name:        "Managed Campaign",
			DailyBudget: mustInternalDecimal(t, "1.00"),
			Status:      spec.StatusActive,
			context: actionContext{
				sourcePath:    "campaigns/managed.yaml",
				sourceOrder:   0,
				campaignOrder: 0,
			},
		}},
	}
	remote := State{
		Campaigns: []Campaign{{
			ID:          "c1",
			Name:        "Legacy Campaign",
			DailyBudget: mustInternalDecimal(t, "1.00"),
			Status:      spec.StatusActive,
			context: actionContext{
				sourcePath:    "campaigns/us.yaml",
				sourceOrder:   0,
				campaignOrder: 3,
			},
		}},
	}

	plan := BuildPlan(desired, remote)
	if len(plan.Actions) != 2 {
		t.Fatalf("expected two actions, got %d", len(plan.Actions))
	}
	action := plan.Actions[0]
	if action.SourcePath != "" {
		t.Fatalf("expected remote-only action to have no source path, got %q", action.SourcePath)
	}
	if action.context.sourceOrder != -1 {
		t.Fatalf("expected remote-only source order -1, got %d", action.context.sourceOrder)
	}
	if action.context.campaignOrder != -1 {
		t.Fatalf("expected remote-only campaign order -1, got %d", action.context.campaignOrder)
	}
	if !action.context.isRemoteState() {
		t.Fatalf("expected explicit remote marker, got %+v", action.context)
	}

	rendered := RenderText(plan)
	if !strings.Contains(rendered, "Remote-only") {
		t.Fatalf("expected remote-only section, got:\n%s", rendered)
	}
}

func TestRemoteContextFromValueRequiresExplicitMarker(t *testing.T) {
	campaign := Campaign{
		Name:        "Legacy Campaign",
		DailyBudget: mustInternalDecimal(t, "1.00"),
		Status:      spec.StatusActive,
		context: actionContext{
			sourcePath:    "campaigns/us.yaml",
			sourceOrder:   0,
			campaignOrder: 3,
		},
	}

	if _, ok := remoteContextFromValue(campaign); ok {
		t.Fatal("expected unmarked campaign context to stay local")
	}

	marked := campaign
	marked.context = marked.context.markedRemoteState()

	context, ok := remoteContextFromValue(marked)
	if !ok {
		t.Fatal("expected marked campaign context to resolve as remote")
	}
	if !context.isRemoteState() {
		t.Fatalf("expected explicit remote marker, got %+v", context)
	}
	if context.sourcePath != "" {
		t.Fatalf("expected remote marker to clear source path, got %q", context.sourcePath)
	}
	if context.sourceOrder != -1 {
		t.Fatalf("expected remote marker to clear source order, got %d", context.sourceOrder)
	}
	if context.campaignOrder != -1 {
		t.Fatalf("expected remote marker to clear campaign order, got %d", context.campaignOrder)
	}
}

func mustInternalDecimal(t *testing.T, value string) spec.Decimal {
	t.Helper()
	decimal, err := spec.ParseDecimal(value)
	if err != nil {
		t.Fatalf("parse decimal %q: %v", value, err)
	}
	return decimal
}
