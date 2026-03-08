package clone_test

import (
	"path/filepath"
	"testing"

	clonepkg "github.com/robaerd/asactl/internal/clone"
	"github.com/robaerd/asactl/internal/spec"
)

func TestCloneScalesAndRenames(t *testing.T) {
	loaded, err := spec.LoadFile(filepath.Join("..", "..", "examples", "us.yaml"))
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	cloned, err := clonepkg.Spec(loaded, "GB", 0.8, 0.5)
	if err != nil {
		t.Fatalf("clone spec: %v", err)
	}
	if cloned.Campaigns[0].Name != "UK - Brand - Exact" {
		t.Fatalf("unexpected campaign name %q", cloned.Campaigns[0].Name)
	}
	if cloned.Campaigns[0].DailyBudget.String() != "0.75" {
		t.Fatalf("unexpected budget %s", cloned.Campaigns[0].DailyBudget.String())
	}
	if cloned.Campaigns[0].AdGroups[0].Keywords[0].Bid.String() != "0.88" {
		t.Fatalf("unexpected bid %s", cloned.Campaigns[0].AdGroups[0].Keywords[0].Bid.String())
	}
}

func TestCloneUsesStableDecimalMultipliers(t *testing.T) {
	loaded, err := spec.LoadFile(filepath.Join("..", "..", "examples", "us.yaml"))
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	cloned, err := clonepkg.Spec(loaded, "CA", 0.3333, 0.3333)
	if err != nil {
		t.Fatalf("clone spec: %v", err)
	}
	if cloned.Campaigns[0].DailyBudget.String() != "0.50" {
		t.Fatalf("unexpected budget %s", cloned.Campaigns[0].DailyBudget.String())
	}
	if cloned.Campaigns[0].AdGroups[0].Keywords[0].Bid.String() != "0.37" {
		t.Fatalf("unexpected bid %s", cloned.Campaigns[0].AdGroups[0].Keywords[0].Bid.String())
	}
}

func TestCloneRewritesGeneratorCampaignRefs(t *testing.T) {
	loaded, err := spec.LoadFile(filepath.Join("..", "..", "examples", "us.yaml"))
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	cloned, err := clonepkg.Spec(loaded, "CA", 1.0, 1.0)
	if err != nil {
		t.Fatalf("clone spec: %v", err)
	}
	if got := cloned.Generators[0].Spec.TargetRef.Campaign; got != "CA - Discovery" {
		t.Fatalf("unexpected generator target %q", got)
	}
	wantSources := []string{"CA - Brand - Exact", "CA - Category - Exact", "CA - Competitor - Exact"}
	if got := cloned.Generators[0].Spec.SourceRefs.Campaigns; len(got) != len(wantSources) || got[0] != wantSources[0] || got[1] != wantSources[1] || got[2] != wantSources[2] {
		t.Fatalf("unexpected generator sources %#v", got)
	}
}

func TestCloneClearsSourceMetadata(t *testing.T) {
	loaded, err := spec.LoadFile(filepath.Join("..", "..", "examples", "composed", "asactl.yaml"))
	if err != nil {
		t.Fatalf("load composed spec: %v", err)
	}
	if !loaded.Meta.Composed {
		t.Fatal("expected composed metadata on source spec")
	}

	cloned, err := clonepkg.Spec(loaded, "CA", 1.0, 1.0)
	if err != nil {
		t.Fatalf("clone spec: %v", err)
	}

	if cloned.Meta.Composed {
		t.Fatalf("expected cloned spec to clear composed metadata, got %#v", cloned.Meta)
	}
	if len(cloned.Meta.CampaignSources) != 0 {
		t.Fatalf("expected cloned spec to clear campaign sources, got %#v", cloned.Meta.CampaignSources)
	}
}

func TestCloneRejectsZeroMultipliers(t *testing.T) {
	loaded, err := spec.LoadFile(filepath.Join("..", "..", "examples", "us.yaml"))
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}

	testCases := []struct {
		name             string
		bidMultiplier    float64
		budgetMultiplier float64
		want             string
	}{
		{name: "zero bid multiplier", bidMultiplier: 0, budgetMultiplier: 1, want: "bid multiplier must be > 0"},
		{name: "zero budget multiplier", bidMultiplier: 1, budgetMultiplier: 0, want: "budget multiplier must be > 0"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := clonepkg.Spec(loaded, "CA", testCase.bidMultiplier, testCase.budgetMultiplier)
			if err == nil || err.Error() != testCase.want {
				t.Fatalf("expected %q, got %v", testCase.want, err)
			}
		})
	}
}
