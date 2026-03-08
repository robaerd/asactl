package diff_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
)

func TestBuildPlanKeywordMatchTypeReplacement(t *testing.T) {
	desired := diff.State{Keywords: []diff.Keyword{{CampaignName: "US - Discovery", AdGroupName: "Discovery - Broad", Text: "reading tracker", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "0.60"), Status: spec.StatusActive}}}
	remote := diff.State{Keywords: []diff.Keyword{{ID: "k1", CampaignName: "US - Discovery", AdGroupName: "Discovery - Broad", Text: "reading tracker", MatchType: spec.MatchTypeBroad, Bid: mustDecimal(t, "0.60"), Status: spec.StatusActive}}}
	plan := diff.BuildPlan(desired, remote)
	text := diff.RenderText(plan)
	if !strings.Contains(text, "CREATE keyword") || !strings.Contains(text, "DELETE keyword") {
		t.Fatalf("expected create and delete for match-type replacement, got:\n%s", text)
	}
}

func TestBuildPlanCPPSwitchDeletesOldCustomAd(t *testing.T) {
	desired := diff.State{CustomAds: []diff.CustomAd{{CampaignName: "US - Category - Exact", AdGroupName: "Tracker Core", ProductPage: "CPP1", Status: spec.StatusActive}}}
	remote := diff.State{CustomAds: []diff.CustomAd{{ID: "ad-old", CampaignName: "US - Category - Exact", AdGroupName: "Tracker Core", ProductPage: "CPP2", Status: spec.StatusActive}}}
	plan := diff.BuildPlan(desired, remote)
	text := diff.RenderText(plan)
	if !strings.Contains(text, "CREATE custom ad") {
		t.Fatalf("expected create action, got:\n%s", text)
	}
	if !strings.Contains(text, "DELETE custom ad") {
		t.Fatalf("expected delete action, got:\n%s", text)
	}
}

func TestBuildPlanMissingDesiredCustomAdProducesSingleDelete(t *testing.T) {
	remote := diff.State{
		CustomAds: []diff.CustomAd{{
			ID:           "ad-old",
			CampaignName: "US - Category - Exact",
			AdGroupName:  "Tracker Core - CPP1",
			ProductPage:  "CPP1",
			Status:       spec.StatusActive,
		}},
	}

	plan := diff.BuildPlan(diff.State{}, remote)
	if plan.Summary.Delete != 1 {
		t.Fatalf("expected exactly one delete action, got %+v", plan.Summary)
	}
}

func TestBuildPlanManagedRecreateDeletesAllManagedCampaigns(t *testing.T) {
	desired := diff.State{
		Campaigns: []diff.Campaign{{Name: "US - Brand - Exact", DailyBudget: mustDecimal(t, "1.50"), Status: spec.StatusActive}},
		AdGroups:  []diff.AdGroup{{CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}},
	}
	remote := diff.State{
		Campaigns: []diff.Campaign{
			{ID: "c1", Name: "US - Brand - Exact", DailyBudget: mustDecimal(t, "1.00"), Status: spec.StatusActive},
			{ID: "c2", Name: "Legacy Campaign", DailyBudget: mustDecimal(t, "1.00"), Status: spec.StatusActive},
		},
		AdGroups: []diff.AdGroup{
			{ID: "g2", CampaignName: "Legacy Campaign", Name: "Legacy Ad Group", DefaultCPTBid: mustDecimal(t, "0.80"), Status: spec.StatusActive},
		},
	}

	plan := diff.BuildPlanWithOptions(desired, remote, diff.PlanOptions{
		RecreateScope:     diff.RecreateScopeManaged,
		RecreateCampaigns: remote.Campaigns,
	})
	text := diff.RenderText(plan)

	if !strings.Contains(text, `DELETE campaign "US - Brand - Exact"`) {
		t.Fatalf("expected matching managed campaign to be deleted in recreate mode, got:\n%s", text)
	}
	if !strings.Contains(text, `DELETE campaign "Legacy Campaign"`) {
		t.Fatalf("expected legacy managed campaign to be deleted in recreate mode, got:\n%s", text)
	}
	if strings.Contains(text, "DELETE adgroup") {
		t.Fatalf("expected descendant deletes to be pruned under recreated campaigns, got:\n%s", text)
	}
}

func TestBuildPlanWipeOrgDeletesAllCampaignsAndRecreatesState(t *testing.T) {
	desired := diff.State{
		Campaigns: []diff.Campaign{{Name: "US - Brand - Exact", DailyBudget: mustDecimal(t, "1.50"), Status: spec.StatusActive}},
		AdGroups:  []diff.AdGroup{{CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}},
	}
	remote := diff.State{
		Campaigns: []diff.Campaign{
			{ID: "c1", Name: "US - Brand - Exact", DailyBudget: mustDecimal(t, "1.00"), Status: spec.StatusActive},
			{ID: "c2", Name: "Legacy Campaign", DailyBudget: mustDecimal(t, "1.00"), Status: spec.StatusActive},
		},
	}

	plan := diff.BuildPlanWithOptions(desired, remote, diff.PlanOptions{
		RecreateScope:     diff.RecreateScopeOrg,
		RecreateCampaigns: remote.Campaigns,
	})
	text := diff.RenderText(plan)

	if !strings.Contains(text, `DELETE campaign "US - Brand - Exact"`) || !strings.Contains(text, `DELETE campaign "Legacy Campaign"`) {
		t.Fatalf("expected org-wide wipe to delete all remote campaigns, got:\n%s", text)
	}
	if plan.Summary.Delete != 2 {
		t.Fatalf("expected delete summary count to be 2, got %d", plan.Summary.Delete)
	}
}

func TestBuildPlanRemoteDeleteUsesRemoteOnlyContext(t *testing.T) {
	remote := diff.State{
		Campaigns: []diff.Campaign{{
			ID:          "c1",
			Name:        "Legacy Campaign",
			DailyBudget: mustDecimal(t, "1.00"),
			Status:      spec.StatusActive,
		}},
	}

	plan := diff.BuildPlan(diff.State{}, remote)
	if len(plan.Actions) != 1 {
		t.Fatalf("expected one delete action, got %+v", plan.Actions)
	}
	if plan.Actions[0].CampaignName != "Legacy Campaign" {
		t.Fatalf("expected campaign name to survive remote context annotation, got %+v", plan.Actions[0])
	}
	if plan.Actions[0].SourcePath != "" {
		t.Fatalf("expected remote delete to stay unsourced, got %+v", plan.Actions[0])
	}
	text := diff.RenderText(plan)
	if strings.Contains(text, "File: ") {
		t.Fatalf("expected no source grouping for fetched deletes, got:\n%s", text)
	}
}

func TestBuildPlanRecreateUsesExplicitRemoteCampaignContext(t *testing.T) {
	plan := diff.BuildPlanWithOptions(diff.State{}, diff.State{}, diff.PlanOptions{
		RecreateScope: diff.RecreateScopeManaged,
		RecreateCampaigns: []diff.Campaign{{
			ID:          "c1",
			Name:        "Legacy Campaign",
			DailyBudget: mustDecimal(t, "1.00"),
			Status:      spec.StatusActive,
		}},
	})
	if len(plan.Actions) != 1 {
		t.Fatalf("expected one delete action, got %+v", plan.Actions)
	}
	if plan.Actions[0].CampaignName != "Legacy Campaign" {
		t.Fatalf("expected explicit recreate campaign to keep campaign context, got %+v", plan.Actions[0])
	}
	if plan.Actions[0].SourcePath != "" {
		t.Fatalf("expected explicit recreate delete to stay unsourced, got %+v", plan.Actions[0])
	}
	text := diff.RenderText(plan)
	if strings.Contains(text, "File: ") {
		t.Fatalf("expected no source grouping for explicit recreate deletes, got:\n%s", text)
	}
}

func TestBuildPlanCampaignStorefrontOrderIsNoop(t *testing.T) {
	desired := diff.State{
		Campaigns: []diff.Campaign{{
			Name:        "US - Brand - Exact",
			Storefronts: []string{"US", "CA"},
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusActive,
		}},
	}
	remote := diff.State{
		Campaigns: []diff.Campaign{{
			ID:          "c1",
			Name:        "US - Brand - Exact",
			Storefronts: []string{"CA", "US"},
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusActive,
		}},
	}

	plan := diff.BuildPlan(desired, remote)
	text := diff.RenderText(plan)
	if strings.Contains(text, "UPDATE campaign") {
		t.Fatalf("expected storefront ordering to be ignored, got:\n%s", text)
	}
}

func TestBuildPlanMissingDesiredAlreadyPausedIsDelete(t *testing.T) {
	testCases := []struct {
		name     string
		resource string
		state    diff.State
	}{
		{
			name:     "campaign",
			resource: "campaign",
			state: diff.State{
				Campaigns: []diff.Campaign{{ID: "c1", Name: "Legacy Campaign", DailyBudget: mustDecimal(t, "1.00"), Status: spec.StatusPaused}},
			},
		},
		{
			name:     "adgroup",
			resource: "adgroup",
			state: diff.State{
				AdGroups: []diff.AdGroup{{ID: "g1", CampaignName: "Legacy Campaign", Name: "Legacy Ad Group", DefaultCPTBid: mustDecimal(t, "0.80"), Status: spec.StatusPaused}},
			},
		},
		{
			name:     "keyword",
			resource: "keyword",
			state: diff.State{
				Keywords: []diff.Keyword{{ID: "k1", CampaignName: "Legacy Campaign", AdGroupName: "Legacy Ad Group", Text: "legacy", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "0.80"), Status: spec.StatusPaused}},
			},
		},
		{
			name:     "negative keyword",
			resource: "negative keyword",
			state: diff.State{
				NegativeKeywords: []diff.NegativeKeyword{{ID: "n1", Scope: diff.ScopeCampaign, CampaignName: "Legacy Campaign", Text: "legacy", MatchType: spec.MatchTypeExact, Status: spec.StatusPaused}},
			},
		},
		{
			name:     "custom ad",
			resource: "custom ad",
			state: diff.State{
				CustomAds: []diff.CustomAd{{ID: "ad1", CampaignName: "Legacy Campaign", AdGroupName: "Legacy Ad Group", ProductPage: "CPP1", Status: spec.StatusPaused}},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			plan := diff.BuildPlan(diff.State{}, testCase.state)
			text := diff.RenderText(plan)
			if strings.Contains(text, "PAUSE "+testCase.resource) {
				t.Fatalf("expected delete for stale %s, got:\n%s", testCase.name, text)
			}
			if !strings.Contains(text, "DELETE "+testCase.resource) {
				t.Fatalf("expected delete for %s, got:\n%s", testCase.name, text)
			}
		})
	}
}

func TestBuildPlanAdgroupProductPageNotDiffedWhenRemoteProductPageUnavailable(t *testing.T) {
	desired := diff.State{
		AdGroups: []diff.AdGroup{{
			CampaignName:  "US - Category - Exact",
			Name:          "Tracker Core",
			DefaultCPTBid: mustDecimal(t, "1.20"),
			ProductPage:   "CPP1",
			Targeting:     spec.TargetingKeywords,
			Status:        spec.StatusActive,
		}},
	}
	remote := diff.State{
		AdGroups: []diff.AdGroup{{
			ID:            "g1",
			CampaignName:  "US - Category - Exact",
			Name:          "Tracker Core",
			DefaultCPTBid: mustDecimal(t, "1.20"),
			ProductPage:   "",
			Targeting:     spec.TargetingKeywords,
			Status:        spec.StatusActive,
		}},
	}

	plan := diff.BuildPlan(desired, remote)
	text := diff.RenderText(plan)
	if strings.Contains(text, "UPDATE adgroup") {
		t.Fatalf("expected adgroup product_page to be managed via custom ads only, got:\n%s", text)
	}
}

func TestBuildPlanPrunesDeletesCoveredByAdGroupDelete(t *testing.T) {
	desired := diff.State{
		Campaigns: []diff.Campaign{{
			Name:        "US - Brand - Exact",
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusActive,
		}},
	}
	remote := diff.State{
		Campaigns: []diff.Campaign{{
			ID:          "c1",
			Name:        "US - Brand - Exact",
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusActive,
		}},
		AdGroups: []diff.AdGroup{{
			ID:            "g1",
			CampaignName:  "US - Brand - Exact",
			Name:          "Brand",
			DefaultCPTBid: mustDecimal(t, "0.90"),
			Status:        spec.StatusActive,
		}},
		Keywords: []diff.Keyword{{
			ID:           "k1",
			CampaignName: "US - Brand - Exact",
			AdGroupName:  "Brand",
			Text:         "readcap",
			MatchType:    spec.MatchTypeExact,
			Bid:          mustDecimal(t, "1.10"),
			Status:       spec.StatusActive,
		}},
		NegativeKeywords: []diff.NegativeKeyword{
			{
				ID:           "n1",
				Scope:        diff.ScopeAdGroup,
				CampaignName: "US - Brand - Exact",
				AdGroupName:  "Brand",
				Text:         "free",
				MatchType:    spec.MatchTypeBroad,
				Status:       spec.StatusActive,
			},
			{
				ID:           "n2",
				Scope:        diff.ScopeCampaign,
				CampaignName: "US - Brand - Exact",
				Text:         "library",
				MatchType:    spec.MatchTypeExact,
				Status:       spec.StatusActive,
			},
		},
		CustomAds: []diff.CustomAd{{
			ID:           "ad1",
			CampaignName: "US - Brand - Exact",
			AdGroupName:  "Brand",
			ProductPage:  "CPP1",
			Status:       spec.StatusActive,
		}},
	}

	plan := diff.BuildPlan(desired, remote)
	if plan.Summary.Delete != 2 {
		t.Fatalf("expected exactly 2 delete actions after adgroup pruning, got %+v", plan.Summary)
	}

	var sawAdGroupDelete bool
	var sawCampaignNegativeDelete bool
	for _, action := range plan.Actions {
		if action.Operation != diff.OperationDelete {
			continue
		}
		switch action.Kind {
		case diff.ResourceAdGroup:
			sawAdGroupDelete = true
		case diff.ResourceNegativeKeyword:
			current, ok := action.Current.(diff.NegativeKeyword)
			if !ok {
				t.Fatalf("expected negative keyword current payload, got %#v", action.Current)
			}
			if current.Scope == diff.ScopeCampaign {
				sawCampaignNegativeDelete = true
			} else {
				t.Fatalf("expected adgroup-scoped negative delete to be pruned, got %+v", action)
			}
		case diff.ResourceKeyword, diff.ResourceCustomAd:
			t.Fatalf("expected child delete action to be pruned, got %+v", action)
		}
	}
	if !sawAdGroupDelete {
		t.Fatal("expected adgroup delete to remain in plan")
	}
	if !sawCampaignNegativeDelete {
		t.Fatal("expected campaign-scoped negative delete to remain in plan")
	}
}

func TestBuildDesiredStateUsesRuleSourceForGeneratedCampaignNegatives(t *testing.T) {
	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "asactl.yaml"), []byte(`version: 1
kind: Manifest
base: base.yaml
campaigns:
  - rules.yaml
  - campaigns.yaml
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "base.yaml"), []byte(`version: 1
kind: Base
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: Readcap
  app_id: "1"
defaults:
  currency: EUR
product_pages:
  CPP1:
    product_page_id: pp1
    name: Reading Tracker
    locale: en-US
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "rules.yaml"), []byte(`version: 1
kind: Campaigns
generators:
  - name: discovery-block-brand-exact
    kind: KeywordToNegative
    spec:
      source_refs:
        campaigns: [US - Brand]
      target_ref:
        campaign: US - Discovery
      filters:
        keyword_match_types: [EXACT]
      generate:
        campaign_negative_keywords:
          match_type: EXACT
          status: ACTIVE
campaigns: []
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "campaigns.yaml"), []byte(`version: 1
kind: Campaigns
campaigns:
  - name: US - Brand
    storefronts: [US]
    daily_budget: 1.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 0.50
        product_page: CPP1
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 0.50
            status: ACTIVE
  - name: US - Discovery
    storefronts: [US]
    daily_budget: 1.00
    status: ACTIVE
    adgroups:
      - name: Discovery - Search Match
        status: ACTIVE
        default_cpt_bid: 0.50
        targeting: SEARCH_MATCH
`), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := spec.LoadFile(filepath.Join(tempDir, "asactl.yaml"))
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	plan := diff.BuildPlan(diff.BuildDesiredState(loaded), diff.State{})
	found := false
	for _, action := range plan.Actions {
		if action.Kind != diff.ResourceNegativeKeyword || action.Operation != diff.OperationCreate {
			continue
		}
		if action.SourcePath != "rules.yaml" {
			continue
		}
		if !strings.Contains(action.Description, `"readcap"`) {
			continue
		}
		found = true
		break
	}
	if !found {
		t.Fatalf("expected generated negative keyword action to be attributed to rules.yaml, got %+v", plan.Actions)
	}
}

func mustDecimal(t *testing.T, value string) spec.Decimal {
	t.Helper()
	decimal, err := spec.ParseDecimal(value)
	if err != nil {
		t.Fatalf("parse decimal %q: %v", value, err)
	}
	return decimal
}
