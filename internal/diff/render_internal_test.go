package diff

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
)

func TestRenderTextGroupsBySourceThenCampaign(t *testing.T) {
	plan := Plan{
		Actions: []Action{
			{
				Operation:    OperationCreate,
				Kind:         ResourceKeyword,
				Description:  `"readcap" (EXACT) in adgroup "Brand"`,
				SourcePath:   "campaigns/us.yaml",
				CampaignName: "US - Brand",
				AdGroupName:  "Brand",
				context: actionContext{
					sourcePath:    "campaigns/us.yaml",
					sourceOrder:   0,
					campaignName:  "US - Brand",
					campaignOrder: 1,
					adGroupName:   "Brand",
				},
			},
			{
				Operation:    OperationCreate,
				Kind:         ResourceKeyword,
				Description:  `"readcap" (EXACT) in adgroup "Brand"`,
				SourcePath:   "campaigns/ex_us_english.yaml",
				CampaignName: "EN Core - Brand",
				AdGroupName:  "Brand",
				context: actionContext{
					sourcePath:    "campaigns/ex_us_english.yaml",
					sourceOrder:   1,
					campaignName:  "EN Core - Brand",
					campaignOrder: 2,
					adGroupName:   "Brand",
				},
			},
			{
				Operation:    OperationDelete,
				Kind:         ResourceCampaign,
				Description:  `"Legacy Campaign"`,
				CampaignName: "Legacy Campaign",
				context: actionContext{
					campaignName:  "Legacy Campaign",
					campaignOrder: -1,
					sourceOrder:   -1,
				},
			},
		},
		Summary: Summary{Create: 2, Delete: 1, Total: 3},
	}

	rendered := RenderText(plan)
	if !strings.Contains(rendered, "File: campaigns/us.yaml") {
		t.Fatalf("expected first source header, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "File: campaigns/ex_us_english.yaml") {
		t.Fatalf("expected second source header, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "Remote-only") {
		t.Fatalf("expected remote-only section, got:\n%s", rendered)
	}
	if strings.Index(rendered, "File: campaigns/us.yaml") > strings.Index(rendered, "File: campaigns/ex_us_english.yaml") {
		t.Fatalf("expected manifest source order, got:\n%s", rendered)
	}
}

func TestRenderTextCollapsesNoopsByCampaign(t *testing.T) {
	plan := Plan{
		Actions: []Action{
			{
				Operation:    OperationNoop,
				Kind:         ResourceCampaign,
				Description:  `"US - Brand" unchanged`,
				CampaignName: "US - Brand",
				context: actionContext{
					campaignName:  "US - Brand",
					campaignOrder: 0,
					sourceOrder:   -1,
				},
			},
			{
				Operation:    OperationNoop,
				Kind:         ResourceKeyword,
				Description:  `"readcap" (EXACT) in adgroup "Brand" unchanged`,
				CampaignName: "US - Brand",
				AdGroupName:  "Brand",
				context: actionContext{
					campaignName:  "US - Brand",
					campaignOrder: 0,
					adGroupName:   "Brand",
					sourceOrder:   -1,
				},
			},
		},
		Summary: Summary{Noop: 2, Total: 2},
	}

	rendered := RenderText(plan)
	if strings.Contains(rendered, `NOOP campaign "US - Brand" unchanged`) {
		t.Fatalf("expected noop campaign action to be collapsed, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "NOOP 2 unchanged actions collapsed") {
		t.Fatalf("expected collapsed noop summary, got:\n%s", rendered)
	}
}

func TestBuildPlanIncludesSourceContextFields(t *testing.T) {
	desired := BuildDesiredState(spec.Spec{
		Version: 1,
		Campaigns: []spec.Campaign{{
			Name:        "EN Core - Brand",
			Storefronts: []string{"GB"},
			DailyBudget: mustDecimal(t, "1.00"),
			Status:      spec.StatusActive,
			AdGroups: []spec.AdGroup{{
				Name:          "Brand",
				Status:        spec.StatusActive,
				DefaultCPTBid: mustDecimal(t, "0.50"),
				ProductPage:   "CPP1",
				Targeting:     spec.TargetingKeywords,
				Keywords: []spec.Keyword{{
					Text:      "readcap",
					MatchType: spec.MatchTypeExact,
					Bid:       mustDecimal(t, "0.50"),
					Status:    spec.StatusActive,
				}},
			}},
		}},
		Meta: spec.Meta{
			Composed: true,
			CampaignSources: map[string]spec.CampaignSource{
				spec.Fold("EN Core - Brand"): {SourcePath: "campaigns/ex_us_english.yaml", SourceOrder: 1},
			},
		},
	})

	plan := BuildPlan(desired, State{})
	var keywordAction Action
	for _, action := range plan.Actions {
		if action.Kind == ResourceKeyword {
			keywordAction = action
			break
		}
	}
	if keywordAction.SourcePath != "campaigns/ex_us_english.yaml" {
		t.Fatalf("expected source path, got %#v", keywordAction)
	}
	if keywordAction.CampaignName != "EN Core - Brand" {
		t.Fatalf("expected campaign name, got %#v", keywordAction)
	}
	if keywordAction.AdGroupName != "Brand" {
		t.Fatalf("expected adgroup name, got %#v", keywordAction)
	}
}

func TestBuildDesiredStateAssignsGeneratedNegativeToRuleSource(t *testing.T) {
	tempDir := t.TempDir()
	manifestPath := filepath.Join(tempDir, "asactl.yaml")
	if err := os.WriteFile(manifestPath, []byte(`version: 1
kind: Manifest
base: base.yaml
campaigns:
  - campaigns/rules.yaml
  - campaigns/targets.yaml
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
	campaignsDir := filepath.Join(tempDir, "campaigns")
	if err := os.MkdirAll(campaignsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(campaignsDir, "rules.yaml"), []byte(`version: 1
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
campaigns:
  - name: US - Brand
    storefronts: [US]
    daily_budget: 1.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 0.50
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 0.50
            status: ACTIVE
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(campaignsDir, "targets.yaml"), []byte(`version: 1
kind: Campaigns
campaigns:
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

	loaded, err := spec.LoadFile(manifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	desired := BuildDesiredState(loaded)
	var generated NegativeKeyword
	found := false
	for _, negative := range desired.NegativeKeywords {
		if negative.Scope == ScopeCampaign && negative.CampaignName == "US - Discovery" && negative.Text == "readcap" {
			generated = negative
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected generated campaign negative in desired state, got %+v", desired.NegativeKeywords)
	}
	if generated.context.sourcePath != "campaigns/rules.yaml" {
		t.Fatalf("expected generated negative to use generator source, got %#v", generated)
	}
	if generated.context.sourceOrder != 0 {
		t.Fatalf("expected generator source order 0, got %#v", generated)
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
