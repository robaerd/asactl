package validate_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/testutil"
	"github.com/robaerd/asactl/internal/validate"
)

func TestValidateUSExample(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	result := validate.Run(loaded)
	if !result.OK() {
		t.Fatalf("expected valid example, got errors: %v", result.Errors)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", result.Warnings)
	}
}

func TestValidateComposedExample(t *testing.T) {
	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "asactl.yaml"), []byte(`version: 1
kind: Manifest
base: base.yaml
campaigns:
  - campaigns/us.yaml
  - campaigns/ex_us_english.yaml
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
  app_id: "1613230582"
defaults:
  currency: EUR
  devices: [IPHONE, IPAD]
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
	if err := os.WriteFile(filepath.Join(campaignsDir, "us.yaml"), []byte(`version: 1
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
    daily_budget: 10.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 1.50
        product_page: CPP1
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 1.50
            status: ACTIVE
  - name: US - Discovery
    storefronts: [US]
    daily_budget: 8.00
    status: ACTIVE
    adgroups:
      - name: Discovery - Search Match
        status: ACTIVE
        default_cpt_bid: 1.20
        targeting: SEARCH_MATCH
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(campaignsDir, "ex_us_english.yaml"), []byte(`version: 1
kind: Campaigns
campaigns:
  - name: EN Core - Brand
    storefronts: [GB, CA]
    daily_budget: 6.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 1.10
        product_page: CPP1
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 1.10
            status: ACTIVE
`), 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := spec.LoadFile(filepath.Join(tempDir, "asactl.yaml"))
	if err != nil {
		t.Fatalf("load composition: %v", err)
	}
	result := validate.Run(loaded)
	if !result.OK() {
		t.Fatalf("expected valid composed example, got errors: %v", result.Errors)
	}
}

func TestValidateSearchMatchWithoutNamingConventionAllowed(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].AdGroups[0].Targeting = spec.TargetingSearchMatch
	loaded.Campaigns[0].AdGroups[0].Keywords = nil
	result := validate.Run(loaded)
	if !result.OK() {
		t.Fatalf("expected validation success, got %v", result.Errors)
	}
}

func TestValidateUnknownProductPageFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].AdGroups[0].ProductPage = "CPP999"
	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected validation failure")
	}
}

func TestValidateDuplicateProductPageIDFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.ProductPages["CPP2"] = spec.ProductPage{
		ProductPageID: loaded.ProductPages["CPP1"].ProductPageID,
		Name:          loaded.ProductPages["CPP2"].Name,
		Locale:        loaded.ProductPages["CPP2"].Locale,
	}

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected validation failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), "duplicates product_page_id") {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateProductPageDuplicateErrorsAreDeterministic(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.ProductPages["CPP1"] = spec.ProductPage{ProductPageID: "pp1", Name: "Reading Tracker", Locale: "en-US"}
	loaded.ProductPages["CPP2"] = spec.ProductPage{ProductPageID: "pp1", Name: "Capture & Organize", Locale: "en-US"}

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected validation failure")
	}
	want := `product_pages "CPP2" duplicates product_page_id for product_pages "CPP1"`
	if len(result.Errors) == 0 || result.Errors[0] != want {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateMissingCurrencyWarns(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Defaults.Currency = ""

	result := validate.Run(loaded)
	if !result.OK() {
		t.Fatalf("expected warning-only result, got errors: %v", result.Errors)
	}
	if !strings.Contains(strings.Join(result.Warnings, "\n"), "defaults.currency is not set") {
		t.Fatalf("expected missing currency warning, got %v", result.Warnings)
	}
}

func TestValidateSearchMatchGroupWithKeywordsFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	discovery := &loaded.Campaigns[len(loaded.Campaigns)-1]
	searchMatchGroup := &discovery.AdGroups[len(discovery.AdGroups)-1]
	searchMatchGroup.Keywords = append(searchMatchGroup.Keywords, spec.Keyword{
		Text:      "unexpected keyword",
		MatchType: spec.MatchTypeExact,
		Bid:       testutil.MustDecimal(t, "0.50"),
		Status:    spec.StatusActive,
	})

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected validation failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), "must define zero keywords") {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateDuplicateCampaignFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns = append(loaded.Campaigns, loaded.Campaigns[0])

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected validation failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), "duplicate campaign name") {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateZeroCampaignBudgetFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].DailyBudget = testutil.MustDecimal(t, "0.00")

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected zero campaign budget failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `daily_budget must be > 0`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateNegativeCampaignBudgetFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].DailyBudget = testutil.MustDecimal(t, "-1.00")

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected negative campaign budget failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `daily_budget must be > 0`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateZeroDefaultCPTBidFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].AdGroups[0].DefaultCPTBid = testutil.MustDecimal(t, "0.00")

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected zero default CPT bid failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `default_cpt_bid must be > 0`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateNegativeDefaultCPTBidFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].AdGroups[0].DefaultCPTBid = testutil.MustDecimal(t, "-0.01")

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected negative default CPT bid failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `default_cpt_bid must be > 0`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateZeroKeywordBidFailsWithPauseGuidance(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].AdGroups[0].Keywords[0].Bid = testutil.MustDecimal(t, "0.00")

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected zero keyword bid failure")
	}
	joined := strings.Join(result.Errors, "\n")
	if !strings.Contains(joined, `bid must be > 0`) || !strings.Contains(joined, `status PAUSED`) || !strings.Contains(joined, `bid 0.00`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateNegativeKeywordBidFailsWithPauseGuidance(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Campaigns[0].AdGroups[0].Keywords[0].Bid = testutil.MustDecimal(t, "-0.01")

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected negative keyword bid failure")
	}
	joined := strings.Join(result.Errors, "\n")
	if !strings.Contains(joined, `bid must be > 0`) || !strings.Contains(joined, `status PAUSED`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateGeneratorRequiresAtLeastOneSourceCampaign(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Generators[0].Spec.SourceRefs.Campaigns = nil

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected empty source_refs.campaigns failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `must define at least one spec.source_refs.campaigns entry`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateGeneratorRejectsSelfReference(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	target := loaded.Generators[0].Spec.TargetRef.Campaign
	loaded.Generators[0].Spec.SourceRefs.Campaigns = []string{target}

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected self-referencing generator failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `must not include itself in spec.source_refs.campaigns`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateGeneratorRequiresSupportedV1Shape(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Generators[0].Spec.Filters.KeywordMatchTypes = []spec.MatchType{spec.MatchTypeBroad}
	loaded.Generators[0].Spec.Generate.CampaignNegativeKeywords.MatchType = spec.MatchTypeBroad
	loaded.Generators[0].Spec.Generate.CampaignNegativeKeywords.Status = spec.StatusPaused

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected unsupported generator shape failure")
	}
	joined := strings.Join(result.Errors, "\n")
	if !strings.Contains(joined, `spec.filters.keyword_match_types must be [EXACT] in v1`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
	if !strings.Contains(joined, `spec.generate.campaign_negative_keywords.match_type must be EXACT in v1`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
	if !strings.Contains(joined, `spec.generate.campaign_negative_keywords.status must be ACTIVE in v1`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateRejectsDuplicateGeneratorNames(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	duplicate := loaded.Generators[0]
	loaded.Generators = append(loaded.Generators, duplicate)

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected duplicate generator name failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), `duplicate generator name "discovery-block-brand-exact"`) {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateInvalidCurrencyFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.Defaults.Currency = "eur"

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected invalid currency failure")
	}
	joined := strings.Join(result.Errors, "\n")
	if !strings.Contains(joined, "defaults.currency") {
		t.Fatalf("expected defaults.currency error, got %v", result.Errors)
	}
}

func TestValidateMissingCampaignGroupIDFails(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.CampaignGroup.ID = ""

	result := validate.Run(loaded)
	if result.OK() {
		t.Fatal("expected missing campaign_group.id failure")
	}
	if !strings.Contains(strings.Join(result.Errors, "\n"), "campaign_group.id is required") {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestValidateOpaqueCampaignGroupIDAllowed(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	loaded.CampaignGroup.ID = "readcap-en"

	result := validate.Run(loaded)
	if !result.OK() {
		t.Fatalf("expected opaque campaign_group.id to be allowed, got %v", result.Errors)
	}
}
