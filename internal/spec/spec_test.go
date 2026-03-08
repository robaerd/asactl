package spec_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/testutil"
	"gopkg.in/yaml.v3"
)

func TestNormalizeGeneratesDiscoveryOverlapNegatives(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	normalized := spec.Normalize(loaded)
	var discovery *spec.Campaign
	for i := range normalized.Campaigns {
		if normalized.Campaigns[i].Name == "US - Discovery" {
			discovery = &normalized.Campaigns[i]
			break
		}
	}
	if discovery == nil {
		t.Fatal("discovery campaign not found")
	}
	foundReadcap := false
	for _, negative := range discovery.CampaignNegativeKeywords {
		if negative.Text == "readcap" && negative.MatchType == spec.MatchTypeExact {
			foundReadcap = true
		}
	}
	if !foundReadcap {
		t.Fatal("expected generated exact overlap negative for readcap")
	}
}

func TestFormatRoundTrips(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	formatted, err := spec.Format(loaded)
	if err != nil {
		t.Fatalf("format spec: %v", err)
	}
	reloaded, err := spec.Load(formatted)
	if err != nil {
		t.Fatalf("reload formatted spec: %v", err)
	}
	if reloaded.Version != 1 {
		t.Fatalf("expected version 1, got %d", reloaded.Version)
	}
	if !strings.Contains(string(formatted), "kind: Config") {
		t.Fatal("formatted yaml missing kind: Config")
	}
	if !strings.Contains(string(formatted), "currency: EUR") {
		t.Fatal("formatted yaml missing defaults.currency")
	}
}

func TestDecimalMarshalYAMLRoundTripsFixedScale(t *testing.T) {
	type decimalDocument struct {
		Value spec.Decimal `yaml:"value"`
	}

	document := decimalDocument{Value: testutil.MustDecimal(t, "1.20")}
	formatted, err := yaml.Marshal(document)
	if err != nil {
		t.Fatalf("marshal yaml: %v", err)
	}
	if !strings.Contains(string(formatted), "1.20") {
		t.Fatalf("expected fixed-scale decimal output, got %q", formatted)
	}

	var decoded decimalDocument
	if err := yaml.Unmarshal(formatted, &decoded); err != nil {
		t.Fatalf("unmarshal yaml: %v", err)
	}
	if decoded.Value.String() != "1.20" {
		t.Fatalf("expected decimal to round-trip, got %s", decoded.Value.String())
	}
}

// Fixed-decimal YAML output relies on yaml.v3 round-tripping float-tagged scalars without changing their numeric value.
func TestFormatRoundTripsFixedDecimals(t *testing.T) {
	input := spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{
			ID: "20744842",
		},
		Auth: spec.Auth{Profile: "default"},
		App: spec.App{
			Name:  "Readcap",
			AppID: "1613230582",
		},
		Defaults: spec.Defaults{
			Currency: "EUR",
		},
		Campaigns: []spec.Campaign{{
			Name:        "US - Brand",
			Storefronts: []string{"US"},
			DailyBudget: testutil.MustDecimal(t, "1"),
			Status:      spec.StatusActive,
			AdGroups: []spec.AdGroup{{
				Name:          "Brand",
				Status:        spec.StatusActive,
				DefaultCPTBid: testutil.MustDecimal(t, "0.5"),
				Targeting:     spec.TargetingKeywords,
				Keywords: []spec.Keyword{{
					Text:      "readcap",
					MatchType: spec.MatchTypeExact,
					Bid:       testutil.MustDecimal(t, "2"),
					Status:    spec.StatusActive,
				}},
			}},
		}},
	}

	formatted, err := spec.Format(input)
	if err != nil {
		t.Fatalf("format spec: %v", err)
	}

	output := string(formatted)
	if !strings.Contains(output, "daily_budget: 1.00") {
		t.Fatalf("expected fixed-decimal campaign budget, got:\n%s", formatted)
	}
	if !strings.Contains(output, "default_cpt_bid: 0.50") {
		t.Fatalf("expected fixed-decimal default CPT bid, got:\n%s", formatted)
	}
	if !strings.Contains(output, "bid: 2.00") {
		t.Fatalf("expected fixed-decimal keyword bid, got:\n%s", formatted)
	}

	reloaded, err := spec.Load(formatted)
	if err != nil {
		t.Fatalf("reload formatted spec: %v", err)
	}
	if got := reloaded.Campaigns[0].DailyBudget.String(); got != "1.00" {
		t.Fatalf("expected campaign budget round-trip to preserve value, got %s", got)
	}
	if got := reloaded.Campaigns[0].AdGroups[0].DefaultCPTBid.String(); got != "0.50" {
		t.Fatalf("expected adgroup bid round-trip to preserve value, got %s", got)
	}
	if got := reloaded.Campaigns[0].AdGroups[0].Keywords[0].Bid.String(); got != "2.00" {
		t.Fatalf("expected keyword bid round-trip to preserve value, got %s", got)
	}
}

func TestFormatPreservesDuplicateKeywords(t *testing.T) {
	loaded := testutil.MustLoadConfigFixture(t)
	duplicate := spec.Keyword{
		Text:      "format-preserve-dup",
		MatchType: spec.MatchTypeExact,
		Bid:       testutil.MustDecimal(t, "0.50"),
		Status:    spec.StatusActive,
	}
	loaded.Campaigns[0].AdGroups[0].Keywords = append(loaded.Campaigns[0].AdGroups[0].Keywords, duplicate, duplicate)

	formatted, err := spec.Format(loaded)
	if err != nil {
		t.Fatalf("format spec: %v", err)
	}
	if count := strings.Count(string(formatted), "{ text: format-preserve-dup, match_type: EXACT, bid: 0.50, status: ACTIVE }"); count != 2 {
		t.Fatalf("expected duplicate keyword to be preserved, got %d occurrences in:\n%s", count, formatted)
	}
}

func TestFormatUsesFlowStyleForKeywordsAndNegatives(t *testing.T) {
	formatted, err := spec.Format(mustFormatAlignmentFixture(t))
	if err != nil {
		t.Fatalf("format spec: %v", err)
	}

	output := string(formatted)
	if !regexp.MustCompile(`- \{ text: readcap,\s{2,}match_type: EXACT, bid: 1\.50, status: ACTIVE \}`).MatchString(output) {
		t.Fatalf("expected keyword flow style, got:\n%s", formatted)
	}
	if !strings.Contains(output, "- { text: readcap reading tracker, match_type: EXACT, bid: 1.50, status: ACTIVE }") {
		t.Fatalf("expected keyword alignment with longest text, got:\n%s", formatted)
	}
	if !strings.Contains(output, "- { text: goodreads the book, match_type: EXACT, status: ACTIVE }") {
		t.Fatalf("expected campaign negative flow style, got:\n%s", formatted)
	}
	if !regexp.MustCompile(`- \{ text: short,\s{2,}match_type: BROAD, status: PAUSED \}`).MatchString(output) {
		t.Fatalf("expected negative keyword alignment, got:\n%s", formatted)
	}
	if !strings.Contains(output, `- { text: 'quoted: value', match_type: BROAD, status: PAUSED }`) &&
		!strings.Contains(output, `- { text: "quoted: value", match_type: BROAD, status: PAUSED }`) {
		t.Fatalf("expected quoted adgroup negative flow style, got:\n%s", formatted)
	}
	if strings.Contains(output, "- {name: discovery-block-brand-exact") {
		t.Fatalf("expected generators to remain block style, got:\n%s", formatted)
	}
	if !strings.Contains(output, "generators:\n  - name: discovery-block-brand-exact\n    kind: KeywordToNegative\n") {
		t.Fatalf("expected generators block style, got:\n%s", formatted)
	}
}

func TestFormatCampaignsFileDoesNotMaterializeGeneratedNegatives(t *testing.T) {
	campaigns := spec.CampaignsFile{
		Version: 1,
		Kind:    spec.KindCampaigns,
		Generators: []spec.Generator{{
			Name: "discovery-block-brand-exact",
			Kind: spec.GeneratorKindKeywordToNegative,
			Spec: spec.GeneratorSpec{
				SourceRefs: spec.GeneratorSourceRefs{Campaigns: []string{"US - Brand"}},
				TargetRef:  spec.GeneratorTargetRef{Campaign: "US - Discovery"},
				Filters:    spec.GeneratorFilters{KeywordMatchTypes: []spec.MatchType{spec.MatchTypeExact}},
				Generate: spec.GeneratorGenerate{
					CampaignNegativeKeywords: spec.GeneratorNegativeKeywordSpec{
						MatchType: spec.MatchTypeExact,
						Status:    spec.StatusActive,
					},
				},
			},
		}},
		Campaigns: []spec.Campaign{
			{
				Name:        "US - Brand",
				Storefronts: []string{"US"},
				DailyBudget: testutil.MustDecimal(t, "1.00"),
				Status:      spec.StatusActive,
				AdGroups: []spec.AdGroup{{
					Name:          "Brand",
					Status:        spec.StatusActive,
					DefaultCPTBid: testutil.MustDecimal(t, "0.50"),
					Targeting:     spec.TargetingKeywords,
					Keywords: []spec.Keyword{{
						Text:      "readcap",
						MatchType: spec.MatchTypeExact,
						Bid:       testutil.MustDecimal(t, "0.50"),
						Status:    spec.StatusActive,
					}},
				}},
			},
			{
				Name:        "US - Discovery",
				Storefronts: []string{"US"},
				DailyBudget: testutil.MustDecimal(t, "1.00"),
				Status:      spec.StatusActive,
				AdGroups: []spec.AdGroup{{
					Name:          "Discovery - Search Match",
					Status:        spec.StatusActive,
					DefaultCPTBid: testutil.MustDecimal(t, "0.50"),
					Targeting:     spec.TargetingSearchMatch,
				}},
			},
		},
	}

	formatted, err := spec.FormatCampaignsFile(campaigns)
	if err != nil {
		t.Fatalf("format campaigns file: %v", err)
	}
	if strings.Contains(string(formatted), "\n    campaign_negative_keywords:\n      -") {
		t.Fatalf("expected formatter to preserve declarative rules without materializing negatives:\n%s", formatted)
	}
	if !strings.Contains(string(formatted), "generators:") {
		t.Fatalf("expected formatter to preserve generators:\n%s", formatted)
	}
}

func TestLoadRejectsNonMapping(t *testing.T) {
	_, err := spec.Load([]byte("- nope\n"))
	if err == nil || !strings.Contains(err.Error(), "mapping") {
		t.Fatalf("expected mapping error, got %v", err)
	}
}

func TestLoadFile(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "sample.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nkind: Config\ncampaign_group:\n  id: \"20744842\"\nauth:\n  profile: default\napp:\n  name: x\n  app_id: \"1\"\ndefaults:\n  currency: EUR\n  devices: [IPHONE]\nproduct_pages:\n  CPP1:\n    product_page_id: pp1\n    name: x\n    locale: en-US\ncampaigns: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := spec.LoadFile(path)
	if err != nil {
		t.Fatalf("load file: %v", err)
	}
	if loaded.App.Name != "x" {
		t.Fatalf("unexpected app name %q", loaded.App.Name)
	}
}

func TestLoadRejectsLegacyAppAdamID(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy-app-id.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nkind: Config\ncampaign_group:\n  id: \"20744842\"\nauth:\n  profile: default\napp:\n  name: x\n  adam_id: \"1\"\ndefaults:\n  currency: EUR\ncampaigns: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), `field "app.adam_id" was renamed to "app.app_id"`) {
		t.Fatalf("expected legacy app.adam_id failure, got %v", err)
	}
}

func TestLoadRejectsLegacyNegativeKeywordRules(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy-rules.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nkind: Config\ncampaign_group:\n  id: \"20744842\"\nauth:\n  profile: default\napp:\n  name: x\n  app_id: \"1\"\ndefaults:\n  currency: EUR\nnegative_keyword_rules: []\ncampaigns: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "field negative_keyword_rules not found") {
		t.Fatalf("expected legacy negative_keyword_rules decode failure, got %v", err)
	}
}

func TestLoadDocumentRejectsLegacyBaseAdamID(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "base.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nkind: Base\ncampaign_group:\n  id: \"20744842\"\nauth:\n  profile: default\napp:\n  name: x\n  adam_id: \"1\"\ndefaults:\n  currency: EUR\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadDocumentFile(path)
	if err == nil || !strings.Contains(err.Error(), `field "app.adam_id" was renamed to "app.app_id"`) {
		t.Fatalf("expected legacy base app.adam_id failure, got %v", err)
	}
}

func TestLoadManifestFile(t *testing.T) {
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
  app_id: "1"
defaults:
  currency: EUR
  devices: [IPHONE]
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
	if err := os.WriteFile(filepath.Join(campaignsDir, "ex_us_english.yaml"), []byte(`version: 1
kind: Campaigns
campaigns:
  - name: EN Core - Brand
    storefronts: [GB, CA]
    daily_budget: 2.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 0.70
        product_page: CPP1
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 0.70
            status: ACTIVE
`), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := spec.LoadFile(filepath.Join(tempDir, "asactl.yaml"))
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.App.AppID != "1" {
		t.Fatalf("unexpected app_id %q", loaded.App.AppID)
	}
	if loaded.CampaignGroup.ID != "20744842" {
		t.Fatalf("unexpected campaign_group.id %q", loaded.CampaignGroup.ID)
	}
	if len(loaded.Campaigns) != 3 {
		t.Fatalf("expected 3 campaigns, got %d", len(loaded.Campaigns))
	}
	if len(loaded.Generators) != 1 {
		t.Fatalf("expected 1 generator, got %d", len(loaded.Generators))
	}
	if !loaded.Meta.Composed {
		t.Fatal("expected composed metadata to be set")
	}
	if source, ok := loaded.Meta.CampaignSources[spec.Fold("EN Core - Brand")]; !ok || source.SourcePath != "campaigns/ex_us_english.yaml" {
		t.Fatalf("expected source provenance for EN Core - Brand, got %#v ok=%v", source, ok)
	}
}

func TestLoadManifestRejectsDuplicateCampaignNamesAcrossFilesCaseInsensitive(t *testing.T) {
	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "asactl.yaml"), []byte(`version: 1
kind: Manifest
base: base.yaml
campaigns:
  - campaigns/brand.yaml
  - campaigns/brand-duplicate.yaml
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
  devices: [IPHONE]
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
	if err := os.WriteFile(filepath.Join(campaignsDir, "brand.yaml"), []byte(`version: 1
kind: Campaigns
campaigns:
  - name: US - Brand
    storefronts: [US]
    daily_budget: 1.00
    status: ACTIVE
    adgroups: []
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(campaignsDir, "brand-duplicate.yaml"), []byte(`version: 1
kind: Campaigns
campaigns:
  - name: "  us - brand  "
    storefronts: [US]
    daily_budget: 2.00
    status: ACTIVE
    adgroups: []
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(filepath.Join(tempDir, "asactl.yaml"))
	if err == nil {
		t.Fatal("expected duplicate campaign load failure")
	}
	message := err.Error()
	if !strings.Contains(message, "duplicate campaign name") {
		t.Fatalf("expected duplicate campaign error, got %v", err)
	}
	if !strings.Contains(message, `"us - brand"`) {
		t.Fatalf("expected duplicate campaign name in error, got %v", err)
	}
	if !strings.Contains(message, "campaigns/brand.yaml") || !strings.Contains(message, "campaigns/brand-duplicate.yaml") {
		t.Fatalf("expected both campaign files in error, got %v", err)
	}
}

func TestLoadFileRejectsBaseDocument(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "base.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
kind: Base
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: Readcap
  app_id: "1"
defaults:
product_pages:
  CPP1:
    product_page_id: pp1
    name: Reading Tracker
    locale: en-US
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "base file") {
		t.Fatalf("expected base file error, got %v", err)
	}
}

func TestLoadRejectsUnknownAuthField(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
kind: Config
auth:
  unexpected: ORG
app:
  name: x
  app_id: "1"
defaults:
product_pages:
  CPP1:
    product_page_id: pp1
    name: x
    locale: en-US
campaigns: []
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("expected unknown auth field decode failure, got %v", err)
	}
}

func TestLoadRejectsMissingKind(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "missing-kind.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: x
  app_id: "1"
defaults:
product_pages:
  CPP1:
    product_page_id: pp1
    name: x
    locale: en-US
campaigns: []
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), `kind is required`) {
		t.Fatalf("expected missing kind failure, got %v", err)
	}
}

func mustFormatAlignmentFixture(t *testing.T) spec.Spec {
	t.Helper()
	return spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{
			ID: "20744842",
		},
		Auth: spec.Auth{Profile: "default"},
		App: spec.App{
			Name:  "Readcap",
			AppID: "1613230582",
		},
		Defaults: spec.Defaults{
			Currency: "EUR",
			Devices:  []spec.Device{spec.DeviceIPhone},
		},
		ProductPages: spec.ProductPageMap{
			"CPP1": {
				ProductPageID: "pp1",
				Name:          "Reading Tracker",
				Locale:        "en-US",
			},
		},
		Generators: []spec.Generator{{
			Name: "discovery-block-brand-exact",
			Kind: spec.GeneratorKindKeywordToNegative,
			Spec: spec.GeneratorSpec{
				SourceRefs: spec.GeneratorSourceRefs{Campaigns: []string{"US - Brand"}},
				TargetRef:  spec.GeneratorTargetRef{Campaign: "US - Discovery"},
				Filters:    spec.GeneratorFilters{KeywordMatchTypes: []spec.MatchType{spec.MatchTypeExact}},
				Generate: spec.GeneratorGenerate{
					CampaignNegativeKeywords: spec.GeneratorNegativeKeywordSpec{
						MatchType: spec.MatchTypeExact,
						Status:    spec.StatusActive,
					},
				},
			},
		}},
		Campaigns: []spec.Campaign{
			{
				Name:        "US - Brand",
				Storefronts: []string{"US"},
				DailyBudget: testutil.MustDecimal(t, "10.00"),
				Status:      spec.StatusActive,
				AdGroups: []spec.AdGroup{{
					Name:          "Brand",
					Status:        spec.StatusActive,
					DefaultCPTBid: testutil.MustDecimal(t, "1.50"),
					ProductPage:   "CPP1",
					Targeting:     spec.TargetingKeywords,
					Keywords: []spec.Keyword{
						{Text: "readcap", MatchType: spec.MatchTypeExact, Bid: testutil.MustDecimal(t, "1.50"), Status: spec.StatusActive},
						{Text: "readcap reading tracker", MatchType: spec.MatchTypeExact, Bid: testutil.MustDecimal(t, "1.50"), Status: spec.StatusActive},
					},
					AdGroupNegativeKeywords: []spec.NegativeKeyword{
						{Text: "short", MatchType: spec.MatchTypeBroad, Status: spec.StatusPaused},
						{Text: "quoted: value", MatchType: spec.MatchTypeBroad, Status: spec.StatusPaused},
					},
				}},
			},
			{
				Name:        "US - Discovery",
				Storefronts: []string{"US"},
				DailyBudget: testutil.MustDecimal(t, "8.00"),
				Status:      spec.StatusActive,
				CampaignNegativeKeywords: []spec.NegativeKeyword{
					{Text: "goodreads the book", MatchType: spec.MatchTypeExact, Status: spec.StatusActive},
				},
				AdGroups: []spec.AdGroup{{
					Name:          "Discovery - Search Match",
					Status:        spec.StatusActive,
					DefaultCPTBid: testutil.MustDecimal(t, "1.20"),
					Targeting:     spec.TargetingSearchMatch,
				}},
			},
		},
	}
}

func TestLoadRejectsLegacyCompositionKinds(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy-manifest.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
kind: Composition
base: base.yaml
resources: []
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), `kind "Composition" is no longer supported; use "Manifest"`) {
		t.Fatalf("expected legacy composition kind failure, got %v", err)
	}
}

func TestLoadRejectsLegacyManifestResourcesField(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy-manifest.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
kind: Manifest
base: base.yaml
resources: []
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), `field "resources" is no longer supported in kind "Manifest"; use "campaigns"`) {
		t.Fatalf("expected legacy manifest resources failure, got %v", err)
	}
}

func TestLoadRejectsLegacyCampaignFragmentKind(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy-campaigns.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
kind: CampaignFragment
campaigns: []
`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := spec.LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), `kind "CampaignFragment" is no longer supported; use "Campaigns"`) {
		t.Fatalf("expected legacy campaign fragment kind failure, got %v", err)
	}
}
