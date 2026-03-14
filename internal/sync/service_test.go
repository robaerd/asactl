package sync

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/auth"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
)

type fakeAPI struct {
	fetchResult appleadsapi.FetchResult
	fetchErr    error
	fetchCalls  int
	checkResult appleadsapi.AuthCheckResult
	checkErr    error
	checkCalls  int
	applyErr    error
	applyCalls  int
	appliedSpec spec.Spec
	appliedPlan diff.Plan
}

func withAPIFactory(factory func(spec.Spec, auth.Config) (adsAPI, error)) EngineOption {
	return func(engine *Engine) {
		if factory != nil {
			engine.apiFactory = factory
		}
	}
}

func (api *fakeAPI) FetchState(context.Context, spec.Spec) (appleadsapi.FetchResult, error) {
	api.fetchCalls++
	return api.fetchResult, api.fetchErr
}

func (api *fakeAPI) CheckAuth(context.Context, spec.Spec) (appleadsapi.AuthCheckResult, error) {
	api.checkCalls++
	return api.checkResult, api.checkErr
}

func (api *fakeAPI) ApplyPlan(_ context.Context, input spec.Spec, plan diff.Plan) error {
	api.applyCalls++
	api.appliedSpec = input
	api.appliedPlan = plan
	return api.applyErr
}

func TestApplyDryRunAndMaxChanges(t *testing.T) {
	loaded := testSpec(t)

	api := &fakeAPI{}
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return api, nil }),
	)

	planned, err := engine.Plan(context.Background(), loaded, Options{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	result, err := engine.Apply(context.Background(), loaded, planned, Options{DryRun: true})
	if err != nil {
		t.Fatalf("dry run apply: %v", err)
	}
	if result.Applied {
		t.Fatal("dry run should not apply")
	}
	if api.applyCalls != 0 {
		t.Fatalf("expected no apply calls during dry run, got %d", api.applyCalls)
	}

	_, err = engine.Apply(context.Background(), loaded, planned, Options{MaxChanges: 1})
	if err == nil || !strings.Contains(err.Error(), "max-changes") {
		t.Fatalf("expected max-changes error, got %v", err)
	}
	if api.applyCalls != 0 {
		t.Fatalf("expected no apply calls when max changes exceeded, got %d", api.applyCalls)
	}
}

func TestSavedPlanRoundTrip(t *testing.T) {
	loaded := testSpec(t)

	api := &fakeAPI{}
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return api, nil }),
	)

	planned, saved, err := engine.PlanSaved(context.Background(), loaded, Options{})
	if err != nil {
		t.Fatalf("plan saved: %v", err)
	}

	content, err := saved.Bytes()
	if err != nil {
		t.Fatalf("saved plan bytes: %v", err)
	}
	parsed, ok, err := ParseSavedPlan(content)
	if err != nil {
		t.Fatalf("parse saved plan: %v", err)
	}
	if !ok {
		t.Fatal("expected saved plan to be recognized")
	}
	if parsed.Kind != SavedPlanKind || parsed.Version != SavedPlanVersion {
		t.Fatalf("unexpected saved plan header: %#v", parsed)
	}
	if parsed.Profile != "default" {
		t.Fatalf("expected saved plan profile=default, got %q", parsed.Profile)
	}
	if parsed.Result().Plan.Summary != planned.Plan.Summary {
		t.Fatalf("expected round-tripped plan summary %v, got %v", planned.Plan.Summary, parsed.Result().Plan.Summary)
	}
	resolved, err := parsed.ResolvedSpec()
	if err != nil {
		t.Fatalf("resolved spec: %v", err)
	}
	if resolved.CampaignGroup.ID != loaded.CampaignGroup.ID {
		t.Fatalf("expected resolved campaign_group.id %q, got %q", loaded.CampaignGroup.ID, resolved.CampaignGroup.ID)
	}
}

func TestSavedPlanRoundTripPreservesRenderMetadata(t *testing.T) {
	loaded := testSpec(t)
	content, err := spec.Format(loaded)
	if err != nil {
		t.Fatalf("format spec: %v", err)
	}

	plan := diff.Plan{Actions: []diff.Action{
		{
			Operation:    diff.OperationNoop,
			Kind:         diff.ResourceCampaign,
			Key:          spec.Fold(loaded.Campaigns[0].Name),
			Description:  `"US - Brand - Exact"`,
			SourcePath:   "campaigns/us.yaml",
			CampaignName: loaded.Campaigns[0].Name,
			Current:      diff.Campaign{ID: "1", Name: loaded.Campaigns[0].Name, Storefronts: []string{"US"}, DailyBudget: mustDecimal(t, "1.50"), Status: spec.StatusActive},
			Desired:      diff.Campaign{Name: loaded.Campaigns[0].Name, Storefronts: []string{"US"}, DailyBudget: mustDecimal(t, "1.50"), Status: spec.StatusActive},
		},
		{
			Operation:    diff.OperationDelete,
			Kind:         diff.ResourceCampaign,
			Key:          spec.Fold("Remote Campaign"),
			Description:  `"Remote Campaign"`,
			CampaignName: "Remote Campaign",
			Current:      diff.Campaign{ID: "9", Name: "Remote Campaign", Storefronts: []string{"US"}, DailyBudget: mustDecimal(t, "1.00"), Status: spec.StatusActive},
		},
	}}
	saved := SavedPlan{
		Kind:             SavedPlanKind,
		Version:          SavedPlanVersion,
		Profile:          "default",
		SpecYAML:         string(content),
		Plan:             plan,
		ActionRenderMeta: []diff.ActionRenderMetadata{{SourceOrder: 0, CampaignOrder: 0}, {SourceOrder: -1, CampaignOrder: -1, Remote: true}},
	}

	bytes, err := saved.Bytes()
	if err != nil {
		t.Fatalf("saved plan bytes: %v", err)
	}
	parsed, ok, err := ParseSavedPlan(bytes)
	if err != nil {
		t.Fatalf("parse saved plan: %v", err)
	}
	if !ok {
		t.Fatal("expected saved plan to be recognized")
	}

	rendered := diff.RenderText(parsed.Result().Plan)
	if !strings.Contains(rendered, "File: campaigns/us.yaml") {
		t.Fatalf("expected source grouping to survive round trip, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "Remote-only") {
		t.Fatalf("expected remote-only grouping to survive round trip, got:\n%s", rendered)
	}
}

func TestApplySavedPlanDryRunAndMaxChanges(t *testing.T) {
	loaded := testSpec(t)

	api := &fakeAPI{}
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return api, nil }),
	)

	_, saved, err := engine.PlanSaved(context.Background(), loaded, Options{})
	if err != nil {
		t.Fatalf("plan saved: %v", err)
	}

	result, err := engine.ApplySavedPlan(context.Background(), saved, Options{DryRun: true})
	if err != nil {
		t.Fatalf("dry run apply saved plan: %v", err)
	}
	if result.Applied {
		t.Fatal("dry-run saved plan apply should not apply")
	}
	if api.applyCalls != 0 {
		t.Fatalf("expected no apply calls during saved-plan dry run, got %d", api.applyCalls)
	}

	_, err = engine.ApplySavedPlan(context.Background(), saved, Options{MaxChanges: 1})
	if err == nil || !strings.Contains(err.Error(), "max-changes") {
		t.Fatalf("expected max-changes error, got %v", err)
	}
	if api.applyCalls != 0 {
		t.Fatalf("expected no apply calls when saved-plan max changes exceeded, got %d", api.applyCalls)
	}
}

func TestApplySavedPlanFailsWhenProfileMissing(t *testing.T) {
	loaded := testSpec(t)

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return &fakeAPI{}, nil }),
	)
	_, saved, err := engine.PlanSaved(context.Background(), loaded, Options{})
	if err != nil {
		t.Fatalf("plan saved: %v", err)
	}

	saved.Profile = "missing"
	_, err = engine.ApplySavedPlan(context.Background(), saved, Options{})
	if err == nil || !strings.Contains(err.Error(), `profile "missing" was not found`) {
		t.Fatalf("expected missing profile error, got %v", err)
	}
}

func TestApplySavedPlanRejectsMalformedPayloadBeforeApply(t *testing.T) {
	loaded := testSpec(t)
	api := &fakeAPI{}
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return api, nil }),
	)

	_, saved, err := engine.PlanSaved(context.Background(), loaded, Options{})
	if err != nil {
		t.Fatalf("plan saved: %v", err)
	}
	saved.Plan.Actions[0].Desired = "not-a-campaign"

	_, err = engine.ApplySavedPlan(context.Background(), saved, Options{})
	if err == nil || !strings.Contains(err.Error(), "desired campaign") {
		t.Fatalf("expected malformed payload error, got %v", err)
	}
	if api.applyCalls != 0 {
		t.Fatalf("expected malformed saved plan to fail before apply, got %d apply calls", api.applyCalls)
	}
}

func TestApplySavedPlanDryRunValidatesRuntimeConfig(t *testing.T) {
	loaded := testSpec(t)
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return &fakeAPI{}, nil }),
	)

	_, saved, err := engine.PlanSaved(context.Background(), loaded, Options{})
	if err != nil {
		t.Fatalf("plan saved: %v", err)
	}

	configPath := os.Getenv("ASACTL_CONFIG")
	if err := os.WriteFile(configPath, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/missing-private-key.pem"
`), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}

	_, err = engine.ApplySavedPlan(context.Background(), saved, Options{DryRun: true})
	if err == nil || !strings.Contains(err.Error(), "private key") {
		t.Fatalf("expected dry-run runtime validation error, got %v", err)
	}
}

func TestPlanAddsRecreateWarning(t *testing.T) {
	loaded := testSpec(t)

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return &fakeAPI{}, nil }),
	)

	planned, err := engine.Plan(context.Background(), loaded, Options{RecreateScope: diff.RecreateScopeManaged})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(planned.Warnings) == 0 || !strings.Contains(strings.Join(planned.Warnings, "\n"), "campaign_group.id + app.app_id scope") {
		t.Fatalf("expected recreate warning, got %v", planned.Warnings)
	}
}

func TestPlanAddsWipeOrgWarning(t *testing.T) {
	loaded := testSpec(t)

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return &fakeAPI{}, nil }),
	)

	planned, err := engine.Plan(context.Background(), loaded, Options{RecreateScope: diff.RecreateScopeOrg})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(planned.Warnings) == 0 || !strings.Contains(strings.Join(planned.Warnings, "\n"), "deletes all remote campaigns visible in the configured campaign group") {
		t.Fatalf("expected wipe-org warning, got %v", planned.Warnings)
	}
}

func TestCheckAuthUsesInjectedAPI(t *testing.T) {
	loaded := testSpec(t)

	api := &fakeAPI{
		checkResult: appleadsapi.AuthCheckResult{
			CampaignGroupID:  "org-1",
			OrgName:          "Readcap",
			AppID:            "123456",
			ProductPageCount: 2,
		},
	}
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return api, nil }),
	)

	result, err := engine.CheckAuth(context.Background(), loaded, "")
	if err != nil {
		t.Fatalf("check auth: %v", err)
	}
	if result.CampaignGroupID != "org-1" || result.ProductPageCount != 2 {
		t.Fatalf("unexpected check-auth result: %#v", result)
	}
	if api.checkCalls != 1 {
		t.Fatalf("expected one check-auth API call, got %d", api.checkCalls)
	}
}

func TestPlanPropagatesFactoryError(t *testing.T) {
	loaded := testSpec(t)

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return nil, errors.New("factory failed") }),
	)

	_, err := engine.Plan(context.Background(), loaded, Options{})
	if err == nil || !strings.Contains(err.Error(), "factory failed") {
		t.Fatalf("expected factory error, got %v", err)
	}
}

func TestPlanFailsWhenCampaignCreateNeedsCurrency(t *testing.T) {
	loaded := testSpec(t)
	loaded.Defaults.Currency = ""

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return &fakeAPI{}, nil }),
	)

	_, err := engine.Plan(context.Background(), loaded, Options{})
	if err == nil || !strings.Contains(err.Error(), "defaults.currency") {
		t.Fatalf("expected defaults.currency error, got %v", err)
	}
}

func TestApplyFailsWhenCampaignCreateNeedsCurrency(t *testing.T) {
	loaded := testSpec(t)
	loaded.Defaults.Currency = ""

	engine := NewEngine(slog.New(slog.NewTextHandler(io.Discard, nil)))
	planned := Result{Plan: diff.Plan{Actions: []diff.Action{{Operation: diff.OperationCreate, Kind: diff.ResourceCampaign}}}}

	_, err := engine.Apply(context.Background(), loaded, planned, Options{})
	if err == nil || !strings.Contains(err.Error(), "defaults.currency") {
		t.Fatalf("expected defaults.currency error, got %v", err)
	}
}

func TestApplyFailsWhenBidBearingPlanNeedsCurrency(t *testing.T) {
	loaded := testSpec(t)
	loaded.Defaults.Currency = ""

	testCases := []diff.Action{
		{Operation: diff.OperationUpdate, Kind: diff.ResourceCampaign, Changes: []diff.FieldChange{{Field: "daily_budget"}}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceAdGroup},
		{Operation: diff.OperationPause, Kind: diff.ResourceAdGroup},
		{Operation: diff.OperationCreate, Kind: diff.ResourceKeyword},
		{Operation: diff.OperationActivate, Kind: diff.ResourceKeyword},
	}

	engine := NewEngine(slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, action := range testCases {
		planned := Result{Plan: diff.Plan{Actions: []diff.Action{action}}}
		_, err := engine.Apply(context.Background(), loaded, planned, Options{})
		if err == nil || !strings.Contains(err.Error(), "defaults.currency") {
			t.Fatalf("expected defaults.currency error for %+v, got %v", action, err)
		}
	}
}

func TestApplyPropagatesFactoryError(t *testing.T) {
	loaded := testSpec(t)
	loaded.Defaults.Currency = "EUR"

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return nil, errors.New("factory failed") }),
	)
	planned := Result{Plan: diff.Plan{Actions: []diff.Action{{Operation: diff.OperationCreate, Kind: diff.ResourceCampaign}}}}

	_, err := engine.Apply(context.Background(), loaded, planned, Options{})
	if err == nil || !strings.Contains(err.Error(), "factory failed") {
		t.Fatalf("expected factory error, got %v", err)
	}
}

func TestCheckAuthPropagatesFactoryError(t *testing.T) {
	loaded := testSpec(t)

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return nil, errors.New("factory failed") }),
	)

	_, err := engine.CheckAuth(context.Background(), loaded, "")
	if err == nil || !strings.Contains(err.Error(), "factory failed") {
		t.Fatalf("expected factory error, got %v", err)
	}
}

func TestCheckAuthFailsValidationBeforeCallingAPI(t *testing.T) {
	loaded := testSpec(t)
	loaded.Campaigns[0].AdGroups[0].Targeting = spec.TargetingSearchMatch

	api := &fakeAPI{}
	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		withAPIFactory(func(spec.Spec, auth.Config) (adsAPI, error) { return api, nil }),
	)

	_, err := engine.CheckAuth(context.Background(), loaded, "")
	if err == nil || !strings.Contains(err.Error(), "must define zero keywords") {
		t.Fatalf("expected validation error, got %v", err)
	}
	if api.checkCalls != 0 {
		t.Fatalf("expected no check-auth API calls, got %d", api.checkCalls)
	}
}

func TestDefaultAPIFactoryUsesInjectedHTTPClientAndEndpoints(t *testing.T) {
	const (
		orgID          = "20744842"
		adamID         = "123456"
		transportValue = "shared"
	)

	var tokenCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected token method %s", r.Method)
		}
		if r.URL.Path != "/oauth/token" {
			t.Fatalf("unexpected token path %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Test-Transport"); got != transportValue {
			t.Fatalf("expected injected transport header on token request, got %q", got)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse token form: %v", err)
		}
		if got := r.Form.Get("client_id"); got != "client-id" {
			t.Fatalf("unexpected client_id %q", got)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("unexpected grant_type %q", got)
		}
		if got := r.Form.Get("scope"); got != "searchadsorg" {
			t.Fatalf("unexpected scope %q", got)
		}
		if err := json.NewEncoder(w).Encode(map[string]any{"access_token": "token-1", "expires_in": 3600}); err != nil {
			t.Fatalf("encode token response: %v", err)
		}
	}))
	defer tokenServer.Close()

	var aclCalls atomic.Int32
	var productPageCalls atomic.Int32
	var campaignCalls atomic.Int32
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		if got := r.Header.Get("X-Test-Transport"); got != transportValue {
			t.Fatalf("expected injected transport header on API request, got %q", got)
		}

		switch r.URL.Path {
		case "/acls":
			aclCalls.Add(1)
			if got := r.Header.Get("X-AP-Context"); got != "" {
				t.Fatalf("expected no org context header for /acls, got %q", got)
			}
			if got := r.Header.Get("X-AP-Org-Id"); got != "" {
				t.Fatalf("expected no X-AP-Org-Id header for /acls, got %q", got)
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"orgId":     orgID,
					"orgName":   "Readcap",
					"roleNames": []string{"API Manager"},
				}},
			}); err != nil {
				t.Fatalf("encode ACL response: %v", err)
			}
		case "/apps/" + adamID + "/product-pages":
			productPageCalls.Add(1)
			assertInjectedOrgHeaders(t, r, orgID)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"data": []any{},
				"pagination": map[string]any{
					"totalResults": 0,
					"startIndex":   0,
					"itemsPerPage": 1000,
				},
			}); err != nil {
				t.Fatalf("encode product pages response: %v", err)
			}
		case "/campaigns":
			campaignCalls.Add(1)
			assertInjectedOrgHeaders(t, r, orgID)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"data": []any{},
				"pagination": map[string]any{
					"totalResults": 0,
					"startIndex":   0,
					"itemsPerPage": 1000,
				},
			}); err != nil {
				t.Fatalf("encode campaigns response: %v", err)
			}
		default:
			t.Fatalf("unexpected API request %s %s", r.Method, r.URL.String())
		}
	}))
	defer apiServer.Close()

	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected default transport type %T", http.DefaultTransport)
	}
	httpClient := &http.Client{
		Transport: headerTransport{
			base:        baseTransport.Clone(),
			headerName:  "X-Test-Transport",
			headerValue: transportValue,
		},
	}

	engine := NewEngine(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithHTTPClient(httpClient),
		WithTokenURL(tokenServer.URL+"/oauth/token"),
		WithAPIBaseURL(apiServer.URL),
	)

	api, err := engine.defaultAPIFactory(spec.Spec{
		CampaignGroup: spec.CampaignGroup{ID: orgID},
	}, auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: pkcs8PrivateKeyPEM(t),
	})
	if err != nil {
		t.Fatalf("default API factory: %v", err)
	}

	result, err := api.CheckAuth(context.Background(), spec.Spec{
		CampaignGroup: spec.CampaignGroup{ID: orgID},
		App:           spec.App{AppID: adamID},
	})
	if err != nil {
		t.Fatalf("check auth through default factory: %v", err)
	}
	if result.CampaignGroupID != orgID {
		t.Fatalf("expected campaign group id %q, got %q", orgID, result.CampaignGroupID)
	}
	if result.OrgName != "Readcap" {
		t.Fatalf("expected org name Readcap, got %q", result.OrgName)
	}
	if result.AppID != adamID {
		t.Fatalf("expected app id %q, got %q", adamID, result.AppID)
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("expected one token request, got %d", tokenCalls.Load())
	}
	if aclCalls.Load() != 1 || productPageCalls.Load() != 1 || campaignCalls.Load() != 1 {
		t.Fatalf("unexpected API request counts: acls=%d product_pages=%d campaigns=%d", aclCalls.Load(), productPageCalls.Load(), campaignCalls.Load())
	}
}

func testSpec(t *testing.T) spec.Spec {
	t.Helper()
	privateKeyPath := filepath.Join(t.TempDir(), "appleads-private.pem")
	if err := os.WriteFile(privateKeyPath, []byte("-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n"), 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "`+filepath.ToSlash(privateKeyPath)+`"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("ASACTL_CONFIG", configPath)
	return spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: "20744842"},
		Auth:          spec.Auth{Profile: "default"},
		App:           spec.App{Name: "Readcap", AppID: "123456"},
		Defaults:      spec.Defaults{Currency: "EUR", Devices: []spec.Device{spec.DeviceIPhone}},
		Campaigns: []spec.Campaign{{
			Name:        "US - Brand - Exact",
			Storefronts: []string{"US"},
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusActive,
			AdGroups: []spec.AdGroup{{
				Name:          "Brand",
				Status:        spec.StatusActive,
				DefaultCPTBid: mustDecimal(t, "0.90"),
				Targeting:     spec.TargetingKeywords,
				Keywords: []spec.Keyword{{
					Text:      "readcap",
					MatchType: spec.MatchTypeExact,
					Bid:       mustDecimal(t, "1.10"),
					Status:    spec.StatusActive,
				}},
			}},
		}},
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

func assertInjectedOrgHeaders(t *testing.T, request *http.Request, orgID string) {
	t.Helper()
	if got := request.Header.Get("X-AP-Context"); got != "orgId="+orgID {
		t.Fatalf("unexpected X-AP-Context header %q", got)
	}
	if got := request.Header.Get("X-AP-Org-Id"); got != orgID {
		t.Fatalf("unexpected X-AP-Org-Id header %q", got)
	}
}

func pkcs8PrivateKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

type headerTransport struct {
	base        http.RoundTripper
	headerName  string
	headerValue string
}

func (transport headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set(transport.headerName, transport.headerValue)
	return transport.base.RoundTrip(clone)
}
