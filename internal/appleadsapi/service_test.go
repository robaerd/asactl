package appleadsapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/auth"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
)

func TestServiceFetchStateResolvesProductPageViaCreative(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":            3663305,
					"adamId":        1613230582,
					"name":          "Reading Tracker",
					"productPageId": "1982a269-0f70-4480-85e8-acfc33681c94",
					"type":          "CUSTOM_PRODUCT_PAGE",
					"state":         "VALID",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"adamId":             1613230582,
					"name":               "US - Brand - Exact",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.50", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                     11,
					"name":                   "Brand",
					"defaultBidAmount":       map[string]any{"amount": "0.90", "currency": "EUR"},
					"automatedKeywordsOptIn": false,
					"status":                 "ACTIVE",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/adgroups/11/targetingkeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups/11/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups/11/ads":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":           2143634301,
					"creativeId":   3663305,
					"creativeType": "CUSTOM_PRODUCT_PAGE",
					"status":       "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	state, err := service.FetchState(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("fetch state: %v", err)
	}
	if len(state.State.CustomAds) != 1 {
		t.Fatalf("expected one custom ad, got %+v", state.State.CustomAds)
	}
	if state.Scope.ManagedCampaignCount != 1 {
		t.Fatalf("expected one managed campaign, got %+v", state.Scope)
	}
	if state.State.CustomAds[0].ProductPage != "CPP1" {
		t.Fatalf("expected CPP1, got %+v", state.State.CustomAds[0])
	}
}

func TestServiceFetchStatePreservesOrgCampaignsForWipeOrg(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": 1, "adamId": 1613230582, "name": "US - Brand - Exact", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.50", "currency": "EUR"}, "status": "ENABLED"},
					{"id": 2, "adamId": 999999, "name": "Other App Campaign", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.00", "currency": "EUR"}, "status": "ENABLED"},
				},
				"pagination": map[string]any{"itemsPerPage": 2, "startIndex": 0, "totalResults": 2},
			})
		case "/campaigns/1/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	result, err := service.FetchState(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("fetch state: %v", err)
	}
	if result.Scope.ManagedCampaignCount != 1 || result.Scope.OtherAppCampaignCount != 1 {
		t.Fatalf("unexpected scope summary: %+v", result.Scope)
	}
	if len(result.State.Campaigns) != 1 || result.State.Campaigns[0].Name != "US - Brand - Exact" {
		t.Fatalf("expected managed state campaigns only, got %+v", result.State.Campaigns)
	}
	if len(result.OrgCampaigns) != 2 {
		t.Fatalf("expected both org campaigns, got %+v", result.OrgCampaigns)
	}
	if result.OrgCampaigns[0].Name != "US - Brand - Exact" || result.OrgCampaigns[1].Name != "Other App Campaign" {
		t.Fatalf("unexpected org campaigns: %+v", result.OrgCampaigns)
	}
}

func TestServiceFetchStateContinuesPagingWhenPageIsShortButTotalResultsIndicatesMore(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	var campaignOffsets []string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns":
			campaignOffsets = append(campaignOffsets, r.URL.Query().Get("offset"))
			switch r.URL.Query().Get("offset") {
			case "0":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{"id": 1, "adamId": 1613230582, "name": "US - Brand - Exact", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.50", "currency": "EUR"}, "status": "ENABLED"},
					},
					"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 2},
				})
			case "1":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{
						{"id": 2, "adamId": 999999, "name": "Other App Campaign", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.00", "currency": "EUR"}, "status": "ENABLED"},
					},
					"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 1, "totalResults": 2},
				})
			default:
				t.Fatalf("unexpected campaigns offset %q", r.URL.Query().Get("offset"))
			}
		case "/campaigns/1/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	result, err := service.FetchState(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("fetch state: %v", err)
	}
	if !slices.Equal(campaignOffsets, []string{"0", "1"}) {
		t.Fatalf("expected campaign paging across offsets 0 and 1, got %v", campaignOffsets)
	}
	if result.Scope.ManagedCampaignCount != 1 || result.Scope.OtherAppCampaignCount != 1 {
		t.Fatalf("unexpected scope summary: %+v", result.Scope)
	}
	if len(result.OrgCampaigns) != 2 {
		t.Fatalf("expected both campaigns from paged fetch, got %+v", result.OrgCampaigns)
	}
}

func TestServiceFetchStateIgnoresMalformedForeignCampaignBudget(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": 1, "adamId": 1613230582, "name": "US - Brand - Exact", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.50", "currency": "EUR"}, "status": "ENABLED"},
					{"id": 2, "adamId": 999999, "name": "Other App Campaign", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "not-a-decimal", "currency": "EUR"}, "status": "ENABLED"},
				},
				"pagination": map[string]any{"itemsPerPage": 2, "startIndex": 0, "totalResults": 2},
			})
		case "/campaigns/1/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	result, err := service.FetchState(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("fetch state: %v", err)
	}
	if result.Scope.ManagedCampaignCount != 1 || result.Scope.OtherAppCampaignCount != 1 {
		t.Fatalf("unexpected scope summary: %+v", result.Scope)
	}
	if len(result.State.Campaigns) != 1 || result.State.Campaigns[0].Name != "US - Brand - Exact" {
		t.Fatalf("expected managed state campaigns only, got %+v", result.State.Campaigns)
	}
	if len(result.OrgCampaigns) != 2 {
		t.Fatalf("expected both org campaigns, got %+v", result.OrgCampaigns)
	}
}

func TestServiceCheckAuthReturnsScopeAndProductPages(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/acls":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"orgId":       "org",
					"orgName":     "Readcap - EN",
					"displayName": "Readcap - EN",
					"currency":    "EUR",
					"timeZone":    "UTC",
					"roleNames":   []string{"API Campaign Manager"},
				}},
				"error":      nil,
				"pagination": nil,
			})
		case "/apps/1613230582/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": "1982a269-0f70-4480-85e8-acfc33681c94", "adamId": 1613230582, "name": "Reading Tracker", "state": "AVAILABLE"},
					{"id": "47d01d42-2976-4709-aea0-1a7b73aff67d", "adamId": 1613230582, "name": "Capture & Organize", "state": "AVAILABLE"},
				},
			})
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{"id": 1, "adamId": 1613230582, "name": "US - Brand - Exact", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.50", "currency": "EUR"}, "status": "ENABLED"},
					{"id": 2, "adamId": 999999, "name": "Other App Campaign", "countriesOrRegions": []string{"US"}, "dailyBudgetAmount": map[string]any{"amount": "1.00", "currency": "EUR"}, "status": "ENABLED"},
				},
				"pagination": map[string]any{"itemsPerPage": 2, "startIndex": 0, "totalResults": 2},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	result, err := service.CheckAuth(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("check auth: %v", err)
	}
	if result.CampaignGroupID != "org" || result.OrgName != "Readcap - EN" {
		t.Fatalf("unexpected org result: %+v", result)
	}
	if result.ProductPageCount != 2 || len(result.ProductPages) != 2 {
		t.Fatalf("expected two product pages, got %+v", result.ProductPages)
	}
	if result.Scope.ManagedCampaignCount != 1 || result.Scope.OtherAppCampaignCount != 1 {
		t.Fatalf("unexpected scope summary: %+v", result.Scope)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", result.Warnings)
	}
}

func TestServiceCheckAuthFailsWhenConfiguredOrgNotAccessible(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/acls":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"orgId":       "different-org",
					"orgName":     "Other Org",
					"displayName": "Other Org",
					"currency":    "EUR",
					"timeZone":    "UTC",
					"roleNames":   []string{"API Campaign Manager"},
				}},
				"error":      nil,
				"pagination": nil,
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	_, err := service.CheckAuth(context.Background(), testSpec())
	if err == nil || !strings.Contains(err.Error(), "configured campaign_group.id") {
		t.Fatalf("expected inaccessible org error, got %v", err)
	}
}

func TestApplyPlanUsesNestedAppleAdsContracts(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns":
			if r.Method == http.MethodGet {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data": []map[string]any{{
						"id":                 1,
						"name":               "Existing Campaign",
						"countriesOrRegions": []string{"US"},
						"dailyBudgetAmount":  map[string]any{"amount": "1.00", "currency": "EUR"},
						"status":             "ENABLED",
					}},
					"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
				})
				return
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["name"], "US - Brand - Exact")
			assertEqual(t, payload["billingEvent"], "TAPS")
			assertEqual(t, payload["adChannelType"], "SEARCH")
			assertEqual(t, payload["orgId"], "org")
			assertRecentUTCStartTime(t, payload["startTime"])
			budget := payload["dailyBudgetAmount"].(map[string]any)
			assertEqual(t, budget["currency"], "EUR")
			assertEqual(t, budget["amount"], "1.50")
			assertEqual(t, payload["adamId"], float64(1613230582))
			assertEqual(t, payload["status"], "ENABLED")
			assertEqual(t, payload["supplySources"].([]any)[0], "APPSTORE_SEARCH_RESULTS")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 300}})
		case "/campaigns/300/adgroups":
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["campaignId"], "300")
			assertEqual(t, payload["name"], "Brand")
			assertRecentUTCStartTime(t, payload["startTime"])
			assertEqual(t, payload["pricingModel"], "CPC")
			assertEqual(t, payload["status"], "ENABLED")
			assertEqual(t, payload["automatedKeywordsOptIn"], false)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 400}})
		case "/campaigns/300/adgroups/400/targetingkeywords/bulk":
			var payload []map[string]any
			decodeBody(t, r, &payload)
			if len(payload) != 1 {
				t.Fatalf("expected 1 keyword payload item, got %d", len(payload))
			}
			assertEqual(t, payload[0]["text"], "readcap")
			assertEqual(t, payload[0]["matchType"], "EXACT")
			assertEqual(t, payload[0]["status"], "ACTIVE")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "500", "success": true}}})
		case "/campaigns/300/negativekeywords/bulk":
			var payload []map[string]any
			decodeBody(t, r, &payload)
			if len(payload) != 1 {
				t.Fatalf("expected 1 negative payload item, got %d", len(payload))
			}
			assertEqual(t, payload[0]["text"], "free books")
			assertEqual(t, payload[0]["matchType"], "EXACT")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "n1", "success": true}}})
		case "/creatives":
			if r.Method != http.MethodGet {
				t.Fatalf("unexpected creative method %s", r.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":            3663305,
					"adamId":        1613230582,
					"name":          "Reading Tracker",
					"productPageId": "1982a269-0f70-4480-85e8-acfc33681c94",
					"type":          "CUSTOM_PRODUCT_PAGE",
					"state":         "VALID",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/apps/1613230582/product-pages":
			if r.Method != http.MethodGet {
				t.Fatalf("unexpected product page method %s", r.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":     "1982a269-0f70-4480-85e8-acfc33681c94",
					"adamId": 1613230582,
					"name":   "Reading Tracker",
					"state":  "AVAILABLE",
				}},
			})
		case "/campaigns/300/adgroups/400/ads":
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["creativeId"], "3663305")
			assertEqual(t, payload["name"], "cpp1")
			assertEqual(t, payload["status"], "ENABLED")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 600}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationCreate, Kind: diff.ResourceCampaign, Key: "campaign", Desired: diff.Campaign{Name: "US - Brand - Exact", DailyBudget: mustDecimal(t, "1.50"), Storefronts: []string{"US"}, Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceAdGroup, Key: "adgroup", Desired: diff.AdGroup{CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceKeyword, Key: "keyword", Desired: diff.Keyword{CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "readcap", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "1.10"), Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceNegativeKeyword, Key: "negative", Desired: diff.NegativeKeyword{Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "free books", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceCustomAd, Key: "custom", Desired: diff.CustomAd{CampaignName: "US - Brand - Exact", AdGroupName: "Brand", ProductPage: "CPP1", Status: spec.StatusActive}},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}

	expected := []string{
		"POST /campaigns",
		"POST /campaigns/300/adgroups",
		"POST /campaigns/300/adgroups/400/targetingkeywords/bulk",
		"POST /campaigns/300/negativekeywords/bulk",
		"GET /apps/1613230582/product-pages",
		"GET /creatives",
		"POST /campaigns/300/adgroups/400/ads",
	}
	if !slices.Equal(requests, expected) {
		t.Fatalf("unexpected request order: got %v want %v", requests, expected)
	}
}

func TestApplyPlanPausesCustomAdBeforeCreatingReplacement(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns/1/adgroups/2/ads/10":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["status"], "PAUSED")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 10}})
		case "/apps/1613230582/product-pages":
			if r.Method != http.MethodGet {
				t.Fatalf("unexpected method %s", r.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":     "47d01d42-2976-4709-aea0-1a7b73aff67d",
					"adamId": 1613230582,
					"name":   "Capture & Organize",
					"state":  "AVAILABLE",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/creatives":
			if r.Method != http.MethodGet {
				t.Fatalf("unexpected method %s", r.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":            3663306,
					"adamId":        1613230582,
					"name":          "Capture & Organize",
					"productPageId": "47d01d42-2976-4709-aea0-1a7b73aff67d",
					"type":          "CUSTOM_PRODUCT_PAGE",
					"state":         "VALID",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/adgroups/2/ads":
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["creativeId"], "3663306")
			assertEqual(t, payload["name"], "cpp2")
			assertEqual(t, payload["status"], "ENABLED")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 600}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{
			Operation: diff.OperationNoop,
			Kind:      diff.ResourceCampaign,
			Key:       "campaign",
			Current: diff.Campaign{
				ID:          "1",
				Name:        "US - Brand - Exact",
				Storefronts: []string{"US"},
				DailyBudget: mustDecimal(t, "1.50"),
				Status:      spec.StatusActive,
			},
			Desired: diff.Campaign{
				ID:          "1",
				Name:        "US - Brand - Exact",
				Storefronts: []string{"US"},
				DailyBudget: mustDecimal(t, "1.50"),
				Status:      spec.StatusActive,
			},
		},
		{
			Operation: diff.OperationNoop,
			Kind:      diff.ResourceAdGroup,
			Key:       "adgroup",
			Current: diff.AdGroup{
				ID:            "2",
				CampaignName:  "US - Brand - Exact",
				Name:          "Brand",
				DefaultCPTBid: mustDecimal(t, "0.90"),
				ProductPage:   "CPP1",
				Targeting:     spec.TargetingKeywords,
				Status:        spec.StatusActive,
			},
			Desired: diff.AdGroup{
				ID:            "2",
				CampaignName:  "US - Brand - Exact",
				Name:          "Brand",
				DefaultCPTBid: mustDecimal(t, "0.90"),
				ProductPage:   "CPP2",
				Targeting:     spec.TargetingKeywords,
				Status:        spec.StatusActive,
			},
		},
		{
			Operation: diff.OperationDelete,
			Kind:      diff.ResourceCustomAd,
			Key:       "custom-delete",
			Current: diff.CustomAd{
				ID:           "10",
				CampaignName: "US - Brand - Exact",
				AdGroupName:  "Brand",
				ProductPage:  "CPP1",
				Status:       spec.StatusActive,
				IsDefault:    false,
			},
		},
		{
			Operation: diff.OperationCreate,
			Kind:      diff.ResourceCustomAd,
			Key:       "custom-create",
			Desired: diff.CustomAd{
				CampaignName: "US - Brand - Exact",
				AdGroupName:  "Brand",
				ProductPage:  "CPP2",
				Status:       spec.StatusActive,
				IsDefault:    false,
			},
		},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}

	expected := []string{
		"PUT /campaigns/1/adgroups/2/ads/10",
		"GET /apps/1613230582/product-pages",
		"GET /creatives",
		"POST /campaigns/1/adgroups/2/ads",
	}
	if !slices.Equal(requests, expected) {
		t.Fatalf("unexpected request order: got %v want %v", requests, expected)
	}
}

func TestServiceFetchStateSkipsPausedUnmanagedCustomAds(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{
						"id":            3663305,
						"adamId":        1613230582,
						"name":          "Reading Tracker",
						"productPageId": "1982a269-0f70-4480-85e8-acfc33681c94",
						"type":          "CUSTOM_PRODUCT_PAGE",
						"state":         "VALID",
					},
					{
						"id":            3663306,
						"adamId":        1613230582,
						"name":          "Capture & Organize",
						"productPageId": "47d01d42-2976-4709-aea0-1a7b73aff67d",
						"type":          "CUSTOM_PRODUCT_PAGE",
						"state":         "VALID",
					},
				},
				"pagination": map[string]any{"itemsPerPage": 2, "startIndex": 0, "totalResults": 2},
			})
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"adamId":             1613230582,
					"name":               "US - Brand - Exact",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.50", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                     11,
					"name":                   "Brand",
					"defaultBidAmount":       map[string]any{"amount": "0.90", "currency": "EUR"},
					"automatedKeywordsOptIn": false,
					"status":                 "ACTIVE",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/adgroups/11/targetingkeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups/11/negativekeywords":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}, "pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0}})
		case "/campaigns/1/adgroups/11/ads":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{
					{
						"id":           2143634301,
						"creativeId":   3663305,
						"creativeType": "CUSTOM_PRODUCT_PAGE",
						"status":       "PAUSED",
					},
					{
						"id":           2143634302,
						"creativeId":   3663306,
						"creativeType": "CUSTOM_PRODUCT_PAGE",
						"status":       "ENABLED",
					},
				},
				"pagination": map[string]any{"itemsPerPage": 2, "startIndex": 0, "totalResults": 2},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	desired := testSpec()
	desired.Campaigns = []spec.Campaign{{
		Name:        "US - Brand - Exact",
		Storefronts: []string{"US"},
		DailyBudget: mustDecimal(t, "1.50"),
		Status:      spec.StatusActive,
		AdGroups: []spec.AdGroup{{
			Name:          "Brand",
			Status:        spec.StatusActive,
			DefaultCPTBid: mustDecimal(t, "0.90"),
			ProductPage:   "CPP2",
			Targeting:     spec.TargetingKeywords,
		}},
	}}

	state, err := service.FetchState(context.Background(), desired)
	if err != nil {
		t.Fatalf("fetch state: %v", err)
	}
	if len(state.State.CustomAds) != 1 {
		t.Fatalf("expected one managed custom ad after skipping paused unmanaged ads, got %+v", state.State.CustomAds)
	}
	if state.State.CustomAds[0].ProductPage != "CPP2" {
		t.Fatalf("expected only desired CPP2 custom ad, got %+v", state.State.CustomAds[0])
	}
}

func TestApplyPlanPartitionsNegativeKeywordsByScopeAndParent(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	paths := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"name":               "US - Brand - Exact",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.00", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/negativekeywords/bulk", "/campaigns/1/adgroups/2/negativekeywords/bulk":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "n1", "success": true}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}, Desired: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceNegativeKeyword, Key: "campaign-negative", Desired: diff.NegativeKeyword{Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "free books", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceNegativeKeyword, Key: "adgroup-negative", Desired: diff.NegativeKeyword{Scope: diff.ScopeAdGroup, CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "library", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}

	if !slices.Contains(paths, "POST /campaigns/1/negativekeywords/bulk") || !slices.Contains(paths, "POST /campaigns/1/adgroups/2/negativekeywords/bulk") {
		t.Fatalf("expected split negative keyword paths, got %v", paths)
	}
}

func TestApplyPlanIncludesCurrencyInMoneyUpdates(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns/1/adgroups/2":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			defaultBid := payload["defaultBidAmount"].(map[string]any)
			assertEqual(t, defaultBid["amount"], "1.26")
			assertEqual(t, defaultBid["currency"], "EUR")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 2}})
		case "/campaigns/1/adgroups/2/targetingkeywords/bulk":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload []map[string]any
			decodeBody(t, r, &payload)
			if len(payload) != 1 {
				t.Fatalf("expected one keyword update, got %d", len(payload))
			}
			bid := payload[0]["bidAmount"].(map[string]any)
			assertEqual(t, bid["amount"], "1.26")
			assertEqual(t, bid["currency"], "EUR")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "5", "success": true}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup-noop", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}, Desired: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}},
		{Operation: diff.OperationUpdate, Kind: diff.ResourceAdGroup, Key: "adgroup-update", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}, Desired: diff.AdGroup{CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "1.26"), Status: spec.StatusActive}},
		{Operation: diff.OperationUpdate, Kind: diff.ResourceKeyword, Key: "keyword-update", Current: diff.Keyword{ID: "5", CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "readcap", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "0.90"), Status: spec.StatusActive}, Desired: diff.Keyword{CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "readcap", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "1.26"), Status: spec.StatusActive}},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}

	expected := []string{
		"PUT /campaigns/1/adgroups/2",
		"PUT /campaigns/1/adgroups/2/targetingkeywords/bulk",
	}
	if !slices.Equal(requests, expected) {
		t.Fatalf("unexpected request order: got %v want %v", requests, expected)
	}
}

func TestApplyPlanCampaignActivateUsesUpdatePath(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns/1":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			if _, ok := payload["name"]; ok {
				t.Fatalf("did not expect name in campaign update payload: %#v", payload)
			}
			if _, ok := payload["countriesOrRegions"]; ok {
				t.Fatalf("did not expect countriesOrRegions in campaign update payload: %#v", payload)
			}
			if _, ok := payload["dailyBudgetAmount"]; ok {
				t.Fatalf("did not expect dailyBudgetAmount in campaign update payload: %#v", payload)
			}
			campaignPayload, ok := payload["campaign"].(map[string]any)
			if !ok {
				t.Fatalf("expected nested campaign payload, got %#v", payload)
			}
			assertEqual(t, campaignPayload["status"], "ENABLED")
			if _, ok := campaignPayload["name"]; ok {
				t.Fatalf("did not expect campaign.name in campaign update payload: %#v", payload)
			}
			if _, ok := campaignPayload["countriesOrRegions"]; ok {
				t.Fatalf("did not expect campaign.countriesOrRegions in campaign update payload: %#v", payload)
			}
			if _, ok := campaignPayload["dailyBudgetAmount"]; ok {
				t.Fatalf("did not expect campaign.dailyBudgetAmount in campaign update payload: %#v", payload)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 1}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{{
		Operation: diff.OperationActivate,
		Kind:      diff.ResourceCampaign,
		Key:       "campaign-activate",
		Current: diff.Campaign{
			ID:          "1",
			Name:        "US - Brand - Exact",
			Storefronts: []string{"US"},
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusPaused,
		},
		Desired: diff.Campaign{
			Name:        "US - Brand - Exact",
			Storefronts: []string{"US"},
			DailyBudget: mustDecimal(t, "1.50"),
			Status:      spec.StatusActive,
		},
	}}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}
}

func TestApplyPlanAdGroupActivateUsesUpdatePath(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns/1/adgroups/2":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["status"], "ENABLED")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 2}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{
			Operation: diff.OperationNoop,
			Kind:      diff.ResourceCampaign,
			Key:       "campaign",
			Current:   diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive},
			Desired:   diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive},
		},
		{
			Operation: diff.OperationActivate,
			Kind:      diff.ResourceAdGroup,
			Key:       "adgroup-activate",
			Current: diff.AdGroup{
				ID:            "2",
				CampaignName:  "US - Brand - Exact",
				Name:          "Brand",
				DefaultCPTBid: mustDecimal(t, "0.90"),
				Status:        spec.StatusPaused,
			},
			Desired: diff.AdGroup{
				CampaignName:  "US - Brand - Exact",
				Name:          "Brand",
				DefaultCPTBid: mustDecimal(t, "0.90"),
				Status:        spec.StatusActive,
			},
		},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}
}

func TestApplyPlanUsesKeywordStatusForKeywordAndNegativePayloads(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns/1/adgroups/2/targetingkeywords/bulk":
			var payload []map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload[0]["status"], "ACTIVE")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "5", "success": true}}})
		case "/campaigns/1/negativekeywords/bulk":
			var payload []map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload[0]["status"], "ACTIVE")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "1", "success": true}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}, Desired: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}},
		{Operation: diff.OperationUpdate, Kind: diff.ResourceKeyword, Key: "keyword-update", Current: diff.Keyword{ID: "5", CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "readcap", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "0.90"), Status: spec.StatusActive}, Desired: diff.Keyword{CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "readcap", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "1.26"), Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceNegativeKeyword, Key: "negative-create", Desired: diff.NegativeKeyword{Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "free books", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}
}

func TestApplyPlanNegativeKeywordUpdateOmitsImmutableFields(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns/1/negativekeywords/bulk":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload []map[string]any
			decodeBody(t, r, &payload)
			if len(payload) != 1 {
				t.Fatalf("expected one negative keyword update, got %d", len(payload))
			}
			assertEqual(t, payload[0]["id"], "5")
			assertEqual(t, payload[0]["status"], "ACTIVE")
			if _, ok := payload[0]["text"]; ok {
				t.Fatalf("expected update payload to omit immutable text, got %#v", payload[0])
			}
			if _, ok := payload[0]["matchType"]; ok {
				t.Fatalf("expected update payload to omit immutable matchType, got %#v", payload[0])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "5", "success": true}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationUpdate, Kind: diff.ResourceNegativeKeyword, Key: "negative-update", Current: diff.NegativeKeyword{ID: "5", Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "free books", MatchType: spec.MatchTypeExact, Status: spec.StatusPaused}, Desired: diff.NegativeKeyword{Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "free books", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}
}

func TestApplyPlanValidatesConfiguredProductPageAgainstLivePagesBeforeCreativeLookup(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/apps/1613230582/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":     "different-product-page",
					"adamId": 1613230582,
					"name":   "Other Product Page",
					"state":  "AVAILABLE",
				}},
			})
		case "/creatives":
			t.Fatal("unexpected creatives lookup before product page validation")
		case "/campaigns/1/adgroups/2/ads":
			t.Fatal("unexpected ad creation before product page validation")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}, Desired: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceCustomAd, Key: "custom-ad", Desired: diff.CustomAd{CampaignName: "US - Brand - Exact", AdGroupName: "Brand", ProductPage: "CPP1", Status: spec.StatusActive}},
	}}

	err := service.ApplyPlan(context.Background(), testSpec(), plan)
	if err == nil || !strings.Contains(err.Error(), `no Apple Ads product page found for product_page "CPP1"`) {
		t.Fatalf("expected product page validation error, got %v", err)
	}
	if !slices.Equal(requests, []string{"GET /apps/1613230582/product-pages"}) {
		t.Fatalf("unexpected requests: %v", requests)
	}
}

func TestApplyPlanDeletesStaleSubresources(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns/1/adgroups/2/targetingkeywords/5":
			if r.Method != http.MethodDelete {
				t.Fatalf("unexpected method %s", r.Method)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/campaigns/1/negativekeywords/delete/bulk":
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload []int64
			decodeBody(t, r, &payload)
			if len(payload) != 1 || payload[0] != 1 {
				t.Fatalf("unexpected campaign negative delete payload: %#v", payload)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": 1})
		case "/campaigns/1/adgroups/2/negativekeywords/delete/bulk":
			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload []int64
			decodeBody(t, r, &payload)
			if len(payload) != 1 || payload[0] != 2 {
				t.Fatalf("unexpected adgroup negative delete payload: %#v", payload)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": 1})
		case "/campaigns/1/adgroups/2/ads/ad1":
			if r.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", r.Method)
			}
			var payload map[string]any
			decodeBody(t, r, &payload)
			assertEqual(t, payload["status"], "PAUSED")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": "ad1"}})
		case "/campaigns/1/adgroups/2":
			if r.Method != http.MethodDelete {
				t.Fatalf("unexpected method %s", r.Method)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}, Desired: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}},
		{Operation: diff.OperationDelete, Kind: diff.ResourceKeyword, Key: "keyword", Current: diff.Keyword{ID: "5", CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "legacy", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
		{Operation: diff.OperationDelete, Kind: diff.ResourceNegativeKeyword, Key: "campaign-negative", Current: diff.NegativeKeyword{ID: "1", Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "legacy", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
		{Operation: diff.OperationDelete, Kind: diff.ResourceNegativeKeyword, Key: "adgroup-negative", Current: diff.NegativeKeyword{ID: "2", Scope: diff.ScopeAdGroup, CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "legacy", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
		{Operation: diff.OperationDelete, Kind: diff.ResourceCustomAd, Key: "custom-ad", Current: diff.CustomAd{ID: "ad1", CampaignName: "US - Brand - Exact", AdGroupName: "Brand", ProductPage: "CPP1", Status: spec.StatusActive}},
		{Operation: diff.OperationDelete, Kind: diff.ResourceAdGroup, Key: "adgroup-delete", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}},
	}}

	if err := service.ApplyPlan(context.Background(), testSpec(), plan); err != nil {
		t.Fatalf("apply plan: %v", err)
	}

	expected := []string{
		"DELETE /campaigns/1/adgroups/2/targetingkeywords/5",
		"POST /campaigns/1/adgroups/2/negativekeywords/delete/bulk",
		"POST /campaigns/1/negativekeywords/delete/bulk",
		"PUT /campaigns/1/adgroups/2/ads/ad1",
		"DELETE /campaigns/1/adgroups/2",
	}
	if !slices.Equal(requests, expected) {
		t.Fatalf("unexpected request order: got %v want %v", requests, expected)
	}
}

func TestApplyPlanFailsOnKeywordItemLevelError(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"name":               "US - Brand - Exact",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.00", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/adgroups/2/targetingkeywords/bulk":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "k1", "success": false, "error": "duplicate"}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup", Current: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}, Desired: diff.AdGroup{ID: "2", CampaignName: "US - Brand - Exact", Name: "Brand", Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceKeyword, Key: "keyword", Desired: diff.Keyword{CampaignName: "US - Brand - Exact", AdGroupName: "Brand", Text: "readcap", MatchType: spec.MatchTypeExact, Bid: mustDecimal(t, "1.10"), Status: spec.StatusActive}},
	}}

	err := service.ApplyPlan(context.Background(), testSpec(), plan)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected keyword item-level error, got %v", err)
	}
}

func TestApplyPlanFailsWhenCreateResponseMissingID(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{{
		Operation: diff.OperationCreate,
		Kind:      diff.ResourceCampaign,
		Key:       "campaign",
		Desired: diff.Campaign{
			Name:        "US - Brand - Exact",
			DailyBudget: mustDecimal(t, "1.50"),
			Storefronts: []string{"US"},
			Status:      spec.StatusActive,
		},
	}}}

	err := service.ApplyPlan(context.Background(), testSpec(), plan)
	if err == nil || !strings.Contains(err.Error(), "response missing resource id") {
		t.Fatalf("expected missing resource id error, got %v", err)
	}
}

func TestApplyPlanFailsOnMalformedNegativeKeywordBulkResponse(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"name":               "US - Brand - Exact",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.00", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		case "/campaigns/1/negativekeywords/bulk":
			_, _ = w.Write([]byte(`{"unexpected":true}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}, Desired: diff.Campaign{ID: "1", Name: "US - Brand - Exact", Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceNegativeKeyword, Key: "negative", Desired: diff.NegativeKeyword{Scope: diff.ScopeCampaign, CampaignName: "US - Brand - Exact", Text: "free books", MatchType: spec.MatchTypeExact, Status: spec.StatusActive}},
	}}

	err := service.ApplyPlan(context.Background(), testSpec(), plan)
	if err == nil || !strings.Contains(err.Error(), "bulk response missing data") {
		t.Fatalf("expected malformed bulk response error, got %v", err)
	}
}

func TestServiceApplyPlanReturnsErrorsForMalformedActions(t *testing.T) {
	input := testSpec()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"name":               "Existing Campaign",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.00", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()
	service := testService(t, tokenServer.URL, apiServer.URL)

	testCases := []struct {
		name    string
		plan    diff.Plan
		wantErr string
	}{
		{
			name:    "delete campaign wrong current type",
			plan:    diff.Plan{Actions: []diff.Action{{Operation: diff.OperationDelete, Kind: diff.ResourceCampaign, Key: "campaign", Current: "wrong"}}},
			wantErr: "expected current campaign",
		},
		{
			name:    "create campaign wrong desired type",
			plan:    diff.Plan{Actions: []diff.Action{{Operation: diff.OperationCreate, Kind: diff.ResourceCampaign, Key: "campaign", Desired: "wrong"}}},
			wantErr: "expected desired campaign",
		},
		{
			name:    "negative bulk wrong desired type",
			plan:    diff.Plan{Actions: []diff.Action{{Operation: diff.OperationCreate, Kind: diff.ResourceNegativeKeyword, Key: "negative", Desired: "wrong"}}},
			wantErr: "expected desired negative keyword",
		},
		{
			name:    "noop campaign wrong current type",
			plan:    diff.Plan{Actions: []diff.Action{{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: "wrong"}}},
			wantErr: "expected current campaign",
		},
		{
			name:    "noop campaign missing remote id",
			plan:    diff.Plan{Actions: []diff.Action{{Operation: diff.OperationNoop, Kind: diff.ResourceCampaign, Key: "campaign", Current: diff.Campaign{Name: "US - Brand - Exact"}}}},
			wantErr: "missing remote id",
		},
		{
			name:    "noop adgroup wrong current type",
			plan:    diff.Plan{Actions: []diff.Action{{Operation: diff.OperationNoop, Kind: diff.ResourceAdGroup, Key: "adgroup", Current: "wrong"}}},
			wantErr: "expected current adgroup",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := service.ApplyPlan(context.Background(), input, testCase.plan)
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantErr, err)
			}
		})
	}
}

func TestApplyPlanHonorsCancelledContextBeforeMutations(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Fatalf("unexpected request %s %s after context cancellation", r.Method, r.URL.Path)
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	plan := diff.Plan{Actions: []diff.Action{{
		Operation: diff.OperationCreate,
		Kind:      diff.ResourceCampaign,
		Key:       "campaign",
		Desired: diff.Campaign{
			Name:        "US - Brand - Exact",
			DailyBudget: mustDecimal(t, "1.50"),
			Storefronts: []string{"US"},
			Status:      spec.StatusActive,
		},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := service.ApplyPlan(ctx, testSpec(), plan)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", err)
	}
	if requests != 0 {
		t.Fatalf("expected no requests, got %d", requests)
	}
}

func TestApplyPlanStopsWhenContextCancelledBetweenPhases(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	firstCampaignCreated := make(chan struct{})
	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 300}})
			select {
			case <-firstCampaignCreated:
			default:
				close(firstCampaignCreated)
			}
		case "/campaigns/300/adgroups":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 400}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	service := testService(t, tokenServer.URL, apiServer.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-firstCampaignCreated
		cancel()
	}()

	plan := diff.Plan{Actions: []diff.Action{
		{Operation: diff.OperationCreate, Kind: diff.ResourceCampaign, Key: "campaign", Desired: diff.Campaign{Name: "US - Brand - Exact", DailyBudget: mustDecimal(t, "1.50"), Storefronts: []string{"US"}, Status: spec.StatusActive}},
		{Operation: diff.OperationCreate, Kind: diff.ResourceAdGroup, Key: "adgroup", Desired: diff.AdGroup{CampaignName: "US - Brand - Exact", Name: "Brand", DefaultCPTBid: mustDecimal(t, "0.90"), Status: spec.StatusActive}},
	}}

	err := service.ApplyPlan(ctx, testSpec(), plan)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if !slices.Equal(requests, []string{"POST /campaigns"}) {
		t.Fatalf("expected apply to stop before adgroup phase, got %v", requests)
	}
}

func testService(t *testing.T, tokenURL, apiBaseURL string) *appleadsapi.Service {
	t.Helper()
	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, nil, auth.WithTokenURL(tokenURL))
	client := appleadsapi.NewClient(
		provider,
		appleadsapi.WithOrgID("org"),
		appleadsapi.WithBaseURL(apiBaseURL),
	)
	return appleadsapi.NewService(client, appleadsapi.WithServiceLogger(nilLogger()))
}

func testSpec() spec.Spec {
	return spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: "org"},
		Auth:          spec.Auth{Profile: "default"},
		App:           spec.App{Name: "Readcap", AppID: "1613230582"},
		Defaults:      spec.Defaults{Currency: "EUR"},
		ProductPages: spec.ProductPageMap{
			"CPP1": {ProductPageID: "1982a269-0f70-4480-85e8-acfc33681c94", Name: "Reading Tracker", AppStoreURL: "https://apps.apple.com/us/app/readcap/id1613230582?ppid=1982a269-0f70-4480-85e8-acfc33681c94", Locale: "en-US"},
			"CPP2": {ProductPageID: "47d01d42-2976-4709-aea0-1a7b73aff67d", Name: "Capture & Organize", AppStoreURL: "https://apps.apple.com/us/app/readcap/id1613230582?ppid=47d01d42-2976-4709-aea0-1a7b73aff67d", Locale: "en-US"},
		},
	}
}

func decodeBody(t *testing.T, r *http.Request, out any) {
	t.Helper()
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
}

func assertRecentUTCStartTime(t *testing.T, value any) {
	t.Helper()
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		t.Fatalf("expected non-empty startTime string, got %#v", value)
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000", text)
	if err != nil {
		t.Fatalf("parse startTime %q: %v", text, err)
	}
	if parsed.Before(time.Now().UTC().Add(30 * time.Second)) {
		t.Fatalf("expected startTime to be in the near future, got %s", parsed.Format(time.RFC3339Nano))
	}
}

func assertEqual(t *testing.T, got, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v want %v", got, want)
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

func nilLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
