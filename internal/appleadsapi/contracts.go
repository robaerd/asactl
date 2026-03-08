package appleadsapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/robaerd/asactl/internal/spec"
)

const (
	apiStatusEnabled             = "ENABLED"
	apiStatusActive              = "ACTIVE"
	apiStatusPaused              = "PAUSED"
	apiBillingEventTaps          = "TAPS"
	apiSupplySourceSearchResults = "APPSTORE_SEARCH_RESULTS"
	apiAdChannelSearch           = "SEARCH"
	apiPricingModelCPC           = "CPC"
	apiCreativeTypeCPP           = "CUSTOM_PRODUCT_PAGE"
)

type moneyPayload struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency,omitempty"`
}

type createCampaignRequest struct {
	OrgID              string       `json:"orgId,omitempty"`
	Name               string       `json:"name"`
	StartTime          string       `json:"startTime,omitempty"`
	AdamID             int64        `json:"adamId"`
	BillingEvent       string       `json:"billingEvent"`
	DailyBudgetAmount  moneyPayload `json:"dailyBudgetAmount"`
	CountriesOrRegions []string     `json:"countriesOrRegions"`
	Status             string       `json:"status"`
	SupplySources      []string     `json:"supplySources"`
	AdChannelType      string       `json:"adChannelType"`
}

type campaignUpdatePayload struct {
	Name               string        `json:"name,omitempty"`
	DailyBudgetAmount  *moneyPayload `json:"dailyBudgetAmount,omitempty"`
	CountriesOrRegions []string      `json:"countriesOrRegions,omitempty"`
	Status             string        `json:"status,omitempty"`
}

type updateCampaignRequest struct {
	Campaign                                 *campaignUpdatePayload `json:"campaign,omitempty"`
	ClearGeoTargetingOnCountryOrRegionChange bool                   `json:"clearGeoTargetingOnCountryOrRegionChange,omitempty"`
}

type createAdGroupRequest struct {
	CampaignID             string       `json:"campaignId,omitempty"`
	OrgID                  string       `json:"orgId,omitempty"`
	Name                   string       `json:"name"`
	StartTime              string       `json:"startTime,omitempty"`
	DefaultBidAmount       moneyPayload `json:"defaultBidAmount"`
	AutomatedKeywordsOptIn bool         `json:"automatedKeywordsOptIn"`
	PricingModel           string       `json:"pricingModel"`
	Status                 string       `json:"status"`
}

type updateAdGroupRequest struct {
	Name                   string       `json:"name,omitempty"`
	DefaultBidAmount       moneyPayload `json:"defaultBidAmount,omitempty"`
	AutomatedKeywordsOptIn bool         `json:"automatedKeywordsOptIn"`
	Status                 string       `json:"status,omitempty"`
}

type createKeywordRequest struct {
	Text      string       `json:"text"`
	MatchType string       `json:"matchType"`
	BidAmount moneyPayload `json:"bidAmount"`
	Status    string       `json:"status,omitempty"`
}

type updateKeywordRequest struct {
	ID        string       `json:"id"`
	BidAmount moneyPayload `json:"bidAmount,omitempty"`
	Status    string       `json:"status,omitempty"`
}

type createNegativeKeywordRequest struct {
	Text      string `json:"text"`
	MatchType string `json:"matchType"`
	Status    string `json:"status,omitempty"`
}

type updateNegativeKeywordRequest struct {
	ID     string `json:"id"`
	Status string `json:"status,omitempty"`
}

type createCreativeRequest struct {
	AdamID        int64  `json:"adamId"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	ProductPageID string `json:"productPageId"`
}

type createAdRequest struct {
	CreativeID string `json:"creativeId"`
	Name       string `json:"name"`
	Status     string `json:"status,omitempty"`
}

type updateAdRequest struct {
	Status string `json:"status,omitempty"`
}

type bulkResponse struct {
	Data []bulkResponseItem `json:"data"`
}

type bulkResponseItem struct {
	ID      apiID           `json:"id"`
	Success *bool           `json:"success,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
	Errors  json.RawMessage `json:"errors,omitempty"`
}

type objectWithID struct {
	ID apiID `json:"id"`
}

type createResponse struct {
	Data objectWithID `json:"data"`
}

type createListResponse struct {
	Data []objectWithID `json:"data"`
}

func apiStatus(value spec.Status) string {
	if value == spec.StatusPaused {
		return apiStatusPaused
	}
	return apiStatusEnabled
}

func keywordStatus(value spec.Status) string {
	if value == spec.StatusPaused {
		return apiStatusPaused
	}
	return apiStatusActive
}

func moneyFromDecimal(value spec.Decimal, currency string) moneyPayload {
	return moneyPayload{Amount: value.String(), Currency: strings.TrimSpace(currency)}
}

func appIDFromSpec(input spec.Spec) (int64, error) {
	value := strings.TrimSpace(input.App.AppID)
	if value == "" {
		return 0, errors.New("app.app_id is blank")
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse app.app_id %q: %w", value, err)
	}
	return parsed, nil
}

func parseItemLevelError(body []byte) error {
	var response bulkResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("parse bulk response: %w", err)
	}
	if response.Data == nil {
		return errors.New("bulk response missing data")
	}
	for _, item := range response.Data {
		if item.Success != nil && !*item.Success {
			return fmt.Errorf("item %q failed: %s", string(item.ID), rawMessageSummary(item.Error, item.Errors))
		}
		if hasRawMessage(item.Error) {
			return fmt.Errorf("item %q failed: %s", string(item.ID), rawMessageSummary(item.Error, item.Errors))
		}
		if hasRawMessage(item.Errors) {
			return fmt.Errorf("item %q failed: %s", string(item.ID), rawMessageSummary(item.Errors, item.Error))
		}
	}
	return nil
}

func decodeCreatedID(body []byte) (string, error) {
	var direct objectWithID
	if err := json.Unmarshal(body, &direct); err == nil && direct.ID != "" {
		return string(direct.ID), nil
	}
	var wrapped createResponse
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Data.ID != "" {
		return string(wrapped.Data.ID), nil
	}
	var wrappedList createListResponse
	if err := json.Unmarshal(body, &wrappedList); err == nil && len(wrappedList.Data) > 0 && wrappedList.Data[0].ID != "" {
		return string(wrappedList.Data[0].ID), nil
	}
	return "", errors.New("response missing resource id")
}

func hasRawMessage(message json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(message))
	return trimmed != "" && trimmed != "null" && trimmed != "[]" && trimmed != "{}"
}

func rawMessageSummary(primary, fallback json.RawMessage) string {
	if text := summarizeRawMessage(primary); text != "" {
		return text
	}
	if text := summarizeRawMessage(fallback); text != "" {
		return text
	}
	return "unknown item-level error"
}

func summarizeRawMessage(message json.RawMessage) string {
	trimmed := strings.TrimSpace(string(message))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	var asString string
	if err := json.Unmarshal(message, &asString); err == nil && strings.TrimSpace(asString) != "" {
		return strings.TrimSpace(asString)
	}
	return trimmed
}
