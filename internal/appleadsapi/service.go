package appleadsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/logging"
	"github.com/robaerd/asactl/internal/spec"
)

type Service struct {
	client *Client
	logger *slog.Logger
}

type ServiceOption func(*Service)

const createStartTimeLead = time.Minute

var (
	listMaxPages = 1000
	listMaxRows  = 1_000_000
)

type paginationInfo struct {
	ItemsPerPage int `json:"itemsPerPage"`
	StartIndex   int `json:"startIndex"`
	TotalResults int `json:"totalResults"`
}

type resourceList[T any] struct {
	Data       []T            `json:"data"`
	Pagination paginationInfo `json:"pagination"`
}

type apiID string

func (id *apiID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*id = ""
		return nil
	}

	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*id = apiID(text)
		return nil
	}

	var number json.Number
	if err := json.Unmarshal(data, &number); err == nil {
		*id = apiID(number.String())
		return nil
	}

	var integer int64
	if err := json.Unmarshal(data, &integer); err == nil {
		*id = apiID(strconv.FormatInt(integer, 10))
		return nil
	}

	return errors.New("api id must be a string or number")
}

type remoteCampaign struct {
	ID                 apiID    `json:"id"`
	AdamID             apiID    `json:"adamId"`
	Name               string   `json:"name"`
	CountriesOrRegions []string `json:"countriesOrRegions"`
	DailyBudgetAmount  money    `json:"dailyBudgetAmount"`
	Status             string   `json:"status"`
	Deleted            bool     `json:"deleted"`
}

type remoteAdGroup struct {
	ID                   apiID  `json:"id"`
	CampaignID           apiID  `json:"campaignId"`
	Name                 string `json:"name"`
	DefaultBidAmount     money  `json:"defaultBidAmount"`
	AutomatedKeywordsOpt bool   `json:"automatedKeywordsOptIn"`
	Status               string `json:"status"`
	Deleted              bool   `json:"deleted"`
}

type remoteKeyword struct {
	ID        apiID  `json:"id"`
	Text      string `json:"text"`
	MatchType string `json:"matchType"`
	BidAmount money  `json:"bidAmount"`
	Status    string `json:"status"`
	Deleted   bool   `json:"deleted"`
}

type remoteNegative struct {
	ID        apiID  `json:"id"`
	Text      string `json:"text"`
	MatchType string `json:"matchType"`
	Status    string `json:"status"`
	Deleted   bool   `json:"deleted"`
}

type remoteCustomAd struct {
	ID           apiID  `json:"id"`
	CampaignID   apiID  `json:"campaignId"`
	AdGroupID    apiID  `json:"adGroupId"`
	CreativeID   apiID  `json:"creativeId"`
	Name         string `json:"name"`
	CreativeType string `json:"creativeType"`
	Status       string `json:"status"`
	Deleted      bool   `json:"deleted"`
}

type remoteCreative struct {
	ID            apiID  `json:"id"`
	AdamID        apiID  `json:"adamId"`
	Name          string `json:"name"`
	ProductPageID apiID  `json:"productPageId"`
	Type          string `json:"type"`
	State         string `json:"state"`
}

type remoteProductPage struct {
	ID     apiID  `json:"id"`
	AdamID apiID  `json:"adamId"`
	Name   string `json:"name"`
	State  string `json:"state"`
}

type remoteACL struct {
	OrgID       apiID    `json:"orgId"`
	OrgName     string   `json:"orgName"`
	DisplayName string   `json:"displayName"`
	Currency    string   `json:"currency"`
	TimeZone    string   `json:"timeZone"`
	RoleNames   []string `json:"roleNames"`
}

type aclResponse struct {
	Data []remoteACL `json:"data"`
}

type money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency,omitempty"`
}

type applySession struct {
	desired           spec.Spec
	campaigns         map[string]diff.Campaign
	adGroups          map[string]diff.AdGroup
	productPages      []remoteProductPage
	productPagesReady bool
	creatives         []remoteCreative
	creativesReady    bool
	productPageIDs    map[string]string
	productPageAssets map[string]string
}

type ScopeSummary struct {
	ManagedCampaignCount  int `json:"managed_campaign_count"`
	OtherAppCampaignCount int `json:"other_app_campaign_count"`
	WipeTargetCount       int `json:"wipe_target_count"`
}

type FetchResult struct {
	State             diff.State      `json:"state"`
	Scope             ScopeSummary    `json:"scope_summary"`
	ManagedCampaigns  []string        `json:"managed_campaigns,omitempty"`
	OtherAppCampaigns []string        `json:"other_app_campaigns,omitempty"`
	OrgCampaigns      []diff.Campaign `json:"org_campaigns,omitempty"`
}

type ProductPageSummary struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state,omitempty"`
}

type AuthCheckResult struct {
	CampaignGroupID   string               `json:"campaign_group_id"`
	OrgName           string               `json:"org_name,omitempty"`
	AppID             string               `json:"app_id"`
	ProductPages      []ProductPageSummary `json:"product_pages,omitempty"`
	ProductPageCount  int                  `json:"product_page_count"`
	Scope             ScopeSummary         `json:"scope_summary"`
	ManagedCampaigns  []string             `json:"managed_campaigns,omitempty"`
	OtherAppCampaigns []string             `json:"other_app_campaigns,omitempty"`
	Warnings          []string             `json:"warnings,omitempty"`
}

type campaignScope struct {
	managedCampaigns []remoteCampaign
	orgCampaigns     []diff.Campaign
	managedNames     []string
	otherAppNames    []string
	summary          ScopeSummary
}

const listPageSize = 1000

func WithServiceLogger(logger *slog.Logger) ServiceOption {
	return func(service *Service) {
		service.logger = logging.Component(logger, "appleadsapi.service")
	}
}

func NewService(client *Client, options ...ServiceOption) *Service {
	service := &Service{
		client: client,
		logger: logging.Component(nil, "appleadsapi.service"),
	}
	for _, option := range options {
		if option != nil {
			option(service)
		}
	}
	return service
}

func (s *Service) FetchState(ctx context.Context, desired spec.Spec) (FetchResult, error) {
	result := FetchResult{State: diff.State{}}
	logger := s.logger.With("app_id", desired.App.AppID)
	logger.Debug("Fetch state started")
	creativeProductPageIndex, err := s.buildCreativeProductPageIndex(ctx, desired)
	if err != nil {
		return result, err
	}
	desiredCustomAdKeys := desiredCustomAdKeySet(desired)

	scope, err := s.scopeCampaigns(ctx, desired)
	if err != nil {
		return result, err
	}
	result.Scope = scope.summary
	result.ManagedCampaigns = slices.Clone(scope.managedNames)
	result.OtherAppCampaigns = slices.Clone(scope.otherAppNames)
	// OrgCampaigns intentionally keeps the full campaign-group view so wipe-org can delete outside the managed app scope.
	result.OrgCampaigns = slices.Clone(scope.orgCampaigns)
	logger.Debug("Campaign scope resolved", "managed_campaigns", scope.summary.ManagedCampaignCount, "other_app_campaigns", scope.summary.OtherAppCampaignCount)

	for _, campaign := range scope.managedCampaigns {
		budget, err := spec.ParseDecimal(campaign.DailyBudgetAmount.Amount)
		if err != nil {
			return result, err
		}
		campaignStatus, err := normalizeManagedStatusOrError(campaign.Status)
		if err != nil {
			return result, err
		}
		campaignID := string(campaign.ID)
		campaignName := campaign.Name
		result.State.Campaigns = append(result.State.Campaigns, diff.Campaign{
			ID:          campaignID,
			Name:        campaignName,
			Storefronts: slices.Clone(campaign.CountriesOrRegions),
			DailyBudget: budget,
			Status:      campaignStatus,
		})

		campaignNegatives, err := s.listCampaignNegativeKeywords(ctx, campaignID)
		if err != nil {
			return result, err
		}
		for _, item := range campaignNegatives {
			if item.Deleted || isDeletedStatus(item.Status) {
				continue
			}
			negativeStatus, err := normalizeManagedStatusOrError(item.Status)
			if err != nil {
				return result, err
			}
			result.State.NegativeKeywords = append(result.State.NegativeKeywords, diff.NegativeKeyword{
				ID:           string(item.ID),
				Scope:        diff.ScopeCampaign,
				CampaignName: campaignName,
				Text:         item.Text,
				MatchType:    spec.MatchType(strings.ToUpper(item.MatchType)),
				Status:       negativeStatus,
			})
		}

		adgroups, err := s.listAdGroups(ctx, campaignID)
		if err != nil {
			return result, err
		}
		for _, item := range adgroups {
			if item.Deleted || isDeletedStatus(item.Status) {
				continue
			}
			adGroupID := string(item.ID)
			adGroupName := item.Name
			bid, err := spec.ParseDecimal(item.DefaultBidAmount.Amount)
			if err != nil {
				return result, err
			}
			adGroupStatus, err := normalizeManagedStatusOrError(item.Status)
			if err != nil {
				return result, err
			}
			result.State.AdGroups = append(result.State.AdGroups, diff.AdGroup{
				ID:            adGroupID,
				CampaignName:  campaignName,
				Name:          adGroupName,
				DefaultCPTBid: bid,
				Targeting:     remoteTargeting(item.AutomatedKeywordsOpt),
				Status:        adGroupStatus,
			})

			keywords, err := s.listKeywords(ctx, campaignID, adGroupID)
			if err != nil {
				return result, err
			}
			for _, keyword := range keywords {
				if keyword.Deleted || isDeletedStatus(keyword.Status) {
					continue
				}
				keywordBid, err := spec.ParseDecimal(keyword.BidAmount.Amount)
				if err != nil {
					return result, err
				}
				keywordStatus, err := normalizeManagedStatusOrError(keyword.Status)
				if err != nil {
					return result, err
				}
				result.State.Keywords = append(result.State.Keywords, diff.Keyword{
					ID:           string(keyword.ID),
					CampaignName: campaignName,
					AdGroupName:  adGroupName,
					Text:         keyword.Text,
					MatchType:    spec.MatchType(strings.ToUpper(keyword.MatchType)),
					Bid:          keywordBid,
					Status:       keywordStatus,
				})
			}

			adGroupNegatives, err := s.listAdGroupNegativeKeywords(ctx, campaignID, adGroupID)
			if err != nil {
				return result, err
			}
			for _, negative := range adGroupNegatives {
				if negative.Deleted || isDeletedStatus(negative.Status) {
					continue
				}
				negativeStatus, err := normalizeManagedStatusOrError(negative.Status)
				if err != nil {
					return result, err
				}
				result.State.NegativeKeywords = append(result.State.NegativeKeywords, diff.NegativeKeyword{
					ID:           string(negative.ID),
					Scope:        diff.ScopeAdGroup,
					CampaignName: campaignName,
					AdGroupName:  adGroupName,
					Text:         negative.Text,
					MatchType:    spec.MatchType(strings.ToUpper(negative.MatchType)),
					Status:       negativeStatus,
				})
			}

			customAds, err := s.listCustomAds(ctx, campaignID, adGroupID)
			if err != nil {
				return result, err
			}
			for _, item := range customAds {
				if item.Deleted || isDeletedStatus(item.Status) {
					continue
				}
				if !strings.EqualFold(item.CreativeType, apiCreativeTypeCPP) {
					continue
				}
				customAdStatus, err := normalizeManagedStatusOrError(item.Status)
				if err != nil {
					return result, err
				}
				productPageName := creativeProductPageIndex[string(item.CreativeID)]
				if productPageName == "" {
					productPageName = unresolvedProductPageName(item)
				}
				customAdKey := desiredCustomAdKey(campaignName, adGroupName, productPageName)
				if customAdStatus == spec.StatusPaused {
					if _, ok := desiredCustomAdKeys[customAdKey]; !ok {
						continue
					}
				}
				result.State.CustomAds = append(result.State.CustomAds, diff.CustomAd{
					ID:           string(item.ID),
					CampaignName: campaignName,
					AdGroupName:  adGroupName,
					ProductPage:  productPageName,
					Status:       customAdStatus,
					IsDefault:    false,
				})
			}
		}
	}
	diff.MarkRemoteState(&result.State)
	logger.Debug(
		"Fetch state completed",
		"campaigns", len(result.State.Campaigns),
		"adgroups", len(result.State.AdGroups),
		"keywords", len(result.State.Keywords),
		"negative_keywords", len(result.State.NegativeKeywords),
		"custom_ads", len(result.State.CustomAds),
	)
	return result, nil
}

func (s *Service) CheckAuth(ctx context.Context, desired spec.Spec) (AuthCheckResult, error) {
	result := AuthCheckResult{
		CampaignGroupID: strings.TrimSpace(s.client.orgID),
		AppID:           strings.TrimSpace(desired.App.AppID),
	}
	logger := s.logger.With("campaign_group_id", result.CampaignGroupID, "app_id", result.AppID)
	logger.Debug("Authentication check started")

	acls, err := s.listACLs(ctx)
	if err != nil {
		return result, err
	}
	campaignGroupACL, err := matchACLByCampaignGroupID(acls, result.CampaignGroupID)
	if err != nil {
		return result, err
	}
	result.OrgName = strings.TrimSpace(campaignGroupACL.OrgName)
	logger.Debug("Resolved organization access", "org_name", result.OrgName, "role_count", len(campaignGroupACL.RoleNames))

	productPages, err := s.listProductPages(ctx, result.AppID)
	if err != nil {
		return result, err
	}
	result.ProductPages = summarizeProductPages(productPages)
	result.ProductPageCount = len(result.ProductPages)
	result.Warnings = append(result.Warnings, productPageWarnings(desired, productPages)...)
	logger.Debug("Resolved product pages", "product_page_count", result.ProductPageCount)

	scope, err := s.scopeCampaigns(ctx, desired)
	if err != nil {
		return result, err
	}
	result.Scope = scope.summary
	result.ManagedCampaigns = slices.Clone(scope.managedNames)
	result.OtherAppCampaigns = slices.Clone(scope.otherAppNames)
	if result.Scope.ManagedCampaignCount == 0 {
		result.Warnings = append(result.Warnings, "no managed campaigns currently exist for the configured campaign_group.id + app.app_id scope")
	}
	logger.Debug(
		"Authentication check completed",
		"managed_campaigns", result.Scope.ManagedCampaignCount,
		"other_app_campaigns", result.Scope.OtherAppCampaignCount,
		"warnings", len(result.Warnings),
	)
	return result, nil
}

func (s *Service) ApplyPlan(ctx context.Context, desired spec.Spec, plan diff.Plan) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	logger := s.logger.With("app_id", desired.App.AppID)
	logger.Debug("Apply plan started", "summary", diff.RenderSummary(plan), "action_count", len(plan.Actions))
	session, err := s.newApplySession(ctx, desired, plan)
	if err != nil {
		return err
	}

	var campaignDeletes []diff.Action
	var campaignActions []diff.Action
	var adGroupDeletes []diff.Action
	var adGroupActions []diff.Action
	var keywordActions []diff.Action
	var negativeActions []diff.Action
	var customAdActions []diff.Action

	for _, action := range plan.Actions {
		if action.Operation == diff.OperationNoop {
			continue
		}
		switch action.Kind {
		case diff.ResourceCampaign:
			if action.Operation == diff.OperationDelete {
				campaignDeletes = append(campaignDeletes, action)
			} else {
				campaignActions = append(campaignActions, action)
			}
		case diff.ResourceAdGroup:
			if action.Operation == diff.OperationDelete {
				adGroupDeletes = append(adGroupDeletes, action)
			} else {
				adGroupActions = append(adGroupActions, action)
			}
		case diff.ResourceKeyword:
			keywordActions = append(keywordActions, action)
		case diff.ResourceNegativeKeyword:
			negativeActions = append(negativeActions, action)
		case diff.ResourceCustomAd:
			customAdActions = append(customAdActions, action)
		}
	}
	logger.Debug(
		"Apply plan grouped",
		"delete", len(campaignDeletes),
		"campaign", len(campaignActions),
		"adgroup_delete", len(adGroupDeletes),
		"adgroup", len(adGroupActions),
		"keyword", len(keywordActions),
		"negative_keyword", len(negativeActions),
		"custom_ad", len(customAdActions),
	)

	if err := contextErr(ctx); err != nil {
		return err
	}
	for index, action := range campaignDeletes {
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyCampaign(ctx, session, action); err != nil {
			return fmt.Errorf("campaign delete phase action %d (%s): %w", index, action.Key, err)
		}
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	for index, action := range campaignActions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyCampaign(ctx, session, action); err != nil {
			return fmt.Errorf("campaign phase action %d (%s): %w", index, action.Key, err)
		}
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	for index, action := range adGroupActions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyAdGroup(ctx, session, action); err != nil {
			return fmt.Errorf("adgroup phase action %d (%s): %w", index, action.Key, err)
		}
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	if err := s.applyKeywords(ctx, session, keywordActions); err != nil {
		return fmt.Errorf("keyword phase: %w", err)
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	if err := s.applyNegativeKeywords(ctx, session, negativeActions); err != nil {
		return fmt.Errorf("negative keyword phase: %w", err)
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	if err := s.applyCustomAds(ctx, session, customAdActions); err != nil {
		return fmt.Errorf("custom ad phase: %w", err)
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	for index, action := range adGroupDeletes {
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyAdGroup(ctx, session, action); err != nil {
			return fmt.Errorf("adgroup delete phase action %d (%s): %w", index, action.Key, err)
		}
	}
	logger.Info("Apply plan completed", "summary", diff.RenderSummary(plan))
	return nil
}

func (s *Service) newApplySession(ctx context.Context, desired spec.Spec, plan diff.Plan) (*applySession, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	session := &applySession{
		desired:           desired,
		campaigns:         map[string]diff.Campaign{},
		adGroups:          map[string]diff.AdGroup{},
		productPageIDs:    map[string]string{},
		productPageAssets: map[string]string{},
	}
	for _, action := range plan.Actions {
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		if err := indexApplyAction(action, session); err != nil {
			return nil, err
		}
	}
	return session, nil
}

func (s *Service) applyCampaign(ctx context.Context, session *applySession, action diff.Action) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	s.logger.Info("Applying action", "operation", action.Operation, "kind", action.Kind, "key", action.Key)
	switch action.Operation {
	case diff.OperationDelete:
		current, err := currentCampaign(action)
		if err != nil {
			return err
		}
		if current.ID == "" {
			return errors.New("cannot delete campaign without remote id")
		}
		if err := s.client.Delete(ctx, "/campaigns/"+current.ID, nil, nil); err != nil {
			return err
		}
		delete(session.campaigns, campaignLookupKey(current.Name))
		return nil
	case diff.OperationCreate:
		payload, err := s.buildCampaignCreateRequest(session, action)
		if err != nil {
			return err
		}
		body, err := s.client.requestJSON(ctx, "POST", "/campaigns", nil, payload)
		if err != nil {
			return err
		}
		createdID, err := decodeCreatedID(body)
		if err != nil {
			return err
		}
		desiredCampaign, err := desiredCampaign(action)
		if err != nil {
			return err
		}
		desiredCampaign.ID = createdID
		session.campaigns[campaignLookupKey(desiredCampaign.Name)] = desiredCampaign
		return nil
	case diff.OperationUpdate, diff.OperationActivate, diff.OperationPause:
		payload, err := s.buildCampaignUpdateRequest(action, session.desired.Defaults.Currency)
		if err != nil {
			return err
		}
		current, err := currentCampaign(action)
		if err != nil {
			return err
		}
		if current.ID == "" {
			return errors.New("cannot update campaign without remote id")
		}
		if _, err := s.client.requestJSON(ctx, "PUT", "/campaigns/"+current.ID, nil, payload); err != nil {
			return err
		}
		updatedCampaign, err := desiredCampaign(action)
		if err != nil {
			return err
		}
		updatedCampaign.ID = current.ID
		session.campaigns[campaignLookupKey(updatedCampaign.Name)] = updatedCampaign
		return nil
	default:
		return fmt.Errorf("unsupported campaign operation %q", action.Operation)
	}
}

func (s *Service) applyAdGroup(ctx context.Context, session *applySession, action diff.Action) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	s.logger.Info("Applying action", "operation", action.Operation, "kind", action.Kind, "key", action.Key)
	switch action.Operation {
	case diff.OperationDelete:
		current, err := currentAdGroup(action)
		if err != nil {
			return err
		}
		campaignID, err := session.resolveCampaignID(current.CampaignName)
		if err != nil {
			return err
		}
		if strings.TrimSpace(current.ID) == "" {
			return errors.New("cannot delete adgroup without remote id")
		}
		if err := s.client.Delete(ctx, "/campaigns/"+campaignID+"/adgroups/"+current.ID, nil, nil); err != nil {
			return err
		}
		delete(session.adGroups, adGroupLookupKey(current.CampaignName, current.Name))
		return nil
	case diff.OperationCreate:
		payload, campaignID, err := s.buildAdGroupCreateRequest(session, action)
		if err != nil {
			return err
		}
		body, err := s.client.requestJSON(ctx, "POST", "/campaigns/"+campaignID+"/adgroups", nil, payload)
		if err != nil {
			return err
		}
		createdID, err := decodeCreatedID(body)
		if err != nil {
			return err
		}
		desiredAdGroup, err := desiredAdGroup(action)
		if err != nil {
			return err
		}
		desiredAdGroup.ID = createdID
		session.adGroups[adGroupLookupKey(desiredAdGroup.CampaignName, desiredAdGroup.Name)] = desiredAdGroup
		return nil
	case diff.OperationUpdate, diff.OperationActivate, diff.OperationPause:
		payload, current, err := s.buildAdGroupUpdateRequest(action, session.desired.Defaults.Currency)
		if err != nil {
			return err
		}
		campaignID, err := session.resolveCampaignID(current.CampaignName)
		if err != nil {
			return err
		}
		if _, err := s.client.requestJSON(ctx, "PUT", "/campaigns/"+campaignID+"/adgroups/"+current.ID, nil, payload); err != nil {
			return err
		}
		updatedAdGroup, err := desiredAdGroup(action)
		if err != nil {
			return err
		}
		updatedAdGroup.ID = current.ID
		session.adGroups[adGroupLookupKey(updatedAdGroup.CampaignName, updatedAdGroup.Name)] = updatedAdGroup
		return nil
	default:
		return fmt.Errorf("unsupported adgroup operation %q", action.Operation)
	}
}

func (s *Service) applyKeywords(ctx context.Context, session *applySession, actions []diff.Action) error {
	if len(actions) == 0 {
		return nil
	}
	createGroups := map[string][]diff.Action{}
	updateGroups := map[string][]diff.Action{}
	deleteGroups := map[string][]diff.Action{}
	for _, action := range actions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		s.logger.Info("Applying action", "operation", action.Operation, "kind", action.Kind, "key", action.Key)
		switch action.Operation {
		case diff.OperationCreate:
			keyword, err := desiredKeyword(action)
			if err != nil {
				return err
			}
			groupKey := adGroupLookupKey(keyword.CampaignName, keyword.AdGroupName)
			createGroups[groupKey] = append(createGroups[groupKey], action)
		case diff.OperationUpdate, diff.OperationPause:
			keyword, err := currentKeyword(action)
			if err != nil {
				return err
			}
			groupKey := adGroupLookupKey(keyword.CampaignName, keyword.AdGroupName)
			updateGroups[groupKey] = append(updateGroups[groupKey], action)
		case diff.OperationDelete:
			keyword, err := currentKeyword(action)
			if err != nil {
				return err
			}
			groupKey := adGroupLookupKey(keyword.CampaignName, keyword.AdGroupName)
			deleteGroups[groupKey] = append(deleteGroups[groupKey], action)
		}
	}
	for _, groupKey := range sortedActionGroupKeys(createGroups) {
		groupActions := createGroups[groupKey]
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyKeywordGroupCreate(ctx, session, groupKey, groupActions); err != nil {
			return fmt.Errorf("create group %s (%s): %w", groupKey, joinActionKeys(groupActions), err)
		}
	}
	for _, groupKey := range sortedActionGroupKeys(updateGroups) {
		groupActions := updateGroups[groupKey]
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyKeywordGroupUpdate(ctx, session, groupKey, groupActions); err != nil {
			return fmt.Errorf("update group %s (%s): %w", groupKey, joinActionKeys(groupActions), err)
		}
	}
	for _, groupKey := range sortedActionGroupKeys(deleteGroups) {
		groupActions := deleteGroups[groupKey]
		if err := contextErr(ctx); err != nil {
			return err
		}
		if err := s.applyKeywordGroupDelete(ctx, session, groupKey, groupActions); err != nil {
			return fmt.Errorf("delete group %s (%s): %w", groupKey, joinActionKeys(groupActions), err)
		}
	}
	return nil
}

func (s *Service) applyKeywordGroupCreate(ctx context.Context, session *applySession, groupKey string, actions []diff.Action) error {
	if len(actions) == 0 {
		return nil
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	campaignID, adGroupID, err := session.resolveAdGroupIDsByLookup(groupKey)
	if err != nil {
		return err
	}
	payload := make([]createKeywordRequest, 0, len(actions))
	for _, action := range actions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		keyword, err := desiredKeyword(action)
		if err != nil {
			return err
		}
		payload = append(payload, createKeywordRequest{
			Text:      keyword.Text,
			MatchType: string(keyword.MatchType),
			BidAmount: moneyFromDecimal(keyword.Bid, session.desired.Defaults.Currency),
			Status:    keywordStatus(keyword.Status),
		})
	}
	body, err := s.client.requestJSON(ctx, "POST", "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/targetingkeywords/bulk", nil, payload)
	if err != nil {
		return err
	}
	if err := parseItemLevelError(body); err != nil {
		return fmt.Errorf("create targeting keywords for %s (%s): %w", groupKey, joinActionKeys(actions), err)
	}
	return nil
}

func (s *Service) applyKeywordGroupUpdate(ctx context.Context, session *applySession, groupKey string, actions []diff.Action) error {
	if len(actions) == 0 {
		return nil
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	campaignID, adGroupID, err := session.resolveAdGroupIDsByLookup(groupKey)
	if err != nil {
		return err
	}
	payload := make([]updateKeywordRequest, 0, len(actions))
	for _, action := range actions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		current, err := currentKeyword(action)
		if err != nil {
			return err
		}
		if current.ID == "" {
			return errors.New("cannot update keyword without remote id")
		}
		keyword, err := desiredKeyword(action)
		if err != nil {
			return err
		}
		payload = append(payload, updateKeywordRequest{
			ID:        current.ID,
			BidAmount: moneyFromDecimal(keyword.Bid, session.desired.Defaults.Currency),
			Status:    keywordStatus(keyword.Status),
		})
	}
	body, err := s.client.requestJSON(ctx, "PUT", "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/targetingkeywords/bulk", nil, payload)
	if err != nil {
		return err
	}
	if err := parseItemLevelError(body); err != nil {
		return fmt.Errorf("update targeting keywords for %s (%s): %w", groupKey, joinActionKeys(actions), err)
	}
	return nil
}

func (s *Service) applyKeywordGroupDelete(ctx context.Context, session *applySession, groupKey string, actions []diff.Action) error {
	if len(actions) == 0 {
		return nil
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	campaignID, adGroupID, err := session.resolveAdGroupIDsByLookup(groupKey)
	if err != nil {
		return err
	}
	for _, action := range actions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		current, err := currentKeyword(action)
		if err != nil {
			return err
		}
		if strings.TrimSpace(current.ID) == "" {
			return errors.New("cannot delete keyword without remote id")
		}
		// Apple Ads v5 keyword deletion is modeled as one delete per keyword until a documented bulk endpoint exists.
		if err := s.client.Delete(ctx, "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/targetingkeywords/"+current.ID, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) applyNegativeKeywords(ctx context.Context, session *applySession, actions []diff.Action) error {
	if len(actions) == 0 {
		return nil
	}
	type bucket struct {
		path    string
		actions []diff.Action
	}
	createBuckets := map[string]bucket{}
	updateBuckets := map[string]bucket{}
	deleteBuckets := map[string]bucket{}
	for _, action := range actions {
		if err := contextErr(ctx); err != nil {
			return err
		}
		s.logger.Info("Applying action", "operation", action.Operation, "kind", action.Kind, "key", action.Key)
		switch action.Operation {
		case diff.OperationCreate:
			negative, err := desiredNegativeKeyword(action)
			if err != nil {
				return err
			}
			bucketKey, path, err := session.negativeKeywordPath(negative)
			if err != nil {
				return err
			}
			item := createBuckets[bucketKey]
			item.path = path
			item.actions = append(item.actions, action)
			createBuckets[bucketKey] = item
		case diff.OperationUpdate, diff.OperationPause:
			negative, err := currentNegativeKeyword(action)
			if err != nil {
				return err
			}
			bucketKey, path, err := session.negativeKeywordPath(negative)
			if err != nil {
				return err
			}
			item := updateBuckets[bucketKey]
			item.path = path
			item.actions = append(item.actions, action)
			updateBuckets[bucketKey] = item
		case diff.OperationDelete:
			negative, err := currentNegativeKeyword(action)
			if err != nil {
				return err
			}
			bucketKey, path, err := session.negativeKeywordPath(negative)
			if err != nil {
				return err
			}
			item := deleteBuckets[bucketKey]
			item.path = path
			item.actions = append(item.actions, action)
			deleteBuckets[bucketKey] = item
		}
	}
	for _, bucketKey := range sortedMapKeys(createBuckets) {
		item := createBuckets[bucketKey]
		if err := contextErr(ctx); err != nil {
			return err
		}
		payload := make([]createNegativeKeywordRequest, 0, len(item.actions))
		for _, action := range item.actions {
			if err := contextErr(ctx); err != nil {
				return err
			}
			negative, err := desiredNegativeKeyword(action)
			if err != nil {
				return err
			}
			payload = append(payload, createNegativeKeywordRequest{Text: negative.Text, MatchType: string(negative.MatchType), Status: keywordStatus(negative.Status)})
		}
		body, err := s.client.requestJSON(ctx, "POST", item.path, nil, payload)
		if err != nil {
			return err
		}
		if err := parseItemLevelError(body); err != nil {
			return fmt.Errorf("create negative keywords for %s (%s): %w", bucketKey, joinActionKeys(item.actions), err)
		}
	}
	for _, bucketKey := range sortedMapKeys(updateBuckets) {
		item := updateBuckets[bucketKey]
		if err := contextErr(ctx); err != nil {
			return err
		}
		payload := make([]updateNegativeKeywordRequest, 0, len(item.actions))
		for _, action := range item.actions {
			if err := contextErr(ctx); err != nil {
				return err
			}
			current, err := currentNegativeKeyword(action)
			if err != nil {
				return err
			}
			if current.ID == "" {
				return errors.New("cannot update negative keyword without remote id")
			}
			negative, err := desiredNegativeKeyword(action)
			if err != nil {
				return err
			}
			payload = append(payload, updateNegativeKeywordRequest{ID: current.ID, Status: keywordStatus(negative.Status)})
		}
		body, err := s.client.requestJSON(ctx, "PUT", item.path, nil, payload)
		if err != nil {
			return err
		}
		if err := parseItemLevelError(body); err != nil {
			return fmt.Errorf("update negative keywords for %s (%s): %w", bucketKey, joinActionKeys(item.actions), err)
		}
	}
	for _, bucketKey := range sortedMapKeys(deleteBuckets) {
		item := deleteBuckets[bucketKey]
		if err := contextErr(ctx); err != nil {
			return err
		}
		payload := make([]int64, 0, len(item.actions))
		deletePath := ""
		for _, action := range item.actions {
			if err := contextErr(ctx); err != nil {
				return err
			}
			current, err := currentNegativeKeyword(action)
			if err != nil {
				return err
			}
			if strings.TrimSpace(current.ID) == "" {
				return errors.New("cannot delete negative keyword without remote id")
			}
			currentDeletePath, err := session.negativeKeywordDeletePath(current)
			if err != nil {
				return err
			}
			if deletePath == "" {
				deletePath = currentDeletePath
			} else if deletePath != currentDeletePath {
				return fmt.Errorf("inconsistent negative keyword delete path for bucket %s", bucketKey)
			}
			negativeID, err := parseResourceIDInt64(current.ID)
			if err != nil {
				return fmt.Errorf("parse negative keyword id %q: %w", current.ID, err)
			}
			payload = append(payload, negativeID)
		}
		if _, err := s.client.requestJSON(ctx, http.MethodPost, deletePath, nil, payload); err != nil {
			return err
		}
		s.logger.Debug("Deleted negative keyword batch", "bucket", bucketKey, "count", len(payload), "path", deletePath)
	}
	return nil
}

func (s *Service) applyCustomAds(ctx context.Context, session *applySession, actions []diff.Action) error {
	var updates []diff.Action
	var deletes []diff.Action
	var creates []diff.Action
	for _, action := range actions {
		switch action.Operation {
		case diff.OperationDelete:
			deletes = append(deletes, action)
		case diff.OperationCreate:
			creates = append(creates, action)
		default:
			updates = append(updates, action)
		}
	}
	for _, action := range updates {
		if err := s.applyCustomAd(ctx, session, action); err != nil {
			return err
		}
	}
	for _, action := range deletes {
		if err := s.applyCustomAd(ctx, session, action); err != nil {
			return err
		}
	}
	for _, action := range creates {
		if err := s.applyCustomAd(ctx, session, action); err != nil {
			return err
		}
	}
	return nil
}

func sortedActionGroupKeys(groups map[string][]diff.Action) []string {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func sortedMapKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (s *Service) applyCustomAd(ctx context.Context, session *applySession, action diff.Action) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	s.logger.Info("Applying action", "operation", action.Operation, "kind", action.Kind, "key", action.Key)
	switch action.Operation {
	case diff.OperationDelete:
		current, err := currentCustomAd(action)
		if err != nil {
			return err
		}
		if strings.TrimSpace(current.ID) == "" {
			return errors.New("cannot delete custom ad without remote id")
		}
		campaignID, adGroupID, err := session.resolveAdGroupIDs(current.CampaignName, current.AdGroupName)
		if err != nil {
			return err
		}
		_, err = s.client.requestJSON(ctx, "PUT", "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/ads/"+current.ID, nil, updateAdRequest{Status: apiStatusPaused})
		return err
	case diff.OperationCreate:
		payload, campaignID, adGroupID, err := s.buildCustomAdCreateRequest(ctx, session, action)
		if err != nil {
			return err
		}
		body, err := s.client.requestJSON(ctx, "POST", "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/ads", nil, payload)
		if err != nil {
			return err
		}
		_, err = decodeCreatedID(body)
		return err
	case diff.OperationUpdate, diff.OperationActivate, diff.OperationPause:
		payload := updateAdRequest{}
		customAd, err := desiredCustomAd(action)
		if err != nil {
			return err
		}
		payload.Status = apiStatus(customAd.Status)
		current, err := currentCustomAd(action)
		if err != nil {
			return err
		}
		if current.ID == "" {
			return errors.New("cannot update custom ad without remote id")
		}
		campaignID, adGroupID, err := session.resolveAdGroupIDs(customAd.CampaignName, customAd.AdGroupName)
		if err != nil {
			return err
		}
		_, err = s.client.requestJSON(ctx, "PUT", "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/ads/"+current.ID, nil, payload)
		return err
	default:
		return fmt.Errorf("unsupported custom ad operation %q", action.Operation)
	}
}

func (s *Service) buildCampaignCreateRequest(session *applySession, action diff.Action) (createCampaignRequest, error) {
	campaign, err := desiredCampaign(action)
	if err != nil {
		return createCampaignRequest{}, err
	}
	appID, err := appIDFromSpec(session.desired)
	if err != nil {
		return createCampaignRequest{}, err
	}
	return createCampaignRequest{
		OrgID:              s.client.orgID,
		Name:               campaign.Name,
		StartTime:          createStartTime(),
		AdamID:             appID,
		BillingEvent:       apiBillingEventTaps,
		DailyBudgetAmount:  moneyFromDecimal(campaign.DailyBudget, session.desired.Defaults.Currency),
		CountriesOrRegions: slices.Clone(campaign.Storefronts),
		Status:             apiStatus(campaign.Status),
		SupplySources:      []string{apiSupplySourceSearchResults},
		AdChannelType:      apiAdChannelSearch,
	}, nil
}

func (s *Service) buildCampaignUpdateRequest(action diff.Action, currency string) (updateCampaignRequest, error) {
	campaign, err := desiredCampaign(action)
	if err != nil {
		return updateCampaignRequest{}, err
	}
	payload := &campaignUpdatePayload{}
	if action.Operation == diff.OperationActivate || action.Operation == diff.OperationPause || actionHasFieldChange(action, "status") {
		payload.Status = apiStatus(campaign.Status)
	}
	if actionHasFieldChange(action, "daily_budget") {
		money := moneyFromDecimal(campaign.DailyBudget, currency)
		payload.DailyBudgetAmount = &money
	}
	request := updateCampaignRequest{Campaign: payload}
	if actionHasFieldChange(action, "storefronts") {
		payload.CountriesOrRegions = slices.Clone(campaign.Storefronts)
		request.ClearGeoTargetingOnCountryOrRegionChange = true
	}
	if actionHasFieldChange(action, "name") {
		payload.Name = campaign.Name
	}
	if payload.Name == "" && payload.DailyBudgetAmount == nil && len(payload.CountriesOrRegions) == 0 && payload.Status == "" {
		return updateCampaignRequest{}, errors.New("campaign update requires at least one changed field")
	}
	return request, nil
}

func (s *Service) buildAdGroupCreateRequest(session *applySession, action diff.Action) (createAdGroupRequest, string, error) {
	adGroup, err := desiredAdGroup(action)
	if err != nil {
		return createAdGroupRequest{}, "", err
	}
	campaignID, err := session.resolveCampaignID(adGroup.CampaignName)
	if err != nil {
		return createAdGroupRequest{}, "", err
	}
	return createAdGroupRequest{
		CampaignID:             campaignID,
		OrgID:                  s.client.orgID,
		Name:                   adGroup.Name,
		StartTime:              createStartTime(),
		DefaultBidAmount:       moneyFromDecimal(adGroup.DefaultCPTBid, session.desired.Defaults.Currency),
		AutomatedKeywordsOptIn: adGroup.Targeting == spec.TargetingSearchMatch,
		PricingModel:           apiPricingModelCPC,
		Status:                 apiStatus(adGroup.Status),
	}, campaignID, nil
}

func createStartTime() string {
	return time.Now().UTC().Add(createStartTimeLead).Format("2006-01-02T15:04:05.000")
}

func (s *Service) buildAdGroupUpdateRequest(action diff.Action, currency string) (updateAdGroupRequest, diff.AdGroup, error) {
	adGroup, err := desiredAdGroup(action)
	if err != nil {
		return updateAdGroupRequest{}, diff.AdGroup{}, err
	}
	current, err := currentAdGroup(action)
	if err != nil {
		return updateAdGroupRequest{}, diff.AdGroup{}, err
	}
	if current.ID == "" {
		return updateAdGroupRequest{}, diff.AdGroup{}, errors.New("cannot update adgroup without remote id")
	}
	return updateAdGroupRequest{
		Name:                   adGroup.Name,
		DefaultBidAmount:       moneyFromDecimal(adGroup.DefaultCPTBid, currency),
		AutomatedKeywordsOptIn: adGroup.Targeting == spec.TargetingSearchMatch,
		Status:                 apiStatus(adGroup.Status),
	}, current, nil
}

func (s *Service) buildCustomAdCreateRequest(ctx context.Context, session *applySession, action diff.Action) (createAdRequest, string, string, error) {
	customAd, err := desiredCustomAd(action)
	if err != nil {
		return createAdRequest{}, "", "", err
	}
	campaignID, adGroupID, err := session.resolveAdGroupIDs(customAd.CampaignName, customAd.AdGroupName)
	if err != nil {
		return createAdRequest{}, "", "", err
	}
	creativeID, err := s.resolveCreativeID(ctx, session, customAd.ProductPage)
	if err != nil {
		return createAdRequest{}, "", "", err
	}
	return createAdRequest{CreativeID: creativeID, Name: customAdCreateName(customAd.ProductPage), Status: apiStatus(customAd.Status)}, campaignID, adGroupID, nil
}

func (s *Service) resolveCreativeID(ctx context.Context, session *applySession, productPageKey string) (string, error) {
	if creativeID := strings.TrimSpace(session.productPageAssets[productPageKey]); creativeID != "" {
		s.logger.Debug("Reusing resolved creative", "product_page", productPageKey, "creative_id", creativeID)
		return creativeID, nil
	}
	productPageID, err := s.resolveProductPageID(ctx, session, productPageKey)
	if err != nil {
		return "", err
	}
	if err := s.ensureCreatives(ctx, session); err != nil {
		return "", err
	}
	for _, creative := range session.creatives {
		if !strings.EqualFold(creative.Type, apiCreativeTypeCPP) {
			continue
		}
		if strings.TrimSpace(string(creative.ProductPageID)) != productPageID {
			continue
		}
		creativeID := string(creative.ID)
		session.productPageAssets[productPageKey] = creativeID
		s.logger.Debug("Resolved creative from existing assets", "product_page", productPageKey, "creative_id", creativeID, "product_page_id", productPageID)
		return creativeID, nil
	}
	appID, err := appIDFromSpec(session.desired)
	if err != nil {
		return "", err
	}
	productPage, ok := session.desired.ProductPages[productPageKey]
	if !ok {
		return "", fmt.Errorf("unknown product_page %q", productPageKey)
	}
	body, err := s.client.requestJSON(ctx, "POST", "/creatives", nil, createCreativeRequest{
		AdamID:        appID,
		Name:          productPageName(productPageKey, productPage),
		Type:          apiCreativeTypeCPP,
		ProductPageID: productPageID,
	})
	if err != nil {
		return "", err
	}
	creativeID, err := decodeCreatedID(body)
	if err != nil {
		return "", err
	}
	session.productPageAssets[productPageKey] = creativeID
	session.creatives = append(session.creatives, remoteCreative{ID: apiID(creativeID), AdamID: apiID(strconv.FormatInt(appID, 10)), Name: productPageName(productPageKey, productPage), ProductPageID: apiID(productPageID), Type: apiCreativeTypeCPP, State: "VALID"})
	s.logger.Debug("Created creative for custom product page", "product_page", productPageKey, "creative_id", creativeID, "product_page_id", productPageID)
	return creativeID, nil
}

func (s *Service) resolveProductPageID(ctx context.Context, session *applySession, productPageKey string) (string, error) {
	if productPageID := strings.TrimSpace(session.productPageIDs[productPageKey]); productPageID != "" {
		s.logger.Debug("Reusing resolved product page", "product_page", productPageKey, "product_page_id", productPageID)
		if err := s.ensureProductPages(ctx, session); err != nil {
			return "", err
		}
		if !hasMatchingProductPageID(productPageID, session.productPages) {
			return "", fmt.Errorf("no Apple Ads product page found for product_page %q (%q)", productPageKey, productPageID)
		}
		return productPageID, nil
	}
	productPage, ok := session.desired.ProductPages[productPageKey]
	if !ok {
		return "", fmt.Errorf("unknown product_page %q", productPageKey)
	}
	if productPageID := strings.TrimSpace(productPage.ProductPageID); productPageID != "" {
		if err := s.ensureProductPages(ctx, session); err != nil {
			return "", err
		}
		if !hasMatchingProductPageID(productPageID, session.productPages) {
			return "", fmt.Errorf("no Apple Ads product page found for product_page %q (%q)", productPageKey, productPageID)
		}
		session.productPageIDs[productPageKey] = productPageID
		s.logger.Debug("Resolved product page from config", "product_page", productPageKey, "product_page_id", productPageID)
		return productPageID, nil
	}
	return "", fmt.Errorf("product_page %q is missing product_page_id", productPageKey)
}

func productPageName(key string, productPage spec.ProductPage) string {
	if name := strings.TrimSpace(productPage.Name); name != "" {
		return name
	}
	return strings.ToLower(strings.TrimSpace(key))
}

func remoteTargeting(automatedKeywordsOptIn bool) spec.Targeting {
	if automatedKeywordsOptIn {
		return spec.TargetingSearchMatch
	}
	return spec.TargetingKeywords
}

func normalizeManagedRemoteStatus(value string) (status spec.Status) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ENABLED", "ACTIVE", "RUNNING":
		return spec.StatusActive
	case "PAUSED", "DISABLED":
		return spec.StatusPaused
	default:
		return spec.Status(strings.ToUpper(strings.TrimSpace(value)))
	}
}

func unsupportedManagedStatus(value string) error {
	return fmt.Errorf("managed remote resource has unsupported status %q", strings.TrimSpace(value))
}

func normalizeManagedStatusOrError(value string) (spec.Status, error) {
	status := normalizeManagedRemoteStatus(value)
	switch status {
	case spec.StatusActive, spec.StatusPaused:
		return status, nil
	default:
		return "", unsupportedManagedStatus(value)
	}
}

func (s *Service) ensureProductPages(ctx context.Context, session *applySession) error {
	if session.productPagesReady {
		return nil
	}
	appID := strings.TrimSpace(session.desired.App.AppID)
	if appID == "" {
		return errors.New("app.app_id is blank")
	}
	productPages, err := listAll[remoteProductPage](ctx, s.client, "/apps/"+appID+"/product-pages")
	if err != nil {
		return err
	}
	session.productPages = productPages
	session.productPagesReady = true
	s.logger.Debug("Fetched product pages", "count", len(productPages))
	return nil
}

func (s *Service) ensureCreatives(ctx context.Context, session *applySession) error {
	if session.creativesReady {
		return nil
	}
	creatives, err := s.listCreatives(ctx)
	if err != nil {
		return err
	}
	session.creatives = creatives
	session.creativesReady = true
	s.logger.Debug("Fetched creatives", "count", len(creatives))
	return nil
}

func (s *Service) buildCreativeProductPageIndex(ctx context.Context, desired spec.Spec) (map[string]string, error) {
	productPageKeys := map[string]string{}
	for productPageKey, productPage := range desired.ProductPages {
		if productPageID := strings.TrimSpace(productPage.ProductPageID); productPageID != "" {
			productPageKeys[productPageID] = productPageKey
		}
	}
	creatives, err := s.listCreatives(ctx)
	if err != nil {
		return nil, err
	}
	creativeProductPageIndex := map[string]string{}
	for _, creative := range creatives {
		if !strings.EqualFold(creative.Type, apiCreativeTypeCPP) {
			continue
		}
		productPageID := strings.TrimSpace(string(creative.ProductPageID))
		if productPageKey := productPageKeys[productPageID]; productPageKey != "" {
			creativeProductPageIndex[string(creative.ID)] = productPageKey
		}
	}
	s.logger.Debug("Built creative-to-product-page index", "product_page_count", len(productPageKeys), "creative_count", len(creativeProductPageIndex))
	return creativeProductPageIndex, nil
}

func (s *Service) scopeCampaigns(ctx context.Context, desired spec.Spec) (campaignScope, error) {
	scope := campaignScope{}
	campaigns, err := s.listCampaigns(ctx)
	if err != nil {
		return scope, err
	}
	targetAppID := strings.TrimSpace(desired.App.AppID)
	for _, campaign := range campaigns {
		if campaign.Deleted || isDeletedStatus(campaign.Status) {
			continue
		}
		scope.orgCampaigns = append(scope.orgCampaigns, diff.Campaign{
			ID:          string(campaign.ID),
			Name:        campaign.Name,
			Storefronts: slices.Clone(campaign.CountriesOrRegions),
		})
		if strings.TrimSpace(string(campaign.AdamID)) == targetAppID {
			scope.managedCampaigns = append(scope.managedCampaigns, campaign)
			scope.managedNames = append(scope.managedNames, campaign.Name)
			continue
		}
		scope.otherAppNames = append(scope.otherAppNames, campaign.Name)
	}
	scope.summary.ManagedCampaignCount = len(scope.managedNames)
	scope.summary.OtherAppCampaignCount = len(scope.otherAppNames)
	slices.Sort(scope.managedNames)
	slices.Sort(scope.otherAppNames)
	s.logger.Debug("Scoped campaigns", "managed_campaigns", len(scope.managedNames), "other_app_campaigns", len(scope.otherAppNames))
	return scope, nil
}

func (s *Service) listCampaigns(ctx context.Context) ([]remoteCampaign, error) {
	return listAll[remoteCampaign](ctx, s.client, "/campaigns")
}

func (s *Service) listACLs(ctx context.Context) ([]remoteACL, error) {
	var response aclResponse
	if err := s.client.GetUnscoped(ctx, "/acls", nil, &response); err != nil {
		return nil, err
	}
	return response.Data, nil
}

func (s *Service) listProductPages(ctx context.Context, appID string) ([]remoteProductPage, error) {
	if strings.TrimSpace(appID) == "" {
		return nil, errors.New("app.app_id is blank")
	}
	return listAll[remoteProductPage](ctx, s.client, "/apps/"+strings.TrimSpace(appID)+"/product-pages")
}

func (s *Service) listAdGroups(ctx context.Context, campaignID string) ([]remoteAdGroup, error) {
	return listAll[remoteAdGroup](ctx, s.client, "/campaigns/"+campaignID+"/adgroups")
}

func (s *Service) listKeywords(ctx context.Context, campaignID, adGroupID string) ([]remoteKeyword, error) {
	return listAll[remoteKeyword](ctx, s.client, "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/targetingkeywords")
}

func (s *Service) listCampaignNegativeKeywords(ctx context.Context, campaignID string) ([]remoteNegative, error) {
	return listAll[remoteNegative](ctx, s.client, "/campaigns/"+campaignID+"/negativekeywords")
}

func (s *Service) listAdGroupNegativeKeywords(ctx context.Context, campaignID, adGroupID string) ([]remoteNegative, error) {
	return listAll[remoteNegative](ctx, s.client, "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/negativekeywords")
}

func (s *Service) listCustomAds(ctx context.Context, campaignID, adGroupID string) ([]remoteCustomAd, error) {
	return listAll[remoteCustomAd](ctx, s.client, "/campaigns/"+campaignID+"/adgroups/"+adGroupID+"/ads")
}

func (s *Service) listCreatives(ctx context.Context) ([]remoteCreative, error) {
	return listAll[remoteCreative](ctx, s.client, "/creatives")
}

func listAll[T any](ctx context.Context, client *Client, path string) ([]T, error) {
	offset := 0
	items := []T{}
	pagesFetched := 0

	for {
		query := url.Values{}
		query.Set("offset", strconv.Itoa(offset))
		query.Set("limit", strconv.Itoa(listPageSize))

		var page resourceList[T]
		if err := client.Get(ctx, path, query, &page); err != nil {
			return nil, err
		}
		pagesFetched++
		items = append(items, page.Data...)

		total := page.Pagination.TotalResults
		if len(page.Data) == 0 {
			return items, nil
		}
		if total > 0 && len(items) >= total {
			return items, nil
		}
		if total == 0 && len(page.Data) < listPageSize {
			return items, nil
		}
		if pagesFetched >= listMaxPages || len(items) >= listMaxRows {
			return nil, fmt.Errorf("pagination guard triggered for %s after %d pages and %d rows", path, pagesFetched, len(items))
		}
		offset += len(page.Data)
	}
}

func summarizeProductPages(productPages []remoteProductPage) []ProductPageSummary {
	summaries := make([]ProductPageSummary, 0, len(productPages))
	for _, productPage := range productPages {
		summaries = append(summaries, ProductPageSummary{
			ID:    string(productPage.ID),
			Name:  productPage.Name,
			State: productPage.State,
		})
	}
	slices.SortFunc(summaries, func(left, right ProductPageSummary) int {
		return strings.Compare(spec.Fold(left.Name), spec.Fold(right.Name))
	})
	return summaries
}

func matchACLByCampaignGroupID(acls []remoteACL, campaignGroupID string) (remoteACL, error) {
	foldedTarget := spec.Fold(campaignGroupID)
	for _, acl := range acls {
		if spec.Fold(string(acl.OrgID)) == foldedTarget {
			return acl, nil
		}
	}
	names := make([]string, 0, len(acls))
	for _, acl := range acls {
		names = append(names, fmt.Sprintf("%s (%s)", acl.OrgName, string(acl.OrgID)))
	}
	slices.Sort(names)
	return remoteACL{}, fmt.Errorf("configured campaign_group.id %q is not accessible; available campaign groups: %s", campaignGroupID, strings.Join(names, ", "))
}

func productPageWarnings(desired spec.Spec, productPages []remoteProductPage) []string {
	warnings := []string{}
	if len(productPages) == 0 {
		return append(warnings, "no product pages were found for app.app_id")
	}
	for productPageKey, productPage := range desired.ProductPages {
		if hasMatchingProductPageID(productPage.ProductPageID, productPages) {
			continue
		}
		warnings = append(warnings, fmt.Sprintf("configured product_page %s (%q) was not found among app product pages", productPageKey, strings.TrimSpace(productPage.ProductPageID)))
	}
	slices.Sort(warnings)
	return warnings
}

func hasMatchingProductPageID(targetProductPageID string, productPages []remoteProductPage) bool {
	targetProductPageID = strings.TrimSpace(targetProductPageID)
	for _, productPage := range productPages {
		if targetProductPageID != "" && strings.TrimSpace(string(productPage.ID)) == targetProductPageID {
			return true
		}
	}
	return false
}

func unresolvedProductPageName(item remoteCustomAd) string {
	if item.CreativeID != "" {
		return "__unresolved__:creative:" + string(item.CreativeID)
	}
	if item.ID != "" {
		return "__unresolved__:ad:" + string(item.ID)
	}
	return "__unresolved__:" + strings.TrimSpace(item.Name)
}

func desiredCustomAdKeySet(desired spec.Spec) map[string]struct{} {
	state := diff.BuildDesiredState(desired)
	keys := make(map[string]struct{}, len(state.CustomAds))
	for _, customAd := range state.CustomAds {
		keys[desiredCustomAdKey(customAd.CampaignName, customAd.AdGroupName, customAd.ProductPage)] = struct{}{}
	}
	return keys
}

func desiredCustomAdKey(campaignName, adGroupName, productPage string) string {
	return spec.Fold(campaignName) + "|" + spec.Fold(adGroupName) + "|" + spec.Fold(productPage)
}

func actionHasFieldChange(action diff.Action, field string) bool {
	for _, change := range action.Changes {
		if change.Field == field {
			return true
		}
	}
	return false
}

func contextErr(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return nil
}

func joinActionKeys(actions []diff.Action) string {
	keys := make([]string, 0, len(actions))
	for _, action := range actions {
		keys = append(keys, action.Key)
	}
	return strings.Join(keys, ",")
}

func isDeletedStatus(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "DELETED")
}

func campaignLookupKey(name string) string {
	return spec.Fold(name)
}

func adGroupLookupKey(campaignName, adGroupName string) string {
	return spec.Fold(campaignName) + "|" + spec.Fold(adGroupName)
}

func customAdCreateName(cpp string) string {
	return strings.ToLower(strings.TrimSpace(cpp))
}

func currentCampaign(action diff.Action) (diff.Campaign, error) {
	return decodeActionValue[diff.Campaign](action.Current, "current campaign")
}

func indexApplyAction(action diff.Action, session *applySession) error {
	switch action.Kind {
	case diff.ResourceCampaign:
		return indexCampaignAction(action, session)
	case diff.ResourceAdGroup:
		return indexAdGroupAction(action, session)
	default:
		return nil
	}
}

func indexCampaignAction(action diff.Action, session *applySession) error {
	if action.Desired != nil && action.Operation != diff.OperationDelete {
		if _, err := desiredCampaign(action); err != nil {
			return fmt.Errorf("campaign action %q has invalid desired payload: %w", action.Key, err)
		}
	}
	switch action.Operation {
	case diff.OperationCreate:
		return nil
	case diff.OperationNoop, diff.OperationUpdate, diff.OperationPause, diff.OperationDelete:
		campaign, err := currentCampaign(action)
		if err != nil {
			return fmt.Errorf("campaign action %q has invalid current payload: %w", action.Key, err)
		}
		if strings.TrimSpace(campaign.ID) == "" {
			return fmt.Errorf("campaign action %q current payload is missing remote id", action.Key)
		}
		session.campaigns[campaignLookupKey(campaign.Name)] = campaign
		return nil
	default:
		return nil
	}
}

func indexAdGroupAction(action diff.Action, session *applySession) error {
	if action.Desired != nil && action.Operation != diff.OperationDelete {
		if _, err := desiredAdGroup(action); err != nil {
			return fmt.Errorf("adgroup action %q has invalid desired payload: %w", action.Key, err)
		}
	}
	switch action.Operation {
	case diff.OperationCreate:
		return nil
	case diff.OperationNoop, diff.OperationUpdate, diff.OperationPause, diff.OperationDelete:
		adGroup, err := currentAdGroup(action)
		if err != nil {
			return fmt.Errorf("adgroup action %q has invalid current payload: %w", action.Key, err)
		}
		if strings.TrimSpace(adGroup.ID) == "" {
			return fmt.Errorf("adgroup action %q current payload is missing remote id", action.Key)
		}
		session.adGroups[adGroupLookupKey(adGroup.CampaignName, adGroup.Name)] = adGroup
		return nil
	default:
		return nil
	}
}

func desiredCampaign(action diff.Action) (diff.Campaign, error) {
	return decodeActionValue[diff.Campaign](action.Desired, "desired campaign")
}

func currentAdGroup(action diff.Action) (diff.AdGroup, error) {
	return decodeActionValue[diff.AdGroup](action.Current, "current adgroup")
}

func desiredAdGroup(action diff.Action) (diff.AdGroup, error) {
	return decodeActionValue[diff.AdGroup](action.Desired, "desired adgroup")
}

func currentKeyword(action diff.Action) (diff.Keyword, error) {
	return decodeActionValue[diff.Keyword](action.Current, "current keyword")
}

func desiredKeyword(action diff.Action) (diff.Keyword, error) {
	return decodeActionValue[diff.Keyword](action.Desired, "desired keyword")
}

func currentCustomAd(action diff.Action) (diff.CustomAd, error) {
	return decodeActionValue[diff.CustomAd](action.Current, "current custom ad")
}

func desiredCustomAd(action diff.Action) (diff.CustomAd, error) {
	return decodeActionValue[diff.CustomAd](action.Desired, "desired custom ad")
}

func currentNegativeKeyword(action diff.Action) (diff.NegativeKeyword, error) {
	return decodeActionValue[diff.NegativeKeyword](action.Current, "current negative keyword")
}

func desiredNegativeKeyword(action diff.Action) (diff.NegativeKeyword, error) {
	return decodeActionValue[diff.NegativeKeyword](action.Desired, "desired negative keyword")
}

func decodeActionValue[T any](value any, label string) (T, error) {
	var zero T
	if value == nil {
		return zero, fmt.Errorf("%s is missing", label)
	}
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	if _, ok := value.(map[string]any); !ok {
		return zero, fmt.Errorf("expected %s, got %T", label, value)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("encode %s: %w", label, err)
	}
	var decoded T
	if err := json.Unmarshal(data, &decoded); err != nil {
		return zero, fmt.Errorf("decode %s: %w", label, err)
	}
	return decoded, nil
}

func (s *applySession) resolveCampaignID(campaignName string) (string, error) {
	campaign, ok := s.campaigns[campaignLookupKey(campaignName)]
	if !ok || strings.TrimSpace(campaign.ID) == "" {
		return "", fmt.Errorf("campaign %q does not have a remote id", campaignName)
	}
	return campaign.ID, nil
}

func (s *applySession) resolveAdGroupIDs(campaignName, adGroupName string) (string, string, error) {
	campaignID, err := s.resolveCampaignID(campaignName)
	if err != nil {
		return "", "", err
	}
	adGroup, ok := s.adGroups[adGroupLookupKey(campaignName, adGroupName)]
	if !ok || strings.TrimSpace(adGroup.ID) == "" {
		return "", "", fmt.Errorf("adgroup %q in campaign %q does not have a remote id", adGroupName, campaignName)
	}
	return campaignID, adGroup.ID, nil
}

func (s *applySession) resolveAdGroupIDsByLookup(key string) (string, string, error) {
	adGroup, ok := s.adGroups[key]
	if !ok {
		return "", "", fmt.Errorf("adgroup %q is not known in apply session", key)
	}
	return s.resolveAdGroupIDs(adGroup.CampaignName, adGroup.Name)
}

func (s *applySession) negativeKeywordPath(negative diff.NegativeKeyword) (string, string, error) {
	campaignID, err := s.resolveCampaignID(negative.CampaignName)
	if err != nil {
		return "", "", err
	}
	if negative.Scope == diff.ScopeCampaign {
		key := string(negative.Scope) + "|" + campaignLookupKey(negative.CampaignName)
		return key, "/campaigns/" + campaignID + "/negativekeywords/bulk", nil
	}
	_, adGroupID, err := s.resolveAdGroupIDs(negative.CampaignName, negative.AdGroupName)
	if err != nil {
		return "", "", err
	}
	key := string(negative.Scope) + "|" + adGroupLookupKey(negative.CampaignName, negative.AdGroupName)
	return key, "/campaigns/" + campaignID + "/adgroups/" + adGroupID + "/negativekeywords/bulk", nil
}

func (s *applySession) negativeKeywordDeletePath(negative diff.NegativeKeyword) (string, error) {
	campaignID, err := s.resolveCampaignID(negative.CampaignName)
	if err != nil {
		return "", err
	}
	if negative.Scope == diff.ScopeCampaign {
		return "/campaigns/" + campaignID + "/negativekeywords/delete/bulk", nil
	}
	_, adGroupID, err := s.resolveAdGroupIDs(negative.CampaignName, negative.AdGroupName)
	if err != nil {
		return "", err
	}
	return "/campaigns/" + campaignID + "/adgroups/" + adGroupID + "/negativekeywords/delete/bulk", nil
}

func parseResourceIDInt64(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, errors.New("resource id is blank")
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}
