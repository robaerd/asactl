package diff

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/robaerd/asactl/internal/spec"
)

type ResourceKind string

type Operation string

const (
	ResourceCampaign        ResourceKind = "campaign"
	ResourceAdGroup         ResourceKind = "adgroup"
	ResourceKeyword         ResourceKind = "keyword"
	ResourceNegativeKeyword ResourceKind = "negative_keyword"
	ResourceCustomAd        ResourceKind = "custom_ad"

	OperationDelete   Operation = "delete"
	OperationCreate   Operation = "create"
	OperationUpdate   Operation = "update"
	OperationPause    Operation = "pause"
	OperationActivate Operation = "activate"
	OperationNoop     Operation = "noop"
)

type Campaign struct {
	ID          string       `json:"id,omitempty"`
	Name        string       `json:"name"`
	Storefronts []string     `json:"storefronts,omitempty"`
	DailyBudget spec.Decimal `json:"daily_budget"`
	Status      spec.Status  `json:"status"`
	context     actionContext
}

type AdGroup struct {
	ID            string         `json:"id,omitempty"`
	CampaignName  string         `json:"campaign_name"`
	Name          string         `json:"name"`
	DefaultCPTBid spec.Decimal   `json:"default_cpt_bid"`
	ProductPage   string         `json:"product_page,omitempty"`
	Targeting     spec.Targeting `json:"targeting"`
	Status        spec.Status    `json:"status"`
	context       actionContext
}

type Keyword struct {
	ID           string         `json:"id,omitempty"`
	CampaignName string         `json:"campaign_name"`
	AdGroupName  string         `json:"adgroup_name"`
	Text         string         `json:"text"`
	MatchType    spec.MatchType `json:"match_type"`
	Bid          spec.Decimal   `json:"bid"`
	Status       spec.Status    `json:"status"`
	context      actionContext
}

type NegativeKeywordScope string

const (
	ScopeCampaign NegativeKeywordScope = "campaign"
	ScopeAdGroup  NegativeKeywordScope = "adgroup"
)

type NegativeKeyword struct {
	ID           string               `json:"id,omitempty"`
	Scope        NegativeKeywordScope `json:"scope"`
	CampaignName string               `json:"campaign_name"`
	AdGroupName  string               `json:"adgroup_name,omitempty"`
	Text         string               `json:"text"`
	MatchType    spec.MatchType       `json:"match_type"`
	Status       spec.Status          `json:"status"`
	context      actionContext
}

type CustomAd struct {
	ID           string      `json:"id,omitempty"`
	CampaignName string      `json:"campaign_name"`
	AdGroupName  string      `json:"adgroup_name"`
	ProductPage  string      `json:"product_page"`
	Status       spec.Status `json:"status"`
	IsDefault    bool        `json:"is_default"`
	context      actionContext
}

type State struct {
	Campaigns        []Campaign        `json:"campaigns,omitempty"`
	AdGroups         []AdGroup         `json:"adgroups,omitempty"`
	Keywords         []Keyword         `json:"keywords,omitempty"`
	NegativeKeywords []NegativeKeyword `json:"negative_keywords,omitempty"`
	CustomAds        []CustomAd        `json:"custom_ads,omitempty"`
}

type Action struct {
	Operation    Operation     `json:"operation"`
	Kind         ResourceKind  `json:"kind"`
	Key          string        `json:"key"`
	Description  string        `json:"description"`
	SourcePath   string        `json:"source_path,omitempty"`
	CampaignName string        `json:"campaign_name,omitempty"`
	AdGroupName  string        `json:"adgroup_name,omitempty"`
	Current      any           `json:"current,omitempty"`
	Desired      any           `json:"desired,omitempty"`
	Changes      []FieldChange `json:"changes,omitempty"`
	context      actionContext `json:"-"`
}

type stateOrigin uint8

const (
	stateOriginDesired stateOrigin = iota
	stateOriginRemote
)

type actionContext struct {
	sourcePath    string
	sourceOrder   int
	campaignName  string
	campaignOrder int
	adGroupName   string
	origin        stateOrigin
}

type FieldChange struct {
	Field  string `json:"field"`
	Before any    `json:"before,omitempty"`
	After  any    `json:"after,omitempty"`
}

type Plan struct {
	Actions []Action `json:"actions"`
	Summary Summary  `json:"summary"`
}

type RecreateScope string

const (
	RecreateScopeNone    RecreateScope = ""
	RecreateScopeManaged RecreateScope = "managed"
	RecreateScopeOrg     RecreateScope = "org"
)

type PlanOptions struct {
	RecreateScope     RecreateScope
	RecreateCampaigns []Campaign
}

type Summary struct {
	Delete   int `json:"delete"`
	Create   int `json:"create"`
	Update   int `json:"update"`
	Pause    int `json:"pause"`
	Activate int `json:"activate"`
	Noop     int `json:"noop"`
	Total    int `json:"total"`
}

func BuildDesiredState(input spec.Spec) State {
	normalized := spec.Normalize(input)
	state := State{}
	for campaignIndex, campaign := range normalized.Campaigns {
		context := desiredActionContext(normalized.Meta, campaign.Name, campaignIndex)
		state.Campaigns = append(state.Campaigns, Campaign{
			Name:        campaign.Name,
			Storefronts: slices.Clone(campaign.Storefronts),
			DailyBudget: campaign.DailyBudget,
			Status:      campaign.Status,
			context:     context,
		})
		for _, negative := range campaign.CampaignNegativeKeywords {
			state.NegativeKeywords = append(state.NegativeKeywords, NegativeKeyword{
				Scope:        ScopeCampaign,
				CampaignName: campaign.Name,
				Text:         negative.Text,
				MatchType:    negative.MatchType,
				Status:       negative.Status,
				context:      negativeKeywordContext(context, negative),
			})
		}
		for _, adGroup := range campaign.AdGroups {
			adGroupContext := context
			adGroupContext.adGroupName = adGroup.Name
			state.AdGroups = append(state.AdGroups, AdGroup{
				CampaignName:  campaign.Name,
				Name:          adGroup.Name,
				DefaultCPTBid: adGroup.DefaultCPTBid,
				ProductPage:   adGroup.ProductPage,
				Targeting:     adGroup.Targeting,
				Status:        adGroup.Status,
				context:       adGroupContext,
			})
			for _, keyword := range adGroup.Keywords {
				state.Keywords = append(state.Keywords, Keyword{
					CampaignName: campaign.Name,
					AdGroupName:  adGroup.Name,
					Text:         keyword.Text,
					MatchType:    keyword.MatchType,
					Bid:          keyword.Bid,
					Status:       keyword.Status,
					context:      adGroupContext,
				})
			}
			for _, negative := range adGroup.AdGroupNegativeKeywords {
				state.NegativeKeywords = append(state.NegativeKeywords, NegativeKeyword{
					Scope:        ScopeAdGroup,
					CampaignName: campaign.Name,
					AdGroupName:  adGroup.Name,
					Text:         negative.Text,
					MatchType:    negative.MatchType,
					Status:       negative.Status,
					context:      negativeKeywordContext(adGroupContext, negative),
				})
			}
			if adGroup.ProductPage != "" {
				state.CustomAds = append(state.CustomAds, CustomAd{
					CampaignName: campaign.Name,
					AdGroupName:  adGroup.Name,
					ProductPage:  adGroup.ProductPage,
					Status:       spec.StatusActive,
					IsDefault:    false,
					context:      adGroupContext,
				})
			}
		}
	}
	sortState(&state)
	return state
}

func desiredActionContext(meta spec.Meta, campaignName string, campaignOrder int) actionContext {
	context := actionContext{
		campaignName:  campaignName,
		campaignOrder: campaignOrder,
		sourceOrder:   -1,
	}
	if source, ok := meta.CampaignSources[spec.Fold(campaignName)]; ok {
		context.sourcePath = source.SourcePath
		context.sourceOrder = source.SourceOrder
	}
	return context
}

func negativeKeywordContext(base actionContext, negative spec.NegativeKeyword) actionContext {
	if strings.TrimSpace(negative.SourcePath) == "" {
		return base
	}
	base.sourcePath = negative.SourcePath
	base.sourceOrder = negative.SourceOrder
	return base
}

func EnsureUnique(state State) error {
	var errs []error
	errs = append(errs, duplicateKeys(state.Campaigns, func(item Campaign) string { return campaignKey(item.Name) }, "campaign")...)
	errs = append(errs, duplicateKeys(state.AdGroups, func(item AdGroup) string { return adGroupKey(item.CampaignName, item.Name) }, "adgroup")...)
	errs = append(errs, duplicateKeys(state.Keywords, func(item Keyword) string {
		return keywordKey(item.CampaignName, item.AdGroupName, item.Text, item.MatchType)
	}, "keyword")...)
	errs = append(errs, duplicateKeys(state.NegativeKeywords, func(item NegativeKeyword) string { return negativeKey(item) }, "negative keyword")...)
	errs = append(errs, duplicateKeys(state.CustomAds, func(item CustomAd) string { return customAdKey(item.CampaignName, item.AdGroupName, item.ProductPage) }, "custom ad")...)
	return errors.Join(errs...)
}

func duplicateKeys[T any](items []T, keyFunc func(T) string, label string) []error {
	seen := map[string]struct{}{}
	var errs []error
	for _, item := range items {
		key := keyFunc(item)
		if _, ok := seen[key]; ok {
			errs = append(errs, fmt.Errorf("duplicate %s key %q in state", label, key))
			continue
		}
		seen[key] = struct{}{}
	}
	return errs
}

func BuildPlan(desired, remote State) Plan {
	return BuildPlanWithOptions(desired, remote, PlanOptions{})
}

func BuildPlanWithOptions(desired, remote State, options PlanOptions) Plan {
	MarkRemoteState(&remote)
	markedRecreateCampaigns := markRemoteCampaigns(options.RecreateCampaigns)
	sortState(&desired)
	sortState(&remote)

	switch options.RecreateScope {
	case RecreateScopeManaged, RecreateScopeOrg:
		return buildRecreatePlan(desired, recreateCampaigns(remote.Campaigns, markedRecreateCampaigns))
	}

	plan := Plan{}
	plan.Actions = append(plan.Actions, diffCampaigns(desired.Campaigns, remote.Campaigns, &plan.Summary)...)
	plan.Actions = append(plan.Actions, diffAdGroups(desired.AdGroups, remote.AdGroups, &plan.Summary)...)
	plan.Actions = append(plan.Actions, diffKeywords(desired.Keywords, remote.Keywords, &plan.Summary)...)
	plan.Actions = append(plan.Actions, diffNegatives(desired.NegativeKeywords, remote.NegativeKeywords, &plan.Summary)...)
	plan.Actions = append(plan.Actions, diffCustomAds(desired.CustomAds, remote.CustomAds, &plan.Summary)...)
	plan.Actions = pruneActionsCoveredByCampaignDeletes(plan.Actions)
	plan.Actions = pruneActionsCoveredByAdGroupDeletes(plan.Actions)
	sortActions(plan.Actions)
	plan.Summary = summarizeActions(plan.Actions)
	return plan
}

func buildRecreatePlan(desired State, remoteCampaigns []Campaign) Plan {
	plan := Plan{}
	for _, campaign := range remoteCampaigns {
		plan.Actions = append(plan.Actions, newAction(
			OperationDelete,
			ResourceCampaign,
			campaignKey(campaign.Name),
			fmt.Sprintf("%q", campaign.Name),
			campaign,
			nil,
			nil,
		))
		plan.Summary.Delete++
	}

	createOnly := BuildPlanWithOptions(desired, State{}, PlanOptions{})
	plan.Actions = append(plan.Actions, createOnly.Actions...)
	plan.Summary.Create += createOnly.Summary.Create
	plan.Summary.Update += createOnly.Summary.Update
	plan.Summary.Pause += createOnly.Summary.Pause
	plan.Summary.Activate += createOnly.Summary.Activate
	plan.Summary.Noop += createOnly.Summary.Noop
	sortActions(plan.Actions)
	plan.Summary.Total = len(plan.Actions)
	return plan
}

func recreateCampaigns(remote, explicit []Campaign) []Campaign {
	if len(explicit) == 0 {
		return remote
	}
	return explicit
}

func pruneActionsCoveredByCampaignDeletes(actions []Action) []Action {
	deletedCampaigns := map[string]struct{}{}
	for _, action := range actions {
		if action.Kind != ResourceCampaign || action.Operation != OperationDelete {
			continue
		}
		if campaign, ok := action.Current.(Campaign); ok {
			deletedCampaigns[campaignKey(campaign.Name)] = struct{}{}
		}
	}
	if len(deletedCampaigns) == 0 {
		return actions
	}

	filtered := make([]Action, 0, len(actions))
	for _, action := range actions {
		if action.Kind != ResourceCampaign {
			if campaignKey, ok := actionCampaignKey(action); ok {
				if _, deleted := deletedCampaigns[campaignKey]; deleted {
					continue
				}
			}
		}
		filtered = append(filtered, action)
	}
	return filtered
}

func pruneActionsCoveredByAdGroupDeletes(actions []Action) []Action {
	deletedAdGroups := map[string]struct{}{}
	for _, action := range actions {
		if action.Kind != ResourceAdGroup || action.Operation != OperationDelete {
			continue
		}
		if key, ok := actionAdGroupKey(action); ok {
			deletedAdGroups[key] = struct{}{}
		}
	}
	if len(deletedAdGroups) == 0 {
		return actions
	}

	filtered := make([]Action, 0, len(actions))
	for _, action := range actions {
		if action.Operation != OperationDelete {
			filtered = append(filtered, action)
			continue
		}
		switch action.Kind {
		case ResourceKeyword, ResourceCustomAd:
			if key, ok := actionAdGroupKey(action); ok {
				if _, deleted := deletedAdGroups[key]; deleted {
					continue
				}
			}
		case ResourceNegativeKeyword:
			if key, ok := actionAdGroupKey(action); ok {
				if _, deleted := deletedAdGroups[key]; deleted {
					continue
				}
			}
		}
		filtered = append(filtered, action)
	}
	return filtered
}

func actionCampaignKey(action Action) (string, bool) {
	if campaign, ok := action.Current.(Campaign); ok {
		return campaignKey(campaign.Name), true
	}
	if campaign, ok := action.Desired.(Campaign); ok {
		return campaignKey(campaign.Name), true
	}

	switch current := action.Current.(type) {
	case AdGroup:
		return campaignKey(current.CampaignName), true
	case Keyword:
		return campaignKey(current.CampaignName), true
	case NegativeKeyword:
		return campaignKey(current.CampaignName), true
	case CustomAd:
		return campaignKey(current.CampaignName), true
	}

	switch desired := action.Desired.(type) {
	case AdGroup:
		return campaignKey(desired.CampaignName), true
	case Keyword:
		return campaignKey(desired.CampaignName), true
	case NegativeKeyword:
		return campaignKey(desired.CampaignName), true
	case CustomAd:
		return campaignKey(desired.CampaignName), true
	}

	return "", false
}

func actionAdGroupKey(action Action) (string, bool) {
	switch current := action.Current.(type) {
	case AdGroup:
		return adGroupKey(current.CampaignName, current.Name), true
	case Keyword:
		return adGroupKey(current.CampaignName, current.AdGroupName), true
	case NegativeKeyword:
		if current.Scope == ScopeAdGroup {
			return adGroupKey(current.CampaignName, current.AdGroupName), true
		}
	case CustomAd:
		return adGroupKey(current.CampaignName, current.AdGroupName), true
	}

	switch desired := action.Desired.(type) {
	case AdGroup:
		return adGroupKey(desired.CampaignName, desired.Name), true
	case Keyword:
		return adGroupKey(desired.CampaignName, desired.AdGroupName), true
	case NegativeKeyword:
		if desired.Scope == ScopeAdGroup {
			return adGroupKey(desired.CampaignName, desired.AdGroupName), true
		}
	case CustomAd:
		return adGroupKey(desired.CampaignName, desired.AdGroupName), true
	}

	return "", false
}

func summarizeActions(actions []Action) Summary {
	summary := Summary{}
	for _, action := range actions {
		switch action.Operation {
		case OperationDelete:
			summary.Delete++
		case OperationCreate:
			summary.Create++
		case OperationUpdate:
			summary.Update++
		case OperationPause:
			summary.Pause++
		case OperationActivate:
			summary.Activate++
		case OperationNoop:
			summary.Noop++
		}
	}
	summary.Total = len(actions)
	return summary
}

func MutatingActionCount(plan Plan) int {
	count := 0
	for _, action := range plan.Actions {
		if action.Operation != OperationNoop {
			count++
		}
	}
	return count
}

func diffCampaigns(desired, remote []Campaign, summary *Summary) []Action {
	desiredIndex := map[string]Campaign{}
	remoteIndex := map[string]Campaign{}
	for _, item := range desired {
		desiredIndex[campaignKey(item.Name)] = item
	}
	for _, item := range remote {
		remoteIndex[campaignKey(item.Name)] = item
	}
	return diffMap(keys(desiredIndex, remoteIndex), summary, func(key string) Action {
		want, have := desiredIndex[key], remoteIndex[key]
		switch {
		case want.Name != "" && have.Name == "":
			return newAction(OperationCreate, ResourceCampaign, key, fmt.Sprintf("%q", want.Name), nil, want, nil)
		case want.Name == "" && have.Name != "":
			return newAction(OperationDelete, ResourceCampaign, key, fmt.Sprintf("%q", have.Name), have, nil, nil)
		default:
			changes := compareCampaign(want, have)
			if len(changes) == 0 {
				return newAction(OperationNoop, ResourceCampaign, key, fmt.Sprintf("%q unchanged", want.Name), have, want, nil)
			}
			return newAction(OperationUpdate, ResourceCampaign, key, fmt.Sprintf("%q", want.Name), have, want, changes)
		}
	})
}

func diffAdGroups(desired, remote []AdGroup, summary *Summary) []Action {
	desiredIndex := map[string]AdGroup{}
	remoteIndex := map[string]AdGroup{}
	for _, item := range desired {
		desiredIndex[adGroupKey(item.CampaignName, item.Name)] = item
	}
	for _, item := range remote {
		remoteIndex[adGroupKey(item.CampaignName, item.Name)] = item
	}
	return diffMap(keys(desiredIndex, remoteIndex), summary, func(key string) Action {
		want, have := desiredIndex[key], remoteIndex[key]
		switch {
		case want.Name != "" && have.Name == "":
			return newAction(OperationCreate, ResourceAdGroup, key, fmt.Sprintf("%q", want.Name), nil, want, nil)
		case want.Name == "" && have.Name != "":
			return newAction(OperationDelete, ResourceAdGroup, key, fmt.Sprintf("%q", have.Name), have, nil, nil)
		default:
			changes := compareAdGroup(want, have)
			if len(changes) == 0 {
				return newAction(OperationNoop, ResourceAdGroup, key, fmt.Sprintf("%q unchanged", want.Name), have, want, nil)
			}
			return newAction(OperationUpdate, ResourceAdGroup, key, fmt.Sprintf("%q", want.Name), have, want, changes)
		}
	})
}

func diffKeywords(desired, remote []Keyword, summary *Summary) []Action {
	desiredIndex := map[string]Keyword{}
	remoteIndex := map[string]Keyword{}
	for _, item := range desired {
		desiredIndex[keywordKey(item.CampaignName, item.AdGroupName, item.Text, item.MatchType)] = item
	}
	for _, item := range remote {
		remoteIndex[keywordKey(item.CampaignName, item.AdGroupName, item.Text, item.MatchType)] = item
	}
	return diffMap(keys(desiredIndex, remoteIndex), summary, func(key string) Action {
		want, have := desiredIndex[key], remoteIndex[key]
		switch {
		case want.Text != "" && have.Text == "":
			return newAction(OperationCreate, ResourceKeyword, key, fmt.Sprintf("%q (%s) in adgroup %q", want.Text, want.MatchType, want.AdGroupName), nil, want, nil)
		case want.Text == "" && have.Text != "":
			return newAction(OperationDelete, ResourceKeyword, key, fmt.Sprintf("%q (%s) in adgroup %q", have.Text, have.MatchType, have.AdGroupName), have, nil, nil)
		default:
			changes := compareKeyword(want, have)
			if len(changes) == 0 {
				return newAction(OperationNoop, ResourceKeyword, key, fmt.Sprintf("%q (%s) in adgroup %q unchanged", want.Text, want.MatchType, want.AdGroupName), have, want, nil)
			}
			return newAction(OperationUpdate, ResourceKeyword, key, fmt.Sprintf("%q (%s) in adgroup %q", want.Text, want.MatchType, want.AdGroupName), have, want, changes)
		}
	})
}

func diffNegatives(desired, remote []NegativeKeyword, summary *Summary) []Action {
	desiredIndex := map[string]NegativeKeyword{}
	remoteIndex := map[string]NegativeKeyword{}
	for _, item := range desired {
		desiredIndex[negativeKey(item)] = item
	}
	for _, item := range remote {
		remoteIndex[negativeKey(item)] = item
	}
	return diffMap(keys(desiredIndex, remoteIndex), summary, func(key string) Action {
		want, have := desiredIndex[key], remoteIndex[key]
		switch {
		case want.Text != "" && have.Text == "":
			return newAction(OperationCreate, ResourceNegativeKeyword, key, negativeKeywordDescription(want), nil, want, nil)
		case want.Text == "" && have.Text != "":
			return newAction(OperationDelete, ResourceNegativeKeyword, key, negativeKeywordDescription(have), have, nil, nil)
		default:
			changes := compareNegative(want, have)
			if len(changes) == 0 {
				return newAction(OperationNoop, ResourceNegativeKeyword, key, negativeKeywordDescription(want)+" unchanged", have, want, nil)
			}
			return newAction(OperationUpdate, ResourceNegativeKeyword, key, negativeKeywordDescription(want), have, want, changes)
		}
	})
}

func diffCustomAds(desired, remote []CustomAd, summary *Summary) []Action {
	desiredIndex := map[string]CustomAd{}
	remoteIndex := map[string]CustomAd{}
	for _, item := range desired {
		desiredIndex[customAdKey(item.CampaignName, item.AdGroupName, item.ProductPage)] = item
	}
	for _, item := range remote {
		if item.IsDefault {
			continue
		}
		remoteIndex[customAdKey(item.CampaignName, item.AdGroupName, item.ProductPage)] = item
	}
	actions := diffMap(keys(desiredIndex, remoteIndex), summary, func(key string) Action {
		want, have := desiredIndex[key], remoteIndex[key]
		switch {
		case want.ProductPage != "" && have.ProductPage == "":
			return newAction(OperationCreate, ResourceCustomAd, key, fmt.Sprintf("product_page %q in adgroup %q", want.ProductPage, want.AdGroupName), nil, want, nil)
		case want.ProductPage == "" && have.ProductPage != "":
			return newAction(OperationDelete, ResourceCustomAd, key, fmt.Sprintf("product_page %q in adgroup %q", have.ProductPage, have.AdGroupName), have, nil, nil)
		default:
			if have.Status != want.Status {
				op := OperationUpdate
				description := fmt.Sprintf("product_page %q in adgroup %q", want.ProductPage, want.AdGroupName)
				if want.Status == spec.StatusActive {
					op = OperationActivate
				} else if want.Status == spec.StatusPaused {
					op = OperationPause
				}
				return newAction(op, ResourceCustomAd, key, description, have, want, []FieldChange{{Field: "status", Before: have.Status, After: want.Status}})
			}
			return newAction(OperationNoop, ResourceCustomAd, key, fmt.Sprintf("product_page %q in adgroup %q unchanged", want.ProductPage, want.AdGroupName), have, want, nil)
		}
	})
	return actions
}

func newAction(op Operation, kind ResourceKind, key, description string, current, desired any, changes []FieldChange) Action {
	context := resolveActionContext(current, desired)
	return Action{
		Operation:    op,
		Kind:         kind,
		Key:          key,
		Description:  description,
		SourcePath:   context.sourcePath,
		CampaignName: context.campaignName,
		AdGroupName:  context.adGroupName,
		Current:      current,
		Desired:      desired,
		Changes:      changes,
		context:      context,
	}
}

func negativeKeywordDescription(keyword NegativeKeyword) string {
	if keyword.Scope == ScopeAdGroup {
		return fmt.Sprintf("%q (%s) in adgroup %q", keyword.Text, keyword.MatchType, keyword.AdGroupName)
	}
	return fmt.Sprintf("%q (%s) in campaign scope", keyword.Text, keyword.MatchType)
}

func resolveActionContext(current, desired any) actionContext {
	if context, ok := desiredContextFromValue(desired); ok {
		return context
	}
	if context, ok := remoteContextFromValue(current); ok {
		return context
	}
	return actionContext{sourceOrder: -1, campaignOrder: -1}
}

func actionContextFromValue(value any) (actionContext, bool) {
	switch item := value.(type) {
	case Campaign:
		context := item.context
		context.campaignName = item.Name
		return context, true
	case AdGroup:
		context := item.context
		context.campaignName = item.CampaignName
		context.adGroupName = item.Name
		return context, true
	case Keyword:
		context := item.context
		context.campaignName = item.CampaignName
		context.adGroupName = item.AdGroupName
		return context, true
	case NegativeKeyword:
		context := item.context
		context.campaignName = item.CampaignName
		context.adGroupName = item.AdGroupName
		return context, true
	case CustomAd:
		context := item.context
		context.campaignName = item.CampaignName
		context.adGroupName = item.AdGroupName
		return context, true
	default:
		return actionContext{}, false
	}
}

func desiredContextFromValue(value any) (actionContext, bool) {
	context, ok := actionContextFromValue(value)
	if !ok || context.isRemoteState() {
		return actionContext{}, false
	}
	return context, true
}

func remoteContextFromValue(value any) (actionContext, bool) {
	context, ok := actionContextFromValue(value)
	if !ok || !context.isRemoteState() {
		return actionContext{}, false
	}
	return context, true
}

func (context actionContext) isRemoteState() bool {
	return context.origin == stateOriginRemote
}

func (context actionContext) markedRemoteState() actionContext {
	context.sourcePath = ""
	context.sourceOrder = -1
	context.campaignOrder = -1
	context.origin = stateOriginRemote
	return context
}

// MarkRemoteState annotates fetched state with an explicit remote-state marker so delete and recreate actions render as remote-only.
func MarkRemoteState(state *State) {
	if state == nil {
		return
	}
	for index := range state.Campaigns {
		state.Campaigns[index].context = state.Campaigns[index].context.markedRemoteState()
	}
	for index := range state.AdGroups {
		state.AdGroups[index].context = state.AdGroups[index].context.markedRemoteState()
	}
	for index := range state.Keywords {
		state.Keywords[index].context = state.Keywords[index].context.markedRemoteState()
	}
	for index := range state.NegativeKeywords {
		state.NegativeKeywords[index].context = state.NegativeKeywords[index].context.markedRemoteState()
	}
	for index := range state.CustomAds {
		state.CustomAds[index].context = state.CustomAds[index].context.markedRemoteState()
	}
}

func markRemoteCampaigns(campaigns []Campaign) []Campaign {
	marked := slices.Clone(campaigns)
	for index := range marked {
		marked[index].context = marked[index].context.markedRemoteState()
	}
	return marked
}

func diffMap(allKeys []string, summary *Summary, builder func(string) Action) []Action {
	actions := make([]Action, 0, len(allKeys))
	for _, key := range allKeys {
		action := builder(key)
		switch action.Operation {
		case OperationDelete:
			summary.Delete++
		case OperationCreate:
			summary.Create++
		case OperationUpdate:
			summary.Update++
		case OperationPause:
			summary.Pause++
		case OperationActivate:
			summary.Activate++
		case OperationNoop:
			summary.Noop++
		}
		actions = append(actions, action)
	}
	return actions
}

func sortActions(actions []Action) {
	slices.SortStableFunc(actions, func(left, right Action) int {
		if left.Kind != right.Kind {
			return kindRank(left.Kind) - kindRank(right.Kind)
		}
		if left.Operation != right.Operation {
			return operationRank(left.Operation) - operationRank(right.Operation)
		}
		return strings.Compare(left.Key, right.Key)
	})
}

func operationRank(operation Operation) int {
	switch operation {
	case OperationDelete:
		return 0
	case OperationCreate:
		return 1
	case OperationUpdate:
		return 2
	case OperationPause:
		return 3
	case OperationActivate:
		return 4
	case OperationNoop:
		return 5
	default:
		return 6
	}
}

func kindRank(kind ResourceKind) int {
	switch kind {
	case ResourceCampaign:
		return 0
	case ResourceAdGroup:
		return 1
	case ResourceKeyword:
		return 2
	case ResourceNegativeKeyword:
		return 3
	case ResourceCustomAd:
		return 4
	default:
		return 5
	}
}

func keys[T any](left, right map[string]T) []string {
	merged := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		merged[key] = struct{}{}
	}
	for key := range right {
		merged[key] = struct{}{}
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func compareCampaign(want, have Campaign) []FieldChange {
	changes := []FieldChange{}
	if !sameStorefronts(want.Storefronts, have.Storefronts) {
		changes = append(changes, FieldChange{Field: "storefronts", Before: have.Storefronts, After: want.Storefronts})
	}
	if !want.DailyBudget.Equal(have.DailyBudget.Decimal) {
		changes = append(changes, FieldChange{Field: "daily_budget", Before: have.DailyBudget, After: want.DailyBudget})
	}
	if want.Status != have.Status {
		changes = append(changes, FieldChange{Field: "status", Before: have.Status, After: want.Status})
	}
	return changes
}

func compareAdGroup(want, have AdGroup) []FieldChange {
	changes := []FieldChange{}
	if !want.DefaultCPTBid.Equal(have.DefaultCPTBid.Decimal) {
		changes = append(changes, FieldChange{Field: "default_cpt_bid", Before: have.DefaultCPTBid, After: want.DefaultCPTBid})
	}
	// ProductPage changes are intentionally managed through custom-ad diffing.
	// Apple exposes CPPs as ads, and the adgroup payload does not reliably carry the managed custom product page.
	if want.Targeting != have.Targeting {
		changes = append(changes, FieldChange{Field: "targeting", Before: have.Targeting, After: want.Targeting})
	}
	if want.Status != have.Status {
		changes = append(changes, FieldChange{Field: "status", Before: have.Status, After: want.Status})
	}
	return changes
}

func compareKeyword(want, have Keyword) []FieldChange {
	changes := []FieldChange{}
	if !want.Bid.Equal(have.Bid.Decimal) {
		changes = append(changes, FieldChange{Field: "bid", Before: have.Bid, After: want.Bid})
	}
	if want.Status != have.Status {
		changes = append(changes, FieldChange{Field: "status", Before: have.Status, After: want.Status})
	}
	return changes
}

func compareNegative(want, have NegativeKeyword) []FieldChange {
	changes := []FieldChange{}
	if want.Status != have.Status {
		changes = append(changes, FieldChange{Field: "status", Before: have.Status, After: want.Status})
	}
	return changes
}

func sameStorefronts(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := slices.Clone(left)
	rightCopy := slices.Clone(right)
	slices.SortFunc(leftCopy, func(leftValue, rightValue string) int {
		return strings.Compare(spec.Fold(leftValue), spec.Fold(rightValue))
	})
	slices.SortFunc(rightCopy, func(leftValue, rightValue string) int {
		return strings.Compare(spec.Fold(leftValue), spec.Fold(rightValue))
	})
	for index := range leftCopy {
		if spec.Fold(leftCopy[index]) != spec.Fold(rightCopy[index]) {
			return false
		}
	}
	return true
}

func campaignKey(name string) string {
	return spec.Fold(name)
}

func adGroupKey(campaignName, adGroupName string) string {
	return spec.Fold(campaignName) + "|" + spec.Fold(adGroupName)
}

func keywordKey(campaignName, adGroupName, text string, matchType spec.MatchType) string {
	return adGroupKey(campaignName, adGroupName) + "|" + spec.Fold(text) + "|" + string(matchType)
}

func negativeKey(item NegativeKeyword) string {
	if item.Scope == ScopeAdGroup {
		return string(item.Scope) + "|" + adGroupKey(item.CampaignName, item.AdGroupName) + "|" + spec.Fold(item.Text) + "|" + string(item.MatchType)
	}
	return string(item.Scope) + "|" + campaignKey(item.CampaignName) + "|" + spec.Fold(item.Text) + "|" + string(item.MatchType)
}

func customAdKey(campaignName, adGroupName, productPage string) string {
	return adGroupKey(campaignName, adGroupName) + "|" + spec.Fold(productPage)
}

func sortState(state *State) {
	slices.SortStableFunc(state.Campaigns, func(left, right Campaign) int {
		return strings.Compare(campaignKey(left.Name), campaignKey(right.Name))
	})
	slices.SortStableFunc(state.AdGroups, func(left, right AdGroup) int {
		return strings.Compare(adGroupKey(left.CampaignName, left.Name), adGroupKey(right.CampaignName, right.Name))
	})
	slices.SortStableFunc(state.Keywords, func(left, right Keyword) int {
		return strings.Compare(keywordKey(left.CampaignName, left.AdGroupName, left.Text, left.MatchType), keywordKey(right.CampaignName, right.AdGroupName, right.Text, right.MatchType))
	})
	slices.SortStableFunc(state.NegativeKeywords, func(left, right NegativeKeyword) int {
		return strings.Compare(negativeKey(left), negativeKey(right))
	})
	slices.SortStableFunc(state.CustomAds, func(left, right CustomAd) int {
		return strings.Compare(customAdKey(left.CampaignName, left.AdGroupName, left.ProductPage), customAdKey(right.CampaignName, right.AdGroupName, right.ProductPage))
	})
}
