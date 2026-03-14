package sync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
)

const (
	SavedPlanKind    = "SavedPlan"
	SavedPlanVersion = 1
)

type SavedPlan struct {
	Kind              string                      `json:"kind"`
	Version           int                         `json:"version"`
	Profile           string                      `json:"profile,omitempty"`
	SpecYAML          string                      `json:"spec_yaml"`
	RecreateScope     diff.RecreateScope          `json:"recreate_scope,omitempty"`
	Plan              diff.Plan                   `json:"plan"`
	ActionRenderMeta  []diff.ActionRenderMetadata `json:"action_render_metadata,omitempty"`
	Warnings          []string                    `json:"warnings,omitempty"`
	ScopeSummary      appleadsapi.ScopeSummary    `json:"scope_summary"`
	ManagedCampaigns  []string                    `json:"managed_campaigns,omitempty"`
	OtherAppCampaigns []string                    `json:"other_app_campaigns,omitempty"`
}

func ParseSavedPlan(data []byte) (SavedPlan, bool, error) {
	var header struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return SavedPlan{}, false, nil
	}
	if strings.TrimSpace(header.Kind) != SavedPlanKind {
		return SavedPlan{}, false, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var saved SavedPlan
	if err := decoder.Decode(&saved); err != nil {
		return SavedPlan{}, true, fmt.Errorf("decode saved plan: %w", err)
	}
	if err := ensureSingleJSONValue(decoder); err != nil {
		return SavedPlan{}, true, err
	}
	if err := saved.Validate(); err != nil {
		return SavedPlan{}, true, err
	}
	return saved, true, nil
}

func (plan SavedPlan) Bytes() ([]byte, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(plan, "", "  ")
}

func (plan SavedPlan) Validate() error {
	if strings.TrimSpace(plan.Kind) != SavedPlanKind {
		return fmt.Errorf("saved plan kind must be %q", SavedPlanKind)
	}
	if plan.Version != SavedPlanVersion {
		return fmt.Errorf("saved plan version must be %d, got %d", SavedPlanVersion, plan.Version)
	}
	if strings.TrimSpace(plan.SpecYAML) == "" {
		return errors.New("saved plan spec_yaml must not be blank")
	}
	input, err := plan.ResolvedSpec()
	if err != nil {
		return err
	}
	if len(plan.ActionRenderMeta) != 0 && len(plan.ActionRenderMeta) != len(plan.Plan.Actions) {
		return fmt.Errorf("saved plan action_render_metadata length %d does not match action count %d", len(plan.ActionRenderMeta), len(plan.Plan.Actions))
	}
	if err := validateSavedPlanPlan(input, plan.Plan); err != nil {
		return err
	}
	return nil
}

func (plan SavedPlan) ResolvedSpec() (spec.Spec, error) {
	loaded, err := spec.LoadSource("saved-plan", []byte(plan.SpecYAML), "")
	if err != nil {
		return spec.Spec{}, fmt.Errorf("load saved plan spec: %w", err)
	}
	return loaded, nil
}

func (plan SavedPlan) Result() Result {
	clonedPlan := plan.Plan
	clonedPlan.Actions = slices.Clone(plan.Plan.Actions)
	diff.ApplyActionRenderMetadata(&clonedPlan, plan.ActionRenderMeta)
	return Result{
		Plan:              clonedPlan,
		Warnings:          slices.Clone(plan.Warnings),
		ScopeSummary:      plan.ScopeSummary,
		ManagedCampaigns:  slices.Clone(plan.ManagedCampaigns),
		OtherAppCampaigns: slices.Clone(plan.OtherAppCampaigns),
	}
}

func newSavedPlan(runtime userconfig.Runtime, result Result, options Options) (SavedPlan, error) {
	content, err := spec.Format(runtime.Spec)
	if err != nil {
		return SavedPlan{}, fmt.Errorf("encode saved plan spec: %w", err)
	}

	saved := SavedPlan{
		Kind:              SavedPlanKind,
		Version:           SavedPlanVersion,
		Profile:           strings.TrimSpace(runtime.ProfileName),
		SpecYAML:          string(content),
		RecreateScope:     options.RecreateScope,
		Plan:              result.Plan,
		ActionRenderMeta:  diff.ExtractActionRenderMetadata(result.Plan),
		Warnings:          slices.Clone(result.Warnings),
		ScopeSummary:      result.ScopeSummary,
		ManagedCampaigns:  slices.Clone(result.ManagedCampaigns),
		OtherAppCampaigns: slices.Clone(result.OtherAppCampaigns),
	}
	if err := saved.Validate(); err != nil {
		return SavedPlan{}, err
	}
	return saved, nil
}

func ensureSingleJSONValue(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("decode saved plan: expected a single JSON document")
		}
		return fmt.Errorf("decode saved plan: %w", err)
	}
	return nil
}

type savedPlanDesiredIndex struct {
	campaigns map[string]diff.Campaign
	adGroups  map[string]diff.AdGroup
	keywords  map[string]diff.Keyword
	negatives map[string]diff.NegativeKeyword
	customAds map[string]diff.CustomAd
}

type savedPlanDependencies struct {
	campaigns map[string]struct{}
	adGroups  map[string]struct{}
}

func validateSavedPlanPlan(input spec.Spec, plan diff.Plan) error {
	desired := buildSavedPlanDesiredIndex(input)
	deps, err := collectSavedPlanDependencies(plan.Actions)
	if err != nil {
		return err
	}
	for _, action := range plan.Actions {
		if err := validateSavedPlanAction(action, desired, deps); err != nil {
			return err
		}
	}
	return nil
}

func buildSavedPlanDesiredIndex(input spec.Spec) savedPlanDesiredIndex {
	state := diff.BuildDesiredState(input)
	index := savedPlanDesiredIndex{
		campaigns: map[string]diff.Campaign{},
		adGroups:  map[string]diff.AdGroup{},
		keywords:  map[string]diff.Keyword{},
		negatives: map[string]diff.NegativeKeyword{},
		customAds: map[string]diff.CustomAd{},
	}
	for _, campaign := range state.Campaigns {
		index.campaigns[savedPlanCampaignKey(campaign.Name)] = campaign
	}
	for _, adGroup := range state.AdGroups {
		index.adGroups[savedPlanAdGroupKey(adGroup.CampaignName, adGroup.Name)] = adGroup
	}
	for _, keyword := range state.Keywords {
		index.keywords[savedPlanKeywordKey(keyword.CampaignName, keyword.AdGroupName, keyword.Text, keyword.MatchType)] = keyword
	}
	for _, negative := range state.NegativeKeywords {
		index.negatives[savedPlanNegativeKey(negative)] = negative
	}
	for _, customAd := range state.CustomAds {
		index.customAds[savedPlanCustomAdKey(customAd.CampaignName, customAd.AdGroupName, customAd.ProductPage)] = customAd
	}
	return index
}

func collectSavedPlanDependencies(actions []diff.Action) (savedPlanDependencies, error) {
	deps := savedPlanDependencies{
		campaigns: map[string]struct{}{},
		adGroups:  map[string]struct{}{},
	}
	for _, action := range actions {
		switch action.Kind {
		case diff.ResourceCampaign:
			if action.Operation == diff.OperationCreate {
				desired, err := decodeSavedActionValue[diff.Campaign](action.Desired, "desired campaign")
				if err != nil {
					return savedPlanDependencies{}, fmt.Errorf("saved plan action %q: %w", action.Key, err)
				}
				deps.campaigns[savedPlanCampaignKey(desired.Name)] = struct{}{}
				continue
			}
			current, err := decodeSavedActionValue[diff.Campaign](action.Current, "current campaign")
			if err != nil {
				return savedPlanDependencies{}, fmt.Errorf("saved plan action %q: %w", action.Key, err)
			}
			if strings.TrimSpace(current.ID) == "" {
				return savedPlanDependencies{}, fmt.Errorf("saved plan action %q current campaign is missing remote id", action.Key)
			}
			deps.campaigns[savedPlanCampaignKey(current.Name)] = struct{}{}
		case diff.ResourceAdGroup:
			if action.Operation == diff.OperationCreate {
				desired, err := decodeSavedActionValue[diff.AdGroup](action.Desired, "desired adgroup")
				if err != nil {
					return savedPlanDependencies{}, fmt.Errorf("saved plan action %q: %w", action.Key, err)
				}
				deps.adGroups[savedPlanAdGroupKey(desired.CampaignName, desired.Name)] = struct{}{}
				continue
			}
			current, err := decodeSavedActionValue[diff.AdGroup](action.Current, "current adgroup")
			if err != nil {
				return savedPlanDependencies{}, fmt.Errorf("saved plan action %q: %w", action.Key, err)
			}
			if strings.TrimSpace(current.ID) == "" {
				return savedPlanDependencies{}, fmt.Errorf("saved plan action %q current adgroup is missing remote id", action.Key)
			}
			deps.adGroups[savedPlanAdGroupKey(current.CampaignName, current.Name)] = struct{}{}
		}
	}
	return deps, nil
}

func validateSavedPlanAction(action diff.Action, desired savedPlanDesiredIndex, deps savedPlanDependencies) error {
	switch action.Kind {
	case diff.ResourceCampaign:
		return validateSavedCampaignAction(action, desired)
	case diff.ResourceAdGroup:
		return validateSavedAdGroupAction(action, desired, deps)
	case diff.ResourceKeyword:
		return validateSavedKeywordAction(action, desired, deps)
	case diff.ResourceNegativeKeyword:
		return validateSavedNegativeKeywordAction(action, desired, deps)
	case diff.ResourceCustomAd:
		return validateSavedCustomAdAction(action, desired, deps)
	default:
		return fmt.Errorf("saved plan action %q has unsupported resource kind %q", action.Key, action.Kind)
	}
}

func validateSavedCampaignAction(action diff.Action, desired savedPlanDesiredIndex) error {
	switch action.Operation {
	case diff.OperationCreate:
		desiredCampaign, err := decodeSavedActionValue[diff.Campaign](action.Desired, "desired campaign")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		expected, ok := desired.campaigns[action.Key]
		return validateSavedDesiredMatch(action, desiredCampaign, expected, ok, savedPlanCampaignKey(desiredCampaign.Name))
	case diff.OperationUpdate, diff.OperationPause, diff.OperationActivate, diff.OperationNoop:
		currentCampaign, err := decodeSavedActionValue[diff.Campaign](action.Current, "current campaign")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentCampaign.ID) == "" {
			return fmt.Errorf("saved plan action %q current campaign is missing remote id", action.Key)
		}
		if got := savedPlanCampaignKey(currentCampaign.Name); got != action.Key {
			return fmt.Errorf("saved plan action %q current campaign key mismatch %q", action.Key, got)
		}
		desiredCampaign, err := decodeSavedActionValue[diff.Campaign](action.Desired, "desired campaign")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		expected, ok := desired.campaigns[action.Key]
		return validateSavedDesiredMatch(action, desiredCampaign, expected, ok, savedPlanCampaignKey(desiredCampaign.Name))
	case diff.OperationDelete:
		currentCampaign, err := decodeSavedActionValue[diff.Campaign](action.Current, "current campaign")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentCampaign.ID) == "" {
			return fmt.Errorf("saved plan action %q current campaign is missing remote id", action.Key)
		}
		if got := savedPlanCampaignKey(currentCampaign.Name); got != action.Key {
			return fmt.Errorf("saved plan action %q current campaign key mismatch %q", action.Key, got)
		}
		return nil
	default:
		return fmt.Errorf("saved plan action %q has unsupported campaign operation %q", action.Key, action.Operation)
	}
}

func validateSavedAdGroupAction(action diff.Action, desired savedPlanDesiredIndex, deps savedPlanDependencies) error {
	switch action.Operation {
	case diff.OperationCreate:
		desiredAdGroup, err := decodeSavedActionValue[diff.AdGroup](action.Desired, "desired adgroup")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if err := requireSavedCampaignDependency(deps, desiredAdGroup.CampaignName, action); err != nil {
			return err
		}
		expected, ok := desired.adGroups[action.Key]
		return validateSavedDesiredMatch(action, desiredAdGroup, expected, ok, savedPlanAdGroupKey(desiredAdGroup.CampaignName, desiredAdGroup.Name))
	case diff.OperationUpdate, diff.OperationPause, diff.OperationActivate, diff.OperationNoop:
		currentAdGroup, err := decodeSavedActionValue[diff.AdGroup](action.Current, "current adgroup")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentAdGroup.ID) == "" {
			return fmt.Errorf("saved plan action %q current adgroup is missing remote id", action.Key)
		}
		if err := requireSavedCampaignDependency(deps, currentAdGroup.CampaignName, action); err != nil {
			return err
		}
		if got := savedPlanAdGroupKey(currentAdGroup.CampaignName, currentAdGroup.Name); got != action.Key {
			return fmt.Errorf("saved plan action %q current adgroup key mismatch %q", action.Key, got)
		}
		desiredAdGroup, err := decodeSavedActionValue[diff.AdGroup](action.Desired, "desired adgroup")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		expected, ok := desired.adGroups[action.Key]
		return validateSavedDesiredMatch(action, desiredAdGroup, expected, ok, savedPlanAdGroupKey(desiredAdGroup.CampaignName, desiredAdGroup.Name))
	case diff.OperationDelete:
		currentAdGroup, err := decodeSavedActionValue[diff.AdGroup](action.Current, "current adgroup")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentAdGroup.ID) == "" {
			return fmt.Errorf("saved plan action %q current adgroup is missing remote id", action.Key)
		}
		if err := requireSavedCampaignDependency(deps, currentAdGroup.CampaignName, action); err != nil {
			return err
		}
		if got := savedPlanAdGroupKey(currentAdGroup.CampaignName, currentAdGroup.Name); got != action.Key {
			return fmt.Errorf("saved plan action %q current adgroup key mismatch %q", action.Key, got)
		}
		return nil
	default:
		return fmt.Errorf("saved plan action %q has unsupported adgroup operation %q", action.Key, action.Operation)
	}
}

func validateSavedKeywordAction(action diff.Action, desired savedPlanDesiredIndex, deps savedPlanDependencies) error {
	switch action.Operation {
	case diff.OperationCreate:
		desiredKeyword, err := decodeSavedActionValue[diff.Keyword](action.Desired, "desired keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if err := requireSavedAdGroupDependency(deps, desiredKeyword.CampaignName, desiredKeyword.AdGroupName, action); err != nil {
			return err
		}
		expected, ok := desired.keywords[action.Key]
		return validateSavedDesiredMatch(action, desiredKeyword, expected, ok, savedPlanKeywordKey(desiredKeyword.CampaignName, desiredKeyword.AdGroupName, desiredKeyword.Text, desiredKeyword.MatchType))
	case diff.OperationUpdate, diff.OperationPause, diff.OperationActivate, diff.OperationNoop:
		currentKeyword, err := decodeSavedActionValue[diff.Keyword](action.Current, "current keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentKeyword.ID) == "" {
			return fmt.Errorf("saved plan action %q current keyword is missing remote id", action.Key)
		}
		if err := requireSavedAdGroupDependency(deps, currentKeyword.CampaignName, currentKeyword.AdGroupName, action); err != nil {
			return err
		}
		if got := savedPlanKeywordKey(currentKeyword.CampaignName, currentKeyword.AdGroupName, currentKeyword.Text, currentKeyword.MatchType); got != action.Key {
			return fmt.Errorf("saved plan action %q current keyword key mismatch %q", action.Key, got)
		}
		desiredKeyword, err := decodeSavedActionValue[diff.Keyword](action.Desired, "desired keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		expected, ok := desired.keywords[action.Key]
		return validateSavedDesiredMatch(action, desiredKeyword, expected, ok, savedPlanKeywordKey(desiredKeyword.CampaignName, desiredKeyword.AdGroupName, desiredKeyword.Text, desiredKeyword.MatchType))
	case diff.OperationDelete:
		currentKeyword, err := decodeSavedActionValue[diff.Keyword](action.Current, "current keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentKeyword.ID) == "" {
			return fmt.Errorf("saved plan action %q current keyword is missing remote id", action.Key)
		}
		if err := requireSavedAdGroupDependency(deps, currentKeyword.CampaignName, currentKeyword.AdGroupName, action); err != nil {
			return err
		}
		if got := savedPlanKeywordKey(currentKeyword.CampaignName, currentKeyword.AdGroupName, currentKeyword.Text, currentKeyword.MatchType); got != action.Key {
			return fmt.Errorf("saved plan action %q current keyword key mismatch %q", action.Key, got)
		}
		return nil
	default:
		return fmt.Errorf("saved plan action %q has unsupported keyword operation %q", action.Key, action.Operation)
	}
}

func validateSavedNegativeKeywordAction(action diff.Action, desired savedPlanDesiredIndex, deps savedPlanDependencies) error {
	switch action.Operation {
	case diff.OperationCreate:
		desiredNegative, err := decodeSavedActionValue[diff.NegativeKeyword](action.Desired, "desired negative keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if err := requireSavedNegativeParent(deps, desiredNegative, action); err != nil {
			return err
		}
		expected, ok := desired.negatives[action.Key]
		return validateSavedDesiredMatch(action, desiredNegative, expected, ok, savedPlanNegativeKey(desiredNegative))
	case diff.OperationUpdate, diff.OperationPause, diff.OperationActivate, diff.OperationNoop:
		currentNegative, err := decodeSavedActionValue[diff.NegativeKeyword](action.Current, "current negative keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentNegative.ID) == "" {
			return fmt.Errorf("saved plan action %q current negative keyword is missing remote id", action.Key)
		}
		if err := requireSavedNegativeParent(deps, currentNegative, action); err != nil {
			return err
		}
		if got := savedPlanNegativeKey(currentNegative); got != action.Key {
			return fmt.Errorf("saved plan action %q current negative keyword key mismatch %q", action.Key, got)
		}
		desiredNegative, err := decodeSavedActionValue[diff.NegativeKeyword](action.Desired, "desired negative keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		expected, ok := desired.negatives[action.Key]
		return validateSavedDesiredMatch(action, desiredNegative, expected, ok, savedPlanNegativeKey(desiredNegative))
	case diff.OperationDelete:
		currentNegative, err := decodeSavedActionValue[diff.NegativeKeyword](action.Current, "current negative keyword")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentNegative.ID) == "" {
			return fmt.Errorf("saved plan action %q current negative keyword is missing remote id", action.Key)
		}
		if err := requireSavedNegativeParent(deps, currentNegative, action); err != nil {
			return err
		}
		if got := savedPlanNegativeKey(currentNegative); got != action.Key {
			return fmt.Errorf("saved plan action %q current negative keyword key mismatch %q", action.Key, got)
		}
		return nil
	default:
		return fmt.Errorf("saved plan action %q has unsupported negative keyword operation %q", action.Key, action.Operation)
	}
}

func validateSavedCustomAdAction(action diff.Action, desired savedPlanDesiredIndex, deps savedPlanDependencies) error {
	switch action.Operation {
	case diff.OperationCreate:
		desiredCustomAd, err := decodeSavedActionValue[diff.CustomAd](action.Desired, "desired custom ad")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if err := requireSavedAdGroupDependency(deps, desiredCustomAd.CampaignName, desiredCustomAd.AdGroupName, action); err != nil {
			return err
		}
		expected, ok := desired.customAds[action.Key]
		return validateSavedDesiredMatch(action, desiredCustomAd, expected, ok, savedPlanCustomAdKey(desiredCustomAd.CampaignName, desiredCustomAd.AdGroupName, desiredCustomAd.ProductPage))
	case diff.OperationUpdate, diff.OperationPause, diff.OperationActivate, diff.OperationNoop:
		currentCustomAd, err := decodeSavedActionValue[diff.CustomAd](action.Current, "current custom ad")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentCustomAd.ID) == "" {
			return fmt.Errorf("saved plan action %q current custom ad is missing remote id", action.Key)
		}
		if err := requireSavedAdGroupDependency(deps, currentCustomAd.CampaignName, currentCustomAd.AdGroupName, action); err != nil {
			return err
		}
		if got := savedPlanCustomAdKey(currentCustomAd.CampaignName, currentCustomAd.AdGroupName, currentCustomAd.ProductPage); got != action.Key {
			return fmt.Errorf("saved plan action %q current custom ad key mismatch %q", action.Key, got)
		}
		desiredCustomAd, err := decodeSavedActionValue[diff.CustomAd](action.Desired, "desired custom ad")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		expected, ok := desired.customAds[action.Key]
		return validateSavedDesiredMatch(action, desiredCustomAd, expected, ok, savedPlanCustomAdKey(desiredCustomAd.CampaignName, desiredCustomAd.AdGroupName, desiredCustomAd.ProductPage))
	case diff.OperationDelete:
		currentCustomAd, err := decodeSavedActionValue[diff.CustomAd](action.Current, "current custom ad")
		if err != nil {
			return fmt.Errorf("saved plan action %q: %w", action.Key, err)
		}
		if strings.TrimSpace(currentCustomAd.ID) == "" {
			return fmt.Errorf("saved plan action %q current custom ad is missing remote id", action.Key)
		}
		if err := requireSavedAdGroupDependency(deps, currentCustomAd.CampaignName, currentCustomAd.AdGroupName, action); err != nil {
			return err
		}
		if got := savedPlanCustomAdKey(currentCustomAd.CampaignName, currentCustomAd.AdGroupName, currentCustomAd.ProductPage); got != action.Key {
			return fmt.Errorf("saved plan action %q current custom ad key mismatch %q", action.Key, got)
		}
		return nil
	default:
		return fmt.Errorf("saved plan action %q has unsupported custom ad operation %q", action.Key, action.Operation)
	}
}

func validateSavedDesiredMatch[T any](action diff.Action, actual T, expected T, found bool, actualKey string) error {
	if actualKey != action.Key {
		return fmt.Errorf("saved plan action %q desired key mismatch %q", action.Key, actualKey)
	}
	if !found {
		return fmt.Errorf("saved plan action %q desired resource is not present in embedded spec_yaml", action.Key)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return fmt.Errorf("encode expected desired payload: %w", err)
	}
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		return fmt.Errorf("encode desired payload: %w", err)
	}
	if string(expectedJSON) != string(actualJSON) {
		return fmt.Errorf("saved plan action %q desired payload no longer matches embedded spec_yaml", action.Key)
	}
	return nil
}

func requireSavedCampaignDependency(deps savedPlanDependencies, campaignName string, action diff.Action) error {
	if _, ok := deps.campaigns[savedPlanCampaignKey(campaignName)]; ok {
		return nil
	}
	return fmt.Errorf("saved plan action %q references campaign %q without a replayable parent action", action.Key, campaignName)
}

func requireSavedAdGroupDependency(deps savedPlanDependencies, campaignName, adGroupName string, action diff.Action) error {
	if err := requireSavedCampaignDependency(deps, campaignName, action); err != nil {
		return err
	}
	if _, ok := deps.adGroups[savedPlanAdGroupKey(campaignName, adGroupName)]; ok {
		return nil
	}
	return fmt.Errorf("saved plan action %q references adgroup %q in campaign %q without a replayable parent action", action.Key, adGroupName, campaignName)
}

func requireSavedNegativeParent(deps savedPlanDependencies, negative diff.NegativeKeyword, action diff.Action) error {
	if negative.Scope == diff.ScopeAdGroup {
		return requireSavedAdGroupDependency(deps, negative.CampaignName, negative.AdGroupName, action)
	}
	return requireSavedCampaignDependency(deps, negative.CampaignName, action)
}

func decodeSavedActionValue[T any](value any, label string) (T, error) {
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

func savedPlanCampaignKey(name string) string {
	return spec.Fold(name)
}

func savedPlanAdGroupKey(campaignName, adGroupName string) string {
	return savedPlanCampaignKey(campaignName) + "|" + spec.Fold(adGroupName)
}

func savedPlanKeywordKey(campaignName, adGroupName, text string, matchType spec.MatchType) string {
	return savedPlanAdGroupKey(campaignName, adGroupName) + "|" + spec.Fold(text) + "|" + string(matchType)
}

func savedPlanNegativeKey(item diff.NegativeKeyword) string {
	if item.Scope == diff.ScopeAdGroup {
		return string(item.Scope) + "|" + savedPlanAdGroupKey(item.CampaignName, item.AdGroupName) + "|" + spec.Fold(item.Text) + "|" + string(item.MatchType)
	}
	return string(item.Scope) + "|" + savedPlanCampaignKey(item.CampaignName) + "|" + spec.Fold(item.Text) + "|" + string(item.MatchType)
}

func savedPlanCustomAdKey(campaignName, adGroupName, productPage string) string {
	return savedPlanAdGroupKey(campaignName, adGroupName) + "|" + spec.Fold(productPage)
}
