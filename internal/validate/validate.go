package validate

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/robaerd/asactl/internal/spec"
)

const (
	WarningThreshold = 4500
	HardCap          = 5000
)

type Result struct {
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

func (r Result) OK() bool {
	return len(r.Errors) == 0
}

func Run(input spec.Spec) Result {
	result := Result{}

	if input.Version != 1 {
		result.Errors = append(result.Errors, fmt.Sprintf("version must be 1, got %d", input.Version))
	}
	validateCampaignGroup(input, &result)
	validateAuth(input, &result)
	validateApp(input, &result)
	validateDefaults(input, &result)
	validateProductPages(input, &result)
	validateGenerators(input, &result)
	validateCampaigns(input, &result)

	return result
}

func validateCampaignGroup(input spec.Spec, result *Result) {
	if strings.TrimSpace(input.CampaignGroup.ID) == "" {
		result.Errors = append(result.Errors, "campaign_group.id is required")
	}
}

func validateAuth(input spec.Spec, result *Result) {
	if strings.TrimSpace(input.Auth.Profile) == "" {
		result.Warnings = append(result.Warnings, "auth.profile is not set; runtime commands must resolve a profile from --profile or the global config default_profile")
	}
}

func validateApp(input spec.Spec, result *Result) {
	if strings.TrimSpace(input.App.Name) == "" {
		result.Errors = append(result.Errors, "app.name is required")
	}
	appID := strings.TrimSpace(input.App.AppID)
	if appID == "" {
		result.Errors = append(result.Errors, "app.app_id is required")
		return
	}
	if appID == "REPLACE_ME" {
		result.Warnings = append(result.Warnings, "app.app_id is still REPLACE_ME")
	}
}

func validateDefaults(input spec.Spec, result *Result) {
	if strings.TrimSpace(input.Defaults.Currency) == "" {
		result.Warnings = append(result.Warnings, "defaults.currency is not set")
	} else if err := validateCurrency(input.Defaults.Currency); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("defaults.currency %v", err))
	}
	for _, device := range input.Defaults.Devices {
		if err := validateDevice(device); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("defaults.devices %v", err))
		}
	}
}

func validateProductPages(input spec.Spec, result *Result) {
	idOwners := map[string]string{}
	for _, key := range sortedMapKeys(input.ProductPages) {
		productPage := input.ProductPages[key]
		if strings.TrimSpace(key) == "" {
			result.Errors = append(result.Errors, "product_pages key must not be blank")
		}
		productPageID := strings.TrimSpace(productPage.ProductPageID)
		if productPageID == "" {
			result.Errors = append(result.Errors, fmt.Sprintf("product_pages %q product_page_id is required", key))
		} else if owner, ok := idOwners[productPageID]; ok {
			result.Errors = append(result.Errors, fmt.Sprintf("product_pages %q duplicates product_page_id for product_pages %q", key, owner))
		} else {
			idOwners[productPageID] = key
		}
		if strings.TrimSpace(productPage.AppStoreURL) != "" && !strings.HasPrefix(strings.TrimSpace(productPage.AppStoreURL), "https://") {
			result.Errors = append(result.Errors, fmt.Sprintf("product_pages %q app_store_url must start with https://", key))
		}
	}
}

func validateGenerators(input spec.Spec, result *Result) {
	campaignNames := campaignNameSet(input.Campaigns)
	seenNames := map[string]struct{}{}
	for index, generator := range input.Generators {
		label := generatorLabel(index, generator)
		name := spec.NormalizeName(generator.Name)
		if name == "" {
			result.Errors = append(result.Errors, fmt.Sprintf("%s name is required", label))
		} else {
			key := spec.Fold(name)
			if _, ok := seenNames[key]; ok {
				result.Errors = append(result.Errors, fmt.Sprintf("duplicate generator name %q", name))
			} else {
				seenNames[key] = struct{}{}
			}
		}
		if generator.Kind != spec.GeneratorKindKeywordToNegative {
			result.Errors = append(result.Errors, fmt.Sprintf("%s kind %q is unsupported", label, generator.Kind))
		}
		targetCampaign := spec.NormalizeName(generator.Spec.TargetRef.Campaign)
		if targetCampaign == "" {
			result.Errors = append(result.Errors, fmt.Sprintf("%s spec.target_ref.campaign is required", label))
		} else if _, ok := campaignNames[spec.Fold(targetCampaign)]; !ok {
			result.Errors = append(result.Errors, fmt.Sprintf("%s target campaign %q does not exist", label, generator.Spec.TargetRef.Campaign))
		}
		if len(generator.Spec.SourceRefs.Campaigns) == 0 {
			result.Errors = append(result.Errors, fmt.Sprintf("%s must define at least one spec.source_refs.campaigns entry", label))
		}
		for _, source := range generator.Spec.SourceRefs.Campaigns {
			if _, ok := campaignNames[spec.Fold(source)]; !ok {
				result.Errors = append(result.Errors, fmt.Sprintf("%s source campaign %q does not exist", label, source))
			}
			if targetCampaign != "" && spec.Fold(source) == spec.Fold(targetCampaign) {
				result.Errors = append(result.Errors, fmt.Sprintf("%s target campaign %q must not include itself in spec.source_refs.campaigns", label, targetCampaign))
			}
		}
		if !slices.Equal(generator.Spec.Filters.KeywordMatchTypes, []spec.MatchType{spec.MatchTypeExact}) {
			result.Errors = append(result.Errors, fmt.Sprintf("%s spec.filters.keyword_match_types must be [EXACT] in v1", label))
		}
		if generator.Spec.Generate.CampaignNegativeKeywords.MatchType != spec.MatchTypeExact {
			result.Errors = append(result.Errors, fmt.Sprintf("%s spec.generate.campaign_negative_keywords.match_type must be EXACT in v1", label))
		}
		if generator.Spec.Generate.CampaignNegativeKeywords.Status != spec.StatusActive {
			result.Errors = append(result.Errors, fmt.Sprintf("%s spec.generate.campaign_negative_keywords.status must be ACTIVE in v1", label))
		}
	}
}

func generatorLabel(index int, generator spec.Generator) string {
	if name := spec.NormalizeName(generator.Name); name != "" {
		return fmt.Sprintf("generator %q", name)
	}
	return fmt.Sprintf("generators[%d]", index)
}

func validateCampaigns(input spec.Spec, result *Result) {
	seenCampaigns := map[string]struct{}{}
	for _, campaign := range input.Campaigns {
		campaignName := spec.NormalizeName(campaign.Name)
		if campaignName == "" {
			result.Errors = append(result.Errors, "campaign name must not be blank")
			continue
		}
		campaignKey := spec.Fold(campaignName)
		if _, ok := seenCampaigns[campaignKey]; ok {
			result.Errors = append(result.Errors, fmt.Sprintf("duplicate campaign name %q", campaignName))
			continue
		}
		seenCampaigns[campaignKey] = struct{}{}
		effectiveStorefronts := campaign.Storefronts
		if len(effectiveStorefronts) == 0 {
			effectiveStorefronts = input.Defaults.Storefronts
		}
		if len(effectiveStorefronts) == 0 {
			result.Errors = append(result.Errors, fmt.Sprintf("campaign %q must define storefronts or inherit defaults.storefronts", campaignName))
		}
		if !campaign.DailyBudget.IsPositive() {
			result.Errors = append(result.Errors, fmt.Sprintf("campaign %q daily_budget must be > 0", campaignName))
		}
		if err := validateStatus(campaign.Status); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("campaign %q %v", campaignName, err))
		}
		validateNegativeKeywordScope(fmt.Sprintf("campaign %q", campaignName), campaign.CampaignNegativeKeywords, result)
		validateCap("campaign negatives", campaignName, len(campaign.CampaignNegativeKeywords), result)
		validateAdGroups(campaign, input, result)
	}
}

func validateAdGroups(campaign spec.Campaign, input spec.Spec, result *Result) {
	seenAdGroups := map[string]struct{}{}
	for _, adGroup := range campaign.AdGroups {
		name := spec.NormalizeName(adGroup.Name)
		if name == "" {
			result.Errors = append(result.Errors, fmt.Sprintf("campaign %q contains blank adgroup name", campaign.Name))
			continue
		}
		adGroupKey := spec.Fold(name)
		if _, ok := seenAdGroups[adGroupKey]; ok {
			result.Errors = append(result.Errors, fmt.Sprintf("campaign %q has duplicate adgroup %q", campaign.Name, name))
			continue
		}
		seenAdGroups[adGroupKey] = struct{}{}
		if err := validateStatus(adGroup.Status); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("adgroup %q in campaign %q %v", name, campaign.Name, err))
		}
		if err := validateTargeting(adGroup.Targeting); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("adgroup %q in campaign %q %v", name, campaign.Name, err))
		}
		if adGroup.ProductPage != "" {
			if _, ok := input.ProductPages[adGroup.ProductPage]; !ok {
				result.Errors = append(result.Errors, fmt.Sprintf("adgroup %q in campaign %q references unknown product_page %q", name, campaign.Name, adGroup.ProductPage))
			}
		}
		if adGroup.Targeting == spec.TargetingSearchMatch && len(adGroup.Keywords) > 0 {
			result.Errors = append(result.Errors, fmt.Sprintf("adgroup %q in campaign %q uses targeting SEARCH_MATCH and must define zero keywords", name, campaign.Name))
		}
		if !adGroup.DefaultCPTBid.IsPositive() {
			result.Errors = append(result.Errors, fmt.Sprintf("adgroup %q in campaign %q default_cpt_bid must be > 0", name, campaign.Name))
		}
		validateKeywords(campaign.Name, adGroup, result)
		validateNegativeKeywordScope(fmt.Sprintf("adgroup %q in campaign %q", name, campaign.Name), adGroup.AdGroupNegativeKeywords, result)
		validateCap("adgroup negatives", campaign.Name+"/"+name, len(adGroup.AdGroupNegativeKeywords), result)
	}
}

func validateKeywords(campaignName string, adGroup spec.AdGroup, result *Result) {
	seen := map[string]struct{}{}
	for _, keyword := range adGroup.Keywords {
		if keyword.Text == "" {
			result.Errors = append(result.Errors, fmt.Sprintf("keyword in adgroup %q in campaign %q must not be blank", adGroup.Name, campaignName))
			continue
		}
		if err := validateStatus(keyword.Status); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("keyword %q in adgroup %q in campaign %q %v", keyword.Text, adGroup.Name, campaignName, err))
		}
		if err := validateMatchType(keyword.MatchType); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("keyword %q in adgroup %q in campaign %q %v", keyword.Text, adGroup.Name, campaignName, err))
		}
		if !keyword.Bid.IsPositive() {
			result.Errors = append(result.Errors, fmt.Sprintf("keyword %q in adgroup %q in campaign %q bid must be > 0; use status PAUSED instead of bid 0.00", keyword.Text, adGroup.Name, campaignName))
		}
		key := spec.Fold(keyword.Text) + "|" + string(keyword.MatchType)
		if _, ok := seen[key]; ok {
			result.Errors = append(result.Errors, fmt.Sprintf("adgroup %q in campaign %q has duplicate keyword %q with match type %s", adGroup.Name, campaignName, keyword.Text, keyword.MatchType))
			continue
		}
		seen[key] = struct{}{}
	}
	validateCap("keywords", campaignName+"/"+adGroup.Name, len(adGroup.Keywords), result)
}

func validateNegativeKeywordScope(scope string, keywords []spec.NegativeKeyword, result *Result) {
	seen := map[string]struct{}{}
	for _, keyword := range keywords {
		if keyword.Text == "" {
			result.Errors = append(result.Errors, fmt.Sprintf("negative keyword in %s must not be blank", scope))
			continue
		}
		if err := validateStatus(keyword.Status); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("negative keyword %q in %s %v", keyword.Text, scope, err))
		}
		if err := validateMatchType(keyword.MatchType); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("negative keyword %q in %s %v", keyword.Text, scope, err))
		}
		key := spec.Fold(keyword.Text) + "|" + string(keyword.MatchType)
		if _, ok := seen[key]; ok {
			result.Errors = append(result.Errors, fmt.Sprintf("%s has duplicate negative keyword %q with match type %s", scope, keyword.Text, keyword.MatchType))
			continue
		}
		seen[key] = struct{}{}
	}
}

func validateStatus(value spec.Status) error {
	switch value {
	case spec.StatusActive, spec.StatusPaused:
		return nil
	default:
		return errors.New("status must be ACTIVE or PAUSED")
	}
}

func validateMatchType(value spec.MatchType) error {
	switch value {
	case spec.MatchTypeExact, spec.MatchTypeBroad:
		return nil
	default:
		return errors.New("match_type must be EXACT or BROAD")
	}
}

func validateTargeting(value spec.Targeting) error {
	switch value {
	case spec.TargetingKeywords, spec.TargetingSearchMatch:
		return nil
	default:
		return errors.New("targeting must be KEYWORDS or SEARCH_MATCH")
	}
}

func validateDevice(value spec.Device) error {
	switch value {
	case spec.DeviceIPhone, spec.DeviceIPad:
		return nil
	default:
		return fmt.Errorf("device must be IPHONE or IPAD, got %q", value)
	}
}

func validateCurrency(value string) error {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) != 3 {
		return fmt.Errorf("must be a 3-letter code, got %q", value)
	}
	for _, r := range trimmed {
		if r < 'A' || r > 'Z' {
			return fmt.Errorf("must contain only uppercase ASCII letters, got %q", value)
		}
	}
	return nil
}

func validateCap(label, scope string, count int, result *Result) {
	if count > HardCap {
		result.Errors = append(result.Errors, fmt.Sprintf("%s for %s exceeds hard cap of %d", label, scope, HardCap))
		return
	}
	if count >= WarningThreshold {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%s for %s is near limit (%d/%d)", label, scope, count, HardCap))
	}
}

func campaignNameSet(campaigns []spec.Campaign) map[string]struct{} {
	seen := make(map[string]struct{}, len(campaigns))
	for _, campaign := range campaigns {
		seen[spec.Fold(campaign.Name)] = struct{}{}
	}
	return seen
}

func sortedMapKeys[T any](input map[string]T) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
