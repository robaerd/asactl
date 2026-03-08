package spec

import (
	"slices"
	"strings"
)

func Normalize(input Spec) Spec {
	clone := input
	clone.Kind = KindConfig
	clone.Defaults = normalizeDefaults(input.Defaults)
	clone.ProductPages = cloneProductPages(input.ProductPages)
	clone.Generators = cloneGenerators(input.Generators)
	clone.Campaigns = make([]Campaign, 0, len(input.Campaigns))

	for _, campaign := range input.Campaigns {
		normalizedCampaign := campaign
		normalizedCampaign.Name = NormalizeName(campaign.Name)
		normalizedCampaign.Storefronts = normalizeStorefronts(campaign.Storefronts, clone.Defaults.Storefronts)
		normalizedCampaign.CampaignNegativeKeywords = dedupeNegatives(cloneNegativeKeywords(campaign.CampaignNegativeKeywords))
		normalizedCampaign.AdGroups = make([]AdGroup, 0, len(campaign.AdGroups))
		for _, adGroup := range campaign.AdGroups {
			normalizedAdGroup := adGroup
			normalizedAdGroup.Name = NormalizeName(adGroup.Name)
			normalizedAdGroup.ProductPage = NormalizeName(adGroup.ProductPage)
			normalizedAdGroup.Keywords = dedupeKeywords(cloneKeywords(adGroup.Keywords))
			normalizedAdGroup.AdGroupNegativeKeywords = dedupeNegatives(cloneNegativeKeywords(adGroup.AdGroupNegativeKeywords))
			normalizedCampaign.AdGroups = append(normalizedCampaign.AdGroups, normalizedAdGroup)
		}
		clone.Campaigns = append(clone.Campaigns, normalizedCampaign)
	}

	applyGeneratedOverlapNegatives(&clone)
	return clone
}

func normalizeDefaults(defaults Defaults) Defaults {
	defaults.Currency = strings.ToUpper(NormalizeName(defaults.Currency))
	defaults.Devices = normalizeDevices(defaults.Devices)
	defaults.Storefronts = normalizeStorefronts(defaults.Storefronts, nil)
	return defaults
}

func normalizeDevices(devices []Device) []Device {
	seen := map[Device]struct{}{}
	result := make([]Device, 0, len(devices))
	for _, device := range devices {
		normalized := Device(strings.ToUpper(NormalizeName(string(device))))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

func normalizeStorefronts(storefronts []string, fallback []string) []string {
	if len(storefronts) == 0 {
		storefronts = fallback
	}
	return normalizeNameList(storefronts)
}

func normalizeNameList(input []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(input))
	for _, item := range input {
		normalized := NormalizeName(item)
		if normalized == "" {
			continue
		}
		key := Fold(normalized)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, normalized)
	}
	return result
}

func cloneProductPages(input ProductPageMap) ProductPageMap {
	output := ProductPageMap{}
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneGenerators(input []Generator) []Generator {
	output := make([]Generator, 0, len(input))
	for _, generator := range input {
		copyGenerator := generator
		copyGenerator.Name = NormalizeName(generator.Name)
		copyGenerator.Kind = GeneratorKind(NormalizeName(string(generator.Kind)))
		copyGenerator.Spec.SourceRefs.Campaigns = normalizeNameList(generator.Spec.SourceRefs.Campaigns)
		copyGenerator.Spec.TargetRef.Campaign = NormalizeName(generator.Spec.TargetRef.Campaign)
		copyGenerator.Spec.Filters.KeywordMatchTypes = cloneMatchTypes(generator.Spec.Filters.KeywordMatchTypes)
		output = append(output, copyGenerator)
	}
	return output
}

func cloneMatchTypes(input []MatchType) []MatchType {
	output := make([]MatchType, 0, len(input))
	for _, item := range input {
		output = append(output, item)
	}
	return output
}

func cloneKeywords(input []Keyword) []Keyword {
	output := make([]Keyword, 0, len(input))
	for _, item := range input {
		copyItem := item
		copyItem.Text = NormalizeName(item.Text)
		output = append(output, copyItem)
	}
	return output
}

func cloneNegativeKeywords(input []NegativeKeyword) []NegativeKeyword {
	output := make([]NegativeKeyword, 0, len(input))
	for _, item := range input {
		copyItem := item
		copyItem.Text = NormalizeName(item.Text)
		output = append(output, copyItem)
	}
	return output
}

func dedupeKeywords(input []Keyword) []Keyword {
	seen := map[string]struct{}{}
	result := make([]Keyword, 0, len(input))
	for _, item := range input {
		key := Fold(item.Text) + "|" + string(item.MatchType)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result
}

func dedupeNegatives(input []NegativeKeyword) []NegativeKeyword {
	seen := map[string]struct{}{}
	result := make([]NegativeKeyword, 0, len(input))
	for _, item := range input {
		key := Fold(item.Text) + "|" + string(item.MatchType)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result
}

func applyGeneratedOverlapNegatives(spec *Spec) {
	if spec == nil {
		return
	}
	for _, generator := range spec.Generators {
		if generator.Kind != GeneratorKindKeywordToNegative {
			continue
		}
		target := findCampaign(spec.Campaigns, generator.Spec.TargetRef.Campaign)
		if target == nil {
			continue
		}
		for _, sourceName := range generator.Spec.SourceRefs.Campaigns {
			source := findCampaign(spec.Campaigns, sourceName)
			if source == nil {
				continue
			}
			for _, adGroup := range source.AdGroups {
				for _, keyword := range adGroup.Keywords {
					if !matchTypeIncluded(generator.Spec.Filters.KeywordMatchTypes, keyword.MatchType) {
						continue
					}
					target.CampaignNegativeKeywords = append(target.CampaignNegativeKeywords, NegativeKeyword{
						Text:        keyword.Text,
						MatchType:   generator.Spec.Generate.CampaignNegativeKeywords.MatchType,
						Status:      generator.Spec.Generate.CampaignNegativeKeywords.Status,
						SourcePath:  generator.SourcePath,
						SourceOrder: generator.SourceOrder,
					})
				}
			}
		}
		target.CampaignNegativeKeywords = dedupeNegatives(target.CampaignNegativeKeywords)
		slices.SortStableFunc(target.CampaignNegativeKeywords, func(left, right NegativeKeyword) int {
			if Fold(left.Text) == Fold(right.Text) {
				if left.MatchType < right.MatchType {
					return -1
				}
				if left.MatchType > right.MatchType {
					return 1
				}
				return 0
			}
			return strings.Compare(Fold(left.Text), Fold(right.Text))
		})
	}
}

func matchTypeIncluded(allowed []MatchType, candidate MatchType) bool {
	for _, matchType := range allowed {
		if matchType == candidate {
			return true
		}
	}
	return false
}

func findCampaign(campaigns []Campaign, name string) *Campaign {
	for index := range campaigns {
		if Fold(campaigns[index].Name) == Fold(name) {
			return &campaigns[index]
		}
	}
	return nil
}
