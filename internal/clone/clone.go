package clone

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/shopspring/decimal"
)

func Spec(input spec.Spec, storefront string, bidMultiplier float64, budgetMultiplier float64) (spec.Spec, error) {
	if strings.TrimSpace(storefront) == "" {
		return spec.Spec{}, errors.New("storefront is required")
	}
	if bidMultiplier <= 0 {
		return spec.Spec{}, errors.New("bid multiplier must be > 0")
	}
	if budgetMultiplier <= 0 {
		return spec.Spec{}, errors.New("budget multiplier must be > 0")
	}
	bidMultiplierDecimal, err := decimalFromFloat64(bidMultiplier)
	if err != nil {
		return spec.Spec{}, fmt.Errorf("parse bid multiplier: %w", err)
	}
	budgetMultiplierDecimal, err := decimalFromFloat64(budgetMultiplier)
	if err != nil {
		return spec.Spec{}, fmt.Errorf("parse budget multiplier: %w", err)
	}
	clone := spec.Normalize(input)
	clone.Meta = spec.Meta{}
	storefront = strings.ToUpper(strings.TrimSpace(storefront))
	oldNames := map[string]string{}
	label := storefrontLabel(storefront)
	for i := range clone.Campaigns {
		old := clone.Campaigns[i].Name
		clone.Campaigns[i].Storefronts = []string{storefront}
		clone.Campaigns[i].DailyBudget = spec.Decimal{Decimal: clone.Campaigns[i].DailyBudget.Decimal.Mul(budgetMultiplierDecimal).Round(2)}
		clone.Campaigns[i].Name = rewriteCampaignName(clone.Campaigns[i].Name, label)
		oldNames[old] = clone.Campaigns[i].Name
		for j := range clone.Campaigns[i].AdGroups {
			clone.Campaigns[i].AdGroups[j].DefaultCPTBid = spec.Decimal{Decimal: clone.Campaigns[i].AdGroups[j].DefaultCPTBid.Decimal.Mul(bidMultiplierDecimal).Round(2)}
			for k := range clone.Campaigns[i].AdGroups[j].Keywords {
				clone.Campaigns[i].AdGroups[j].Keywords[k].Bid = spec.Decimal{Decimal: clone.Campaigns[i].AdGroups[j].Keywords[k].Bid.Decimal.Mul(bidMultiplierDecimal).Round(2)}
			}
		}
	}
	if len(clone.Defaults.Storefronts) > 0 {
		clone.Defaults.Storefronts = []string{storefront}
	}
	for i := range clone.Generators {
		if rewritten, ok := oldNames[clone.Generators[i].Spec.TargetRef.Campaign]; ok {
			clone.Generators[i].Spec.TargetRef.Campaign = rewritten
		}
		for j := range clone.Generators[i].Spec.SourceRefs.Campaigns {
			if rewritten, ok := oldNames[clone.Generators[i].Spec.SourceRefs.Campaigns[j]]; ok {
				clone.Generators[i].Spec.SourceRefs.Campaigns[j] = rewritten
			}
		}
	}
	return clone, nil
}

func storefrontLabel(storefront string) string {
	switch storefront {
	case "GB":
		return "UK"
	default:
		return storefront
	}
}

func rewriteCampaignName(name, label string) string {
	parts := strings.SplitN(name, " - ", 2)
	if len(parts) == 2 {
		return label + " - " + parts[1]
	}
	return label + " - " + name
}

func decimalFromFloat64(value float64) (decimal.Decimal, error) {
	return decimal.NewFromString(strconv.FormatFloat(value, 'f', -1, 64))
}
