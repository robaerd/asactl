package testutil

import (
	"testing"

	"github.com/robaerd/asactl/internal/spec"
)

func MustDecimal(tb testing.TB, value string) spec.Decimal {
	tb.Helper()
	decimal, err := spec.ParseDecimal(value)
	if err != nil {
		tb.Fatalf("parse decimal %q: %v", value, err)
	}
	return decimal
}

func MustLoadConfigFixture(tb testing.TB) spec.Spec {
	tb.Helper()
	loaded, err := spec.Load([]byte(`version: 1
kind: Config
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: Readcap
  app_id: "1613230582"
defaults:
  currency: EUR
  devices: [IPHONE, IPAD]
  storefronts: [US]
product_pages:
  CPP1:
    product_page_id: pp1
    name: Reading Tracker
    locale: en-US
  CPP2:
    product_page_id: pp2
    name: Capture & Organize
    locale: en-US
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
    daily_budget: 10.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 1.50
        product_page: CPP1
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 1.50
            status: ACTIVE
  - name: US - Discovery
    storefronts: [US]
    daily_budget: 8.00
    status: ACTIVE
    adgroups:
      - name: Discovery - Search Match
        status: ACTIVE
        default_cpt_bid: 1.20
        targeting: SEARCH_MATCH
`))
	if err != nil {
		tb.Fatalf("load config fixture: %v", err)
	}
	return loaded
}
