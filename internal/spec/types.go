package spec

import (
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

type Status string

type MatchType string

type Device string

type Targeting string

type GeneratorKind string

const (
	StatusActive Status = "ACTIVE"
	StatusPaused Status = "PAUSED"

	MatchTypeExact MatchType = "EXACT"
	MatchTypeBroad MatchType = "BROAD"

	DeviceIPhone Device = "IPHONE"
	DeviceIPad   Device = "IPAD"

	TargetingKeywords    Targeting = "KEYWORDS"
	TargetingSearchMatch Targeting = "SEARCH_MATCH"

	GeneratorKindKeywordToNegative GeneratorKind = "KeywordToNegative"
)

type Spec struct {
	Version       int            `yaml:"version"`
	Kind          Kind           `yaml:"kind"`
	CampaignGroup CampaignGroup  `yaml:"campaign_group"`
	Auth          Auth           `yaml:"auth"`
	App           App            `yaml:"app"`
	Defaults      Defaults       `yaml:"defaults"`
	ProductPages  ProductPageMap `yaml:"product_pages,omitempty"`
	Generators    []Generator    `yaml:"generators,omitempty"`
	Campaigns     []Campaign     `yaml:"campaigns"`
	Meta          Meta           `yaml:"-"`
}

type Meta struct {
	Composed        bool                      `yaml:"-"`
	CampaignSources map[string]CampaignSource `yaml:"-"`
}

type CampaignSource struct {
	SourcePath  string `yaml:"-"`
	SourceOrder int    `yaml:"-"`
}

type CampaignGroup struct {
	ID string `yaml:"id"`
}

type Auth struct {
	Profile string `yaml:"profile,omitempty"`
}

type App struct {
	Name  string `yaml:"name"`
	AppID string `yaml:"app_id"`
}

type Defaults struct {
	Currency    string   `yaml:"currency,omitempty"`
	Devices     []Device `yaml:"devices,omitempty"`
	Storefronts []string `yaml:"storefronts,omitempty"`
}

type ProductPageMap map[string]ProductPage

type ProductPage struct {
	ProductPageID string `yaml:"product_page_id"`
	Name          string `yaml:"name,omitempty"`
	AppStoreURL   string `yaml:"app_store_url,omitempty"`
	Locale        string `yaml:"locale,omitempty"`
}

type Generator struct {
	Name        string        `yaml:"name"`
	Kind        GeneratorKind `yaml:"kind"`
	Spec        GeneratorSpec `yaml:"spec"`
	SourcePath  string        `yaml:"-"`
	SourceOrder int           `yaml:"-"`
}

type GeneratorSpec struct {
	SourceRefs GeneratorSourceRefs `yaml:"source_refs"`
	TargetRef  GeneratorTargetRef  `yaml:"target_ref"`
	Filters    GeneratorFilters    `yaml:"filters"`
	Generate   GeneratorGenerate   `yaml:"generate"`
}

type GeneratorSourceRefs struct {
	Campaigns []string `yaml:"campaigns"`
}

type GeneratorTargetRef struct {
	Campaign string `yaml:"campaign"`
}

type GeneratorFilters struct {
	KeywordMatchTypes []MatchType `yaml:"keyword_match_types"`
}

type GeneratorGenerate struct {
	CampaignNegativeKeywords GeneratorNegativeKeywordSpec `yaml:"campaign_negative_keywords"`
}

type GeneratorNegativeKeywordSpec struct {
	MatchType MatchType `yaml:"match_type"`
	Status    Status    `yaml:"status"`
}

type Campaign struct {
	Name                     string            `yaml:"name"`
	Storefronts              []string          `yaml:"storefronts,omitempty"`
	DailyBudget              Decimal           `yaml:"daily_budget"`
	Status                   Status            `yaml:"status"`
	CampaignNegativeKeywords []NegativeKeyword `yaml:"campaign_negative_keywords,omitempty"`
	AdGroups                 []AdGroup         `yaml:"adgroups"`
}

type AdGroup struct {
	Name                    string            `yaml:"name"`
	Status                  Status            `yaml:"status"`
	DefaultCPTBid           Decimal           `yaml:"default_cpt_bid"`
	ProductPage             string            `yaml:"product_page,omitempty"`
	Targeting               Targeting         `yaml:"targeting"`
	Keywords                []Keyword         `yaml:"keywords,omitempty"`
	AdGroupNegativeKeywords []NegativeKeyword `yaml:"adgroup_negative_keywords,omitempty"`
}

type Keyword struct {
	Text      string    `yaml:"text"`
	MatchType MatchType `yaml:"match_type"`
	Bid       Decimal   `yaml:"bid"`
	Status    Status    `yaml:"status"`
}

type NegativeKeyword struct {
	Text        string    `yaml:"text"`
	MatchType   MatchType `yaml:"match_type"`
	Status      Status    `yaml:"status"`
	SourcePath  string    `yaml:"-"`
	SourceOrder int       `yaml:"-"`
}

func (k Keyword) MarshalYAML() (any, error) {
	return flowMappingNode(
		flowField{Key: "text", Value: k.Text},
		flowField{Key: "match_type", Value: k.MatchType},
		flowField{Key: "bid", Value: k.Bid},
		flowField{Key: "status", Value: k.Status},
	)
}

func (n NegativeKeyword) MarshalYAML() (any, error) {
	return flowMappingNode(
		flowField{Key: "text", Value: n.Text},
		flowField{Key: "match_type", Value: n.MatchType},
		flowField{Key: "status", Value: n.Status},
	)
}

func (m ProductPageMap) MarshalYAML() (any, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		valueNode := &yaml.Node{}
		if err := valueNode.Encode(m[key]); err != nil {
			return nil, err
		}
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
			valueNode,
		)
	}
	return node, nil
}

func NormalizeName(value string) string {
	return strings.TrimSpace(value)
}

func Fold(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

type flowField struct {
	Key   string
	Value any
}

func flowMappingNode(fields ...flowField) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Style: yaml.FlowStyle}
	for _, field := range fields {
		valueNode := &yaml.Node{}
		if err := valueNode.Encode(field.Value); err != nil {
			return nil, err
		}
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: field.Key},
			valueNode,
		)
	}
	return node, nil
}
