package spec

type Kind string

const (
	KindConfig    Kind = "Config"
	KindManifest  Kind = "Manifest"
	KindBase      Kind = "Base"
	KindCampaigns Kind = "Campaigns"
)

type DocumentType string

const (
	DocumentTypeConfig    DocumentType = "config"
	DocumentTypeManifest  DocumentType = "manifest"
	DocumentTypeBase      DocumentType = "base"
	DocumentTypeCampaigns DocumentType = "campaigns"
)

type Manifest struct {
	Version   int      `yaml:"version"`
	Kind      Kind     `yaml:"kind"`
	Base      string   `yaml:"base"`
	Campaigns []string `yaml:"campaigns,omitempty"`
}

type Base struct {
	Version       int            `yaml:"version"`
	Kind          Kind           `yaml:"kind"`
	CampaignGroup CampaignGroup  `yaml:"campaign_group"`
	Auth          Auth           `yaml:"auth"`
	App           App            `yaml:"app"`
	Defaults      Defaults       `yaml:"defaults"`
	ProductPages  ProductPageMap `yaml:"product_pages,omitempty"`
}

type CampaignsFile struct {
	Version    int         `yaml:"version"`
	Kind       Kind        `yaml:"kind"`
	Generators []Generator `yaml:"generators,omitempty"`
	Campaigns  []Campaign  `yaml:"campaigns,omitempty"`
}

type Document struct {
	Type          DocumentType
	Path          string
	Spec          Spec
	Manifest      Manifest
	Base          Base
	Campaigns     CampaignsFile
	BasePath      string
	CampaignPaths []string
}

type FormattedFile struct {
	Path    string
	Content []byte
}
