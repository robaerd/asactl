package spec

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func LoadFile(path string) (Spec, error) {
	document, err := LoadDocumentFile(path)
	if err != nil {
		return Spec{}, err
	}
	return loadableSpec(document, path)
}

func Load(data []byte) (Spec, error) {
	document, err := loadDocument("-", data, "")
	if err != nil {
		return Spec{}, err
	}
	return loadableSpec(document, "stdin")
}

func LoadSource(path string, data []byte, rootDir string) (Spec, error) {
	document, err := loadDocument(path, data, rootDir)
	if err != nil {
		return Spec{}, err
	}
	label := path
	if label == "" {
		label = "stdin"
	}
	return loadableSpec(document, label)
}

func LoadDocumentFile(path string) (Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Document{}, fmt.Errorf("read spec: %w", err)
	}
	return loadDocument(path, data, "")
}

func LoadDocumentSource(path string, data []byte, rootDir string) (Document, error) {
	return loadDocument(path, data, rootDir)
}

func loadableSpec(document Document, label string) (Spec, error) {
	switch document.Type {
	case DocumentTypeConfig, DocumentTypeManifest:
		return document.Spec, nil
	case DocumentTypeBase:
		return Spec{}, fmt.Errorf("%s is a base file and cannot be used directly; use a config or a manifest", label)
	case DocumentTypeCampaigns:
		return Spec{}, fmt.Errorf("%s is a campaigns file and cannot be used directly; use a config or a manifest", label)
	default:
		return Spec{}, fmt.Errorf("unsupported document type %q", document.Type)
	}
}

func loadDocument(path string, data []byte, rootDir string) (Document, error) {
	root, err := parseRootNode(data)
	if err != nil {
		return Document{}, err
	}
	kind, err := detectKind(root)
	if err != nil {
		return Document{}, err
	}
	if kind == KindManifest && hasRootKey(root, "resources") {
		return Document{}, errors.New(`decode spec: field "resources" is no longer supported in kind "Manifest"; use "campaigns"`)
	}
	switch kind {
	case KindConfig:
		if err := rejectLegacyAppAdamID(root); err != nil {
			return Document{}, err
		}
		loaded, err := decodeConfig(data)
		if err != nil {
			return Document{}, err
		}
		loaded.Meta = Meta{Composed: false}
		return Document{Type: DocumentTypeConfig, Path: path, Spec: loaded}, nil
	case KindManifest:
		return loadManifestDocument(path, data, rootDir)
	case KindBase:
		if err := rejectLegacyAppAdamID(root); err != nil {
			return Document{}, err
		}
		base, err := decodeBase(data)
		if err != nil {
			return Document{}, err
		}
		return Document{Type: DocumentTypeBase, Path: path, Base: base}, nil
	case KindCampaigns:
		campaigns, err := decodeCampaignsFile(data)
		if err != nil {
			return Document{}, err
		}
		return Document{Type: DocumentTypeCampaigns, Path: path, Campaigns: campaigns}, nil
	default:
		return Document{}, fmt.Errorf("decode spec: unsupported kind %q", kind)
	}
}

func loadManifestDocument(path string, data []byte, rootDir string) (Document, error) {
	manifest, err := decodeManifest(data)
	if err != nil {
		return Document{}, err
	}
	if manifest.Version != 1 {
		return Document{}, fmt.Errorf("decode spec: manifest version must be 1, got %d", manifest.Version)
	}
	if strings.TrimSpace(manifest.Base) == "" {
		return Document{}, errors.New("decode spec: manifest base is required")
	}
	basePath, err := resolveRelativePath(path, rootDir, manifest.Base)
	if err != nil {
		return Document{}, fmt.Errorf("load base %q: %w", manifest.Base, err)
	}
	baseDoc, err := LoadDocumentFile(basePath)
	if err != nil {
		return Document{}, fmt.Errorf("load base %q: %w", manifest.Base, err)
	}
	if baseDoc.Type != DocumentTypeBase {
		return Document{}, fmt.Errorf("load base %q: expected kind %q, got %q", manifest.Base, KindBase, baseDoc.Type)
	}
	if baseDoc.Base.Version != manifest.Version {
		return Document{}, fmt.Errorf("load base %q: version %d does not match manifest version %d", manifest.Base, baseDoc.Base.Version, manifest.Version)
	}
	composed := Spec{
		Version:       manifest.Version,
		Kind:          KindConfig,
		CampaignGroup: baseDoc.Base.CampaignGroup,
		Auth:          baseDoc.Base.Auth,
		App:           baseDoc.Base.App,
		Defaults:      baseDoc.Base.Defaults,
		ProductPages:  baseDoc.Base.ProductPages,
		Meta: Meta{
			Composed:        true,
			CampaignSources: map[string]CampaignSource{},
		},
	}

	campaignPaths := make([]string, 0, len(manifest.Campaigns))
	for _, sourcePath := range manifest.Campaigns {
		if strings.TrimSpace(sourcePath) == "" {
			return Document{}, errors.New("decode spec: manifest campaigns must not contain blank paths")
		}
		campaignPath, err := resolveRelativePath(path, rootDir, sourcePath)
		if err != nil {
			return Document{}, fmt.Errorf("load campaigns file %q: %w", sourcePath, err)
		}
		campaignDoc, err := LoadDocumentFile(campaignPath)
		if err != nil {
			return Document{}, fmt.Errorf("load campaigns file %q: %w", sourcePath, err)
		}
		if campaignDoc.Type != DocumentTypeCampaigns {
			return Document{}, fmt.Errorf("load campaigns file %q: expected kind %q, got %q", sourcePath, KindCampaigns, campaignDoc.Type)
		}
		if campaignDoc.Campaigns.Version != manifest.Version {
			return Document{}, fmt.Errorf("load campaigns file %q: version %d does not match manifest version %d", sourcePath, campaignDoc.Campaigns.Version, manifest.Version)
		}
		sourceOrder := len(campaignPaths)
		for _, campaign := range campaignDoc.Campaigns.Campaigns {
			campaignKey := Fold(campaign.Name)
			if existing, ok := composed.Meta.CampaignSources[campaignKey]; ok {
				return Document{}, fmt.Errorf(
					"load campaigns file %q: duplicate campaign name %q already defined in %q",
					sourcePath,
					NormalizeName(campaign.Name),
					existing.SourcePath,
				)
			}
			composed.Meta.CampaignSources[campaignKey] = CampaignSource{
				SourcePath:  sourcePath,
				SourceOrder: sourceOrder,
			}
		}
		for _, generator := range campaignDoc.Campaigns.Generators {
			generator.SourcePath = sourcePath
			generator.SourceOrder = sourceOrder
			composed.Generators = append(composed.Generators, generator)
		}
		composed.Campaigns = append(composed.Campaigns, campaignDoc.Campaigns.Campaigns...)
		campaignPaths = append(campaignPaths, campaignPath)
	}
	return Document{
		Type:          DocumentTypeManifest,
		Path:          path,
		Spec:          composed,
		Manifest:      manifest,
		BasePath:      basePath,
		CampaignPaths: campaignPaths,
	}, nil
}

func parseRootNode(data []byte) (*yaml.Node, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("spec root must be a YAML mapping")
	}
	return root.Content[0], nil
}

func detectKind(root *yaml.Node) (Kind, error) {
	for i := 0; i+1 < len(root.Content); i += 2 {
		key := strings.TrimSpace(root.Content[i].Value)
		if key != "kind" {
			continue
		}
		value := strings.TrimSpace(root.Content[i+1].Value)
		switch Kind(value) {
		case KindConfig, KindManifest, KindBase, KindCampaigns:
			return Kind(value), nil
		case "":
			return "", errors.New(`decode spec: kind is required; use "Config", "Manifest", "Base", or "Campaigns"`)
		case "Composition":
			return "", errors.New(`decode spec: kind "Composition" is no longer supported; use "Manifest"`)
		case "CampaignFragment":
			return "", errors.New(`decode spec: kind "CampaignFragment" is no longer supported; use "Campaigns"`)
		default:
			return "", fmt.Errorf("decode spec: unsupported kind %q", value)
		}
	}
	return "", errors.New(`decode spec: kind is required; use "Config", "Manifest", "Base", or "Campaigns"`)
}

func decodeConfig(data []byte) (Spec, error) {
	var loaded Spec
	if err := decodeKnownFields(data, &loaded); err != nil {
		return Spec{}, fmt.Errorf("decode spec: %w", err)
	}
	if loaded.Kind != KindConfig {
		return Spec{}, fmt.Errorf("decode spec: expected kind %q, got %q", KindConfig, loaded.Kind)
	}
	return loaded, nil
}

func decodeManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	if err := decodeKnownFields(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode spec: %w", err)
	}
	if manifest.Kind != KindManifest {
		return Manifest{}, fmt.Errorf("decode spec: expected kind %q, got %q", KindManifest, manifest.Kind)
	}
	return manifest, nil
}

func decodeBase(data []byte) (Base, error) {
	var base Base
	if err := decodeKnownFields(data, &base); err != nil {
		return Base{}, fmt.Errorf("decode spec: %w", err)
	}
	if base.Kind != KindBase {
		return Base{}, fmt.Errorf("decode spec: expected kind %q, got %q", KindBase, base.Kind)
	}
	return base, nil
}

func decodeCampaignsFile(data []byte) (CampaignsFile, error) {
	var campaigns CampaignsFile
	if err := decodeKnownFields(data, &campaigns); err != nil {
		return CampaignsFile{}, fmt.Errorf("decode spec: %w", err)
	}
	if campaigns.Kind != KindCampaigns {
		return CampaignsFile{}, fmt.Errorf("decode spec: expected kind %q, got %q", KindCampaigns, campaigns.Kind)
	}
	return campaigns, nil
}

func decodeKnownFields(data []byte, out any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	return decoder.Decode(out)
}

func hasRootKey(root *yaml.Node, key string) bool {
	for i := 0; i+1 < len(root.Content); i += 2 {
		if strings.TrimSpace(root.Content[i].Value) == key {
			return true
		}
	}
	return false
}

func rejectLegacyAppAdamID(root *yaml.Node) error {
	appNode, ok := lookupMappingValue(root, "app")
	if !ok || appNode.Kind != yaml.MappingNode {
		return nil
	}
	if _, ok := lookupMappingValue(appNode, "adam_id"); ok {
		return errors.New(`decode spec: field "app.adam_id" was renamed to "app.app_id"`)
	}
	return nil
}

func lookupMappingValue(node *yaml.Node, key string) (*yaml.Node, bool) {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return node.Content[i+1], true
		}
	}
	return nil, false
}

func resolveRelativePath(path, rootDir, ref string) (string, error) {
	if filepath.IsAbs(ref) {
		return ref, nil
	}
	if strings.TrimSpace(path) != "" && path != "-" {
		return filepath.Join(filepath.Dir(path), ref), nil
	}
	if strings.TrimSpace(rootDir) != "" {
		return filepath.Join(rootDir, ref), nil
	}
	return "", errors.New("relative includes require --root when loading from stdin")
}
