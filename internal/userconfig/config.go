package userconfig

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/robaerd/asactl/internal/auth"
	"github.com/robaerd/asactl/internal/spec"
)

const (
	DefaultFileName = "config.toml"
	DefaultDirName  = ".asactl"
	OverrideEnvVar  = "ASACTL_CONFIG"
)

type File struct {
	Version        int                `toml:"version" json:"version"`
	DefaultProfile string             `toml:"default_profile,omitempty" json:"default_profile,omitempty"`
	Profiles       map[string]Profile `toml:"profiles,omitempty" json:"profiles,omitempty"`
}

type Profile struct {
	ClientID       string `toml:"client_id,omitempty" json:"client_id,omitempty"`
	TeamID         string `toml:"team_id,omitempty" json:"team_id,omitempty"`
	KeyID          string `toml:"key_id,omitempty" json:"key_id,omitempty"`
	PrivateKeyPath string `toml:"private_key_path,omitempty" json:"private_key_path,omitempty"`
}

type Loaded struct {
	Path   string
	File   File
	Exists bool
}

type Runtime struct {
	Spec         spec.Spec
	AuthConfig   auth.Config
	ProfileName  string
	ConfigPath   string
	ConfigLoaded bool
}

func DefaultPath() (string, error) {
	root, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	return filepath.Join(root, DefaultDirName, DefaultFileName), nil
}

func ResolvePath() (string, error) {
	if override := strings.TrimSpace(os.Getenv(OverrideEnvVar)); override != "" {
		return filepath.Clean(override), nil
	}
	return DefaultPath()
}

func Load() (Loaded, error) {
	path, err := ResolvePath()
	if err != nil {
		return Loaded{}, err
	}
	return LoadPath(path)
}

func LoadPath(path string) (Loaded, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Loaded{Path: path, File: NewFile(""), Exists: false}, nil
		}
		return Loaded{}, fmt.Errorf("read config %q: %w", path, err)
	}
	decoder := toml.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var file File
	if err := decoder.Decode(&file); err != nil {
		return Loaded{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	if file.Version == 0 {
		file.Version = 1
	}
	if file.Version != 1 {
		return Loaded{}, fmt.Errorf("config %q version must be 1, got %d", path, file.Version)
	}
	if file.Profiles == nil {
		file.Profiles = map[string]Profile{}
	}
	return Loaded{Path: path, File: file, Exists: true}, nil
}

func SavePath(path string, file File) error {
	if file.Version == 0 {
		file.Version = 1
	}
	if file.Profiles == nil {
		file.Profiles = map[string]Profile{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	content, err := toml.Marshal(file)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write config %q: %w", path, err)
	}
	return nil
}

func NewFile(defaultProfile string) File {
	return File{
		Version:        1,
		DefaultProfile: strings.TrimSpace(defaultProfile),
		Profiles:       map[string]Profile{},
	}
}

func StarterProfile() Profile {
	return Profile{
		ClientID:       "YOUR_APPLE_ADS_CLIENT_ID",
		TeamID:         "YOUR_APPLE_ADS_TEAM_ID",
		KeyID:          "YOUR_APPLE_ADS_KEY_ID",
		PrivateKeyPath: "/absolute/path/to/appleads-private-key.pem",
	}
}

func ResolveRuntime(input spec.Spec, profileOverride string) (Runtime, error) {
	loaded, err := Load()
	if err != nil {
		return Runtime{}, err
	}
	profileName := ResolveProfileSelection(profileOverride, input.Auth.Profile, loaded.File.DefaultProfile)
	var selected Profile
	if profileName != "" {
		var ok bool
		selected, ok = loaded.File.Profiles[profileName]
		if !ok {
			return Runtime{}, fmt.Errorf("profile %q was not found in %s", profileName, loaded.Path)
		}
	}
	if strings.TrimSpace(input.CampaignGroup.ID) == "" {
		return Runtime{}, errors.New("campaign_group.id must be set in YAML")
	}
	authConfig, err := auth.ConfigFromInputs(auth.Inputs{
		ClientID:       strings.TrimSpace(selected.ClientID),
		TeamID:         strings.TrimSpace(selected.TeamID),
		KeyID:          strings.TrimSpace(selected.KeyID),
		PrivateKeyPath: strings.TrimSpace(selected.PrivateKeyPath),
	})
	if err != nil {
		return Runtime{}, err
	}
	return Runtime{
		Spec:         input,
		AuthConfig:   authConfig,
		ProfileName:  profileName,
		ConfigPath:   loaded.Path,
		ConfigLoaded: loaded.Exists,
	}, nil
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func ResolveProfileSelection(override, specProfile, defaultProfile string) string {
	return firstNonBlank(override, specProfile, defaultProfile, "default")
}

func (f File) SortedProfileNames() []string {
	names := make([]string, 0, len(f.Profiles))
	for name := range f.Profiles {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func RedactedProfile(profile Profile) Profile {
	redacted := profile
	if strings.TrimSpace(redacted.ClientID) != "" {
		redacted.ClientID = redactTrailing(redacted.ClientID, 4)
	}
	if strings.TrimSpace(redacted.TeamID) != "" {
		redacted.TeamID = redactTrailing(redacted.TeamID, 4)
	}
	if strings.TrimSpace(redacted.KeyID) != "" {
		redacted.KeyID = redactTrailing(redacted.KeyID, 4)
	}
	if strings.TrimSpace(redacted.PrivateKeyPath) != "" {
		redacted.PrivateKeyPath = "****"
	}
	return redacted
}

func redactTrailing(value string, keep int) string {
	if keep <= 0 {
		return "****"
	}
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= keep {
		return "****"
	}
	return "****" + string(runes[len(runes)-keep:])
}
