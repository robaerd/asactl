package userconfig_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
)

func TestDefaultPathUsesDotdirInHome(t *testing.T) {
	home := t.TempDir()
	switch runtime.GOOS {
	case "windows":
		t.Setenv("USERPROFILE", home)
		t.Setenv("HOME", "")
		t.Setenv("HOMEDRIVE", "")
		t.Setenv("HOMEPATH", "")
	default:
		t.Setenv("HOME", home)
	}

	path, err := userconfig.DefaultPath()
	if err != nil {
		t.Fatalf("default path: %v", err)
	}

	want := filepath.Join(home, ".asactl", "config.toml")
	if path != want {
		t.Fatalf("expected %q, got %q", want, path)
	}
}

func TestResolvePathUsesOverride(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)

	path, err := userconfig.ResolvePath()
	if err != nil {
		t.Fatalf("resolve path: %v", err)
	}
	if path != override {
		t.Fatalf("expected %q, got %q", override, path)
	}
}

func TestStarterProfileUsesDocumentedPlaceholders(t *testing.T) {
	t.Setenv("APPLE_ADS_CLIENT_ID", "client-id")
	t.Setenv("APPLE_ADS_TEAM_ID", "team-id")
	t.Setenv("APPLE_ADS_KEY_ID", "key-id")
	t.Setenv("APPLE_ADS_PRIVATE_KEY_PATH", "/tmp/private.pem")

	profile := userconfig.StarterProfile()
	if profile.ClientID != "YOUR_APPLE_ADS_CLIENT_ID" {
		t.Fatalf("unexpected client_id %q", profile.ClientID)
	}
	if profile.TeamID != "YOUR_APPLE_ADS_TEAM_ID" {
		t.Fatalf("unexpected team_id %q", profile.TeamID)
	}
	if profile.KeyID != "YOUR_APPLE_ADS_KEY_ID" {
		t.Fatalf("unexpected key_id %q", profile.KeyID)
	}
	if profile.PrivateKeyPath != "/absolute/path/to/appleads-private-key.pem" {
		t.Fatalf("unexpected private_key_path %q", profile.PrivateKeyPath)
	}
}

func TestResolveRuntimeUsesProfileCredentials(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, configPath)
	if err := os.WriteFile(configPath, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "`+filepath.ToSlash(writePrivateKey(t))+`"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	runtime, err := userconfig.ResolveRuntime(spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: "20744842"},
		Auth:          spec.Auth{Profile: "default"},
		App:           spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err != nil {
		t.Fatalf("resolve runtime: %v", err)
	}
	if runtime.Spec.CampaignGroup.ID != "20744842" {
		t.Fatalf("unexpected campaign_group.id %q", runtime.Spec.CampaignGroup.ID)
	}
	if runtime.AuthConfig.ClientID != "client-id" {
		t.Fatalf("unexpected client_id %q", runtime.AuthConfig.ClientID)
	}
}

func TestResolveRuntimeIgnoresRemovedEndpointOverrides(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, configPath)
	profileKey := writePrivateKey(t)
	if err := os.WriteFile(configPath, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "`+filepath.ToSlash(profileKey)+`"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	runtime, err := userconfig.ResolveRuntime(spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: "20744842"},
		Auth:          spec.Auth{Profile: "default"},
		App:           spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err != nil {
		t.Fatalf("resolve runtime: %v", err)
	}
	if runtime.AuthConfig.ClientID != "client-id" {
		t.Fatalf("expected profile client id, got %q", runtime.AuthConfig.ClientID)
	}
}

func TestResolveRuntimeRequiresCampaignGroupIDInYAML(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, configPath)
	if err := os.WriteFile(configPath, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "`+filepath.ToSlash(writePrivateKey(t))+`"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := userconfig.ResolveRuntime(spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "default"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || err.Error() != "campaign_group.id must be set in YAML" {
		t.Fatalf("expected missing campaign_group.id error, got %v", err)
	}
}

func TestLoadPathRejectsUnknownProfileFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		field string
	}{
		{name: "unexpected", field: "unexpected"},
		{name: "legacy-looking", field: "scope_id"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(configPath, []byte(`
version = 1
default_profile = "default"

[profiles.default]
`+tc.field+` = "20744842"
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/private.pem"
`), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			_, err := userconfig.LoadPath(configPath)
			if err == nil || !strings.Contains(err.Error(), "strict mode") {
				t.Fatalf("expected unknown-field decode failure for %q, got %v", tc.field, err)
			}
		})
	}
}

func writePrivateKey(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appleads-private.pem")
	if err := os.WriteFile(path, []byte("-----BEGIN PRIVATE KEY-----\nTEST\n-----END PRIVATE KEY-----\n"), 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	return path
}
