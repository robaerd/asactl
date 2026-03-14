package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
)

type fakeEditor struct {
	path   string
	err    error
	stdout string
	stderr string
}

func (f *fakeEditor) Edit(_ context.Context, path string, streams editorStreams) error {
	f.path = path
	if strings.TrimSpace(f.stdout) != "" {
		_, _ = io.WriteString(streams.Stdout, f.stdout)
	}
	if strings.TrimSpace(f.stderr) != "" {
		_, _ = io.WriteString(streams.Stderr, f.stderr)
	}
	return f.err
}

func TestConfigPathPrintsResolvedPath(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	root := &rootOptions{}
	cmd := newConfigPathCommand(root)
	stdout := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(ioDiscard)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config path: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != override {
		t.Fatalf("unexpected output %q", stdout.String())
	}
}

func TestConfigInitWritesStarterPlaceholders(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	t.Setenv("APPLE_ADS_CLIENT_ID", "client-id")
	t.Setenv("APPLE_ADS_TEAM_ID", "team-id")
	t.Setenv("APPLE_ADS_KEY_ID", "key-id")
	t.Setenv("APPLE_ADS_PRIVATE_KEY_PATH", "/tmp/private.pem")

	root := &rootOptions{}
	cmd := newConfigInitCommand(root)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(ioDiscard)
	cmd.SetArgs([]string{"--profile", "default"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config init: %v", err)
	}
	content, err := os.ReadFile(override)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(content)
	if !strings.Contains(text, "default_profile = 'default'") && !strings.Contains(text, "default_profile = \"default\"") {
		t.Fatalf("expected default profile in config, got %s", text)
	}
	if !strings.Contains(text, `client_id = "YOUR_APPLE_ADS_CLIENT_ID"`) && !strings.Contains(text, `client_id = 'YOUR_APPLE_ADS_CLIENT_ID'`) {
		t.Fatalf("expected placeholder client_id in config, got %s", text)
	}
	if !strings.Contains(text, `team_id = "YOUR_APPLE_ADS_TEAM_ID"`) && !strings.Contains(text, `team_id = 'YOUR_APPLE_ADS_TEAM_ID'`) {
		t.Fatalf("expected placeholder team_id in config, got %s", text)
	}
	if !strings.Contains(text, `key_id = "YOUR_APPLE_ADS_KEY_ID"`) && !strings.Contains(text, `key_id = 'YOUR_APPLE_ADS_KEY_ID'`) {
		t.Fatalf("expected placeholder key_id in config, got %s", text)
	}
	if !strings.Contains(text, `private_key_path = "/absolute/path/to/appleads-private-key.pem"`) && !strings.Contains(text, `private_key_path = '/absolute/path/to/appleads-private-key.pem'`) {
		t.Fatalf("expected placeholder private_key_path in config, got %s", text)
	}
}

func TestConfigInitForceOverwritesProfileWithStarterPlaceholders(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "live-client-id"
team_id = "live-team-id"
key_id = "live-key-id"
private_key_path = "/tmp/live.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	root := &rootOptions{}
	cmd := newConfigInitCommand(root)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(ioDiscard)
	cmd.SetArgs([]string{"--profile", "default", "--force"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config init --force: %v", err)
	}
	content, err := os.ReadFile(override)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(content)
	if !strings.Contains(text, `client_id = "YOUR_APPLE_ADS_CLIENT_ID"`) && !strings.Contains(text, `client_id = 'YOUR_APPLE_ADS_CLIENT_ID'`) {
		t.Fatalf("expected placeholder client_id after force init, got %s", text)
	}
	if strings.Contains(text, "live-client-id") {
		t.Fatalf("expected existing client_id to be overwritten, got %s", text)
	}
}

func TestConfigShowJSONReturnsStructuredPayload(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "very-secret-client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/private.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	root := &rootOptions{JSONOutput: true}
	cmd := newConfigShowCommand(root)
	stdout := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(ioDiscard)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config show: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode json output: %v", err)
	}
	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %+v", payload)
	}
	if payload["selected_profile"] != "default" {
		t.Fatalf("expected selected profile default, got %+v", payload)
	}
	profilePayload, ok := payload["profile"].(map[string]any)
	if !ok {
		t.Fatalf("expected profile payload, got %+v", payload["profile"])
	}
	if profilePayload["client_id"] != "****t-id" {
		t.Fatalf("expected redacted client id, got %+v", profilePayload)
	}
	if profilePayload["team_id"] != "****m-id" {
		t.Fatalf("expected redacted team id, got %+v", profilePayload)
	}
	if profilePayload["key_id"] != "****y-id" {
		t.Fatalf("expected redacted key id, got %+v", profilePayload)
	}
	if profilePayload["private_key_path"] != "****" {
		t.Fatalf("expected redacted private key path, got %+v", profilePayload)
	}
}

func TestConfigShowPrintsRedactedProfile(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "very-secret-client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/private.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	root := &rootOptions{}
	cmd := newConfigShowCommand(root)
	stdout := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(ioDiscard)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config show: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "Selected profile: default") {
		t.Fatalf("expected selected profile in output, got %q", output)
	}
	if strings.Contains(output, "very-secret-client-id") {
		t.Fatalf("expected client id to be redacted, got %q", output)
	}
	if !strings.Contains(output, "client_id: ****t-id") {
		t.Fatalf("expected redacted client id in output, got %q", output)
	}
	if !strings.Contains(output, "team_id: ****m-id") {
		t.Fatalf("expected redacted team id in output, got %q", output)
	}
	if !strings.Contains(output, "key_id: ****y-id") {
		t.Fatalf("expected redacted key id in output, got %q", output)
	}
	if !strings.Contains(output, "private_key_path: ****") {
		t.Fatalf("expected redacted private key path in output, got %q", output)
	}
}

func TestConfigEditUsesInjectedEditor(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	editor := &fakeEditor{}
	root := &rootOptions{Editor: editor}
	cmd := newConfigEditCommand(root)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(ioDiscard)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config edit: %v", err)
	}
	if editor.path != override {
		t.Fatalf("expected editor to open %q, got %q", override, editor.path)
	}
	content, err := os.ReadFile(override)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(content)
	if !strings.Contains(text, `client_id = "YOUR_APPLE_ADS_CLIENT_ID"`) && !strings.Contains(text, `client_id = 'YOUR_APPLE_ADS_CLIENT_ID'`) {
		t.Fatalf("expected placeholder client_id in created config, got %s", text)
	}
}

func TestConfigEditJSONKeepsStdoutMachineReadable(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	editor := &fakeEditor{stdout: "editor stdout\n", stderr: "editor stderr\n"}
	root := &rootOptions{Editor: editor, JSONOutput: true}
	cmd := newConfigEditCommand(root)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config edit --json: %v", err)
	}
	if strings.Contains(stdout.String(), "editor stdout") {
		t.Fatalf("expected stdout to contain only json, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "editor stdout") || !strings.Contains(stderr.String(), "editor stderr") {
		t.Fatalf("expected editor output on stderr, got %q", stderr.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode json output: %v", err)
	}
	if payload["ok"] != true {
		t.Fatalf("expected ok=true payload, got %+v", payload)
	}
}

func TestConfigEditDoesNotAcceptForceFlag(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	root := &rootOptions{Editor: &fakeEditor{}}
	cmd := newConfigEditCommand(root)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(ioDiscard)
	cmd.SetArgs([]string{"--force"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "unknown flag: --force") {
		t.Fatalf("expected unknown flag error, got %v", err)
	}
}

func TestConfigEditDoesNotMutateExistingProfileBeforeOpening(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "live-client-id"
team_id = "live-team-id"
key_id = "live-key-id"
private_key_path = "/tmp/live.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	editor := &fakeEditor{}
	root := &rootOptions{Editor: editor}
	cmd := newConfigEditCommand(root)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(ioDiscard)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("config edit: %v", err)
	}
	content, err := os.ReadFile(override)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(content)
	if !strings.Contains(text, `client_id = "live-client-id"`) {
		t.Fatalf("expected existing profile to remain unchanged, got %s", text)
	}
}

func TestConfigEditFailsWithoutConfiguredEditor(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")
	root := &rootOptions{Editor: envEditor{}}
	cmd := newConfigEditCommand(root)
	cmd.SetOut(&bytes.Buffer{})
	stderr := &bytes.Buffer{}
	cmd.SetErr(stderr)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "no editor is configured") {
		t.Fatalf("expected editor error, got %v", err)
	}
}

func TestEnvEditorFallsBackToVimWhenInteractive(t *testing.T) {
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")
	editor := envEditor{
		interactive: func() bool { return true },
		lookupPath: func(name string) (string, error) {
			if name == "vim" {
				return "/usr/bin/vim", nil
			}
			return "", fmt.Errorf("not found: %s", name)
		},
	}

	command, err := editor.editorCommand()
	if err != nil {
		t.Fatalf("editor command: %v", err)
	}
	if command != "/usr/bin/vim" {
		t.Fatalf("expected vim fallback, got %q", command)
	}
}

func TestEnvEditorDoesNotFallbackWhenNonInteractive(t *testing.T) {
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")
	editor := envEditor{
		interactive: func() bool { return false },
		lookupPath: func(name string) (string, error) {
			return "/usr/bin/" + name, nil
		},
	}

	_, err := editor.editorCommand()
	if err == nil || !strings.Contains(err.Error(), "no editor is configured") {
		t.Fatalf("expected non-interactive editor error, got %v", err)
	}
}

func TestEditorInvocationParsesQuotedExecutableAndArgs(t *testing.T) {
	const editorPath = `C:\Program Files\VS Code\Code.exe`

	command, args, err := editorInvocationWithLookup(`"`+editorPath+`" --wait`, `/tmp/config path.toml`, func(name string) (string, error) {
		if name == editorPath {
			return name, nil
		}
		return "", fmt.Errorf("not found: %s", name)
	})
	if err != nil {
		t.Fatalf("editor invocation: %v", err)
	}
	if command != editorPath {
		t.Fatalf("unexpected command %q", command)
	}
	want := []string{"--wait", `/tmp/config path.toml`}
	if !slices.Equal(args, want) {
		t.Fatalf("unexpected args %#v want %#v", args, want)
	}
}

func TestEditorInvocationParsesSingleQuotedArgs(t *testing.T) {
	command, args, err := editorInvocation(`nvim -u 'custom init.vim'`, "/tmp/config.toml")
	if err != nil {
		t.Fatalf("editor invocation: %v", err)
	}
	if command != "nvim" {
		t.Fatalf("unexpected command %q", command)
	}
	want := []string{"-u", "custom init.vim", "/tmp/config.toml"}
	if !slices.Equal(args, want) {
		t.Fatalf("unexpected args %#v want %#v", args, want)
	}
}

func TestEditorInvocationResolvesExecutablePathWithSpacesWithoutQuotes(t *testing.T) {
	const editorPath = `/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code`

	command, args, err := editorInvocationWithLookup(editorPath+` --wait`, `/tmp/config path.toml`, func(name string) (string, error) {
		if name == editorPath {
			return name, nil
		}
		return "", fmt.Errorf("not found: %s", name)
	})
	if err != nil {
		t.Fatalf("editor invocation: %v", err)
	}
	if command != editorPath {
		t.Fatalf("unexpected command %q", command)
	}
	want := []string{"--wait", `/tmp/config path.toml`}
	if !slices.Equal(args, want) {
		t.Fatalf("unexpected args %#v want %#v", args, want)
	}
}

func TestEditorInvocationAllowsLiteralDollarArgs(t *testing.T) {
	command, args, err := editorInvocation(`nvim +$`, "/tmp/config.toml")
	if err != nil {
		t.Fatalf("editor invocation: %v", err)
	}
	if command != "nvim" {
		t.Fatalf("unexpected command %q", command)
	}
	want := []string{"+$", "/tmp/config.toml"}
	if !slices.Equal(args, want) {
		t.Fatalf("unexpected args %#v want %#v", args, want)
	}
}

func TestEditorInvocationRejectsUnmatchedQuotes(t *testing.T) {
	_, _, err := editorInvocation(`"code --wait`, "/tmp/config.toml")
	if err == nil || !strings.Contains(err.Error(), "unmatched quotes") {
		t.Fatalf("expected unmatched quotes error, got %v", err)
	}
}

func TestEditorInvocationRejectsShellInterpreter(t *testing.T) {
	testCases := []string{
		`sh -c 'vim "$1"' --`,
		`bash -lc 'vim "$1"' --`,
		`zsh -ic 'vim "$1"' --`,
		`cmd /S /C code --wait`,
		`powershell -NoProfile -Command code --wait`,
		`/usr/bin/env bash -lc 'vim "$1"' --`,
	}

	for _, value := range testCases {
		t.Run(value, func(t *testing.T) {
			_, _, err := editorInvocation(value, "/tmp/config.toml")
			if err == nil || !strings.Contains(err.Error(), "shell-based editor commands are not supported") {
				t.Fatalf("expected shell interpreter rejection, got %v", err)
			}
		})
	}
}

func TestMaybeBootstrapRuntimeConfigGuidesMissingConfigForDefaultProfile(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	err := maybeBootstrapRuntimeConfig(spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "default"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "User config is missing") {
		t.Fatalf("expected missing-config guidance, got %v", err)
	}
	if !strings.Contains(err.Error(), "Run 'asactl config init' and then 'asactl config edit'") {
		t.Fatalf("expected config init/edit guidance, got %v", err)
	}
	if _, statErr := os.Stat(override); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected config file to remain missing, stat err=%v", statErr)
	}
}

func TestMaybeBootstrapRuntimeConfigGuidesMissingConfigForNamedProfile(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)

	err := maybeBootstrapRuntimeConfig(spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "prod"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), `profile "prod"`) {
		t.Fatalf("expected named-profile guidance, got %v", err)
	}
	if !strings.Contains(err.Error(), `Add profile "prod" or change your profile selection`) {
		t.Fatalf("expected add-profile guidance, got %v", err)
	}
	if _, statErr := os.Stat(override); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected config file to remain missing, stat err=%v", statErr)
	}
}

func TestMaybeBootstrapRuntimeConfigGuidesMissingProfile(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/private.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	err := maybeBootstrapRuntimeConfig(spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "prod"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "profile \"prod\" is missing") {
		t.Fatalf("expected missing profile guidance, got %v", err)
	}
	if !strings.Contains(err.Error(), "Run 'asactl config edit'") {
		t.Fatalf("expected config edit guidance, got %v", err)
	}
	if !strings.Contains(err.Error(), `add profile "prod" or change your profile selection`) {
		t.Fatalf("expected add-profile guidance, got %v", err)
	}
}

func TestMaybeBootstrapRuntimeConfigUsesProfileOverridePrecedence(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/private.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	err := maybeBootstrapRuntimeConfig(spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "prod"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "ops")
	if err == nil || !strings.Contains(err.Error(), `profile "ops" is missing`) {
		t.Fatalf("expected override profile guidance, got %v", err)
	}
}

func TestMaybeBootstrapRuntimeConfigFallsBackToImplicitDefaultProfile(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	if err := os.WriteFile(override, []byte(`
version = 1

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "/tmp/private.pem"
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := maybeBootstrapRuntimeConfig(spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, ""); err != nil {
		t.Fatalf("expected implicit default profile to pass preflight, got %v", err)
	}
}

var ioDiscard = &discardWriter{}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestEnvEditorReturnsConfiguredError(t *testing.T) {
	editor := &fakeEditor{err: errors.New("boom")}
	if err := editor.Edit(context.Background(), "/tmp/config.toml", editorStreams{}); err == nil {
		t.Fatal("expected fake editor error")
	}
}
