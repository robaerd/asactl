package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
)

type fakeEditor struct {
	path string
	err  error
}

func (f *fakeEditor) Edit(_ context.Context, path string) error {
	f.path = path
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

func TestConfigInitWritesStarterProfile(t *testing.T) {
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
	if !strings.Contains(text, "client_id = \"client-id\"") && !strings.Contains(text, "client_id = 'client-id'") {
		t.Fatalf("expected client_id in config, got %s", text)
	}
}

func TestConfigShowJSONRedactsClientID(t *testing.T) {
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
	var payload struct {
		Profile struct {
			ClientID string `json:"client_id"`
		} `json:"profile"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if payload.Profile.ClientID == "very-secret-client-id" || payload.Profile.ClientID == "" {
		t.Fatalf("expected redacted client id, got %q", payload.Profile.ClientID)
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

func TestMaybeBootstrapRuntimeConfigCreatesStarterFileAndOpensEditor(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)
	t.Setenv("APPLE_ADS_CLIENT_ID", "client-id")
	t.Setenv("APPLE_ADS_TEAM_ID", "team-id")
	t.Setenv("APPLE_ADS_KEY_ID", "key-id")
	t.Setenv("APPLE_ADS_PRIVATE_KEY_PATH", "/tmp/private.pem")

	editor := &fakeEditor{}
	err := maybeBootstrapRuntimeConfig(context.Background(), editor, spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "default"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "Created starter config") {
		t.Fatalf("expected bootstrap creation error, got %v", err)
	}
	if editor.path != override {
		t.Fatalf("expected editor to open %q, got %q", override, editor.path)
	}
	content, readErr := os.ReadFile(override)
	if readErr != nil {
		t.Fatalf("read config: %v", readErr)
	}
	text := string(content)
	if !strings.Contains(text, "default_profile = \"default\"") && !strings.Contains(text, "default_profile = 'default'") {
		t.Fatalf("expected default profile in config, got %s", text)
	}
}

func TestMaybeBootstrapRuntimeConfigCreatesStarterFileWithoutExplicitProfile(t *testing.T) {
	override := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, override)

	err := maybeBootstrapRuntimeConfig(context.Background(), &fakeEditor{}, spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: "20744842"},
		App:           spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "Created starter config") {
		t.Fatalf("expected bootstrap creation guidance, got %v", err)
	}
	if _, statErr := os.Stat(override); statErr != nil {
		t.Fatalf("expected config file creation, stat err=%v", statErr)
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

	editor := &fakeEditor{}
	err := maybeBootstrapRuntimeConfig(context.Background(), editor, spec.Spec{
		Version: 1,
		Kind:    spec.KindConfig,
		Auth:    spec.Auth{Profile: "prod"},
		App:     spec.App{Name: "Readcap", AppID: "123456"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "profile \"prod\" is missing") {
		t.Fatalf("expected missing profile guidance, got %v", err)
	}
	if editor.path != override {
		t.Fatalf("expected editor to open %q, got %q", override, editor.path)
	}
}

var ioDiscard = &discardWriter{}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestEnvEditorReturnsConfiguredError(t *testing.T) {
	editor := &fakeEditor{err: errors.New("boom")}
	if err := editor.Edit(context.Background(), "/tmp/config.toml"); err == nil {
		t.Fatal("expected fake editor error")
	}
}
