package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/robaerd/asactl/internal/logging"
	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
	"github.com/spf13/cobra"
)

type configEditor interface {
	Edit(context.Context, string, editorStreams) error
}

type editorStreams struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type envEditor struct {
	lookupPath  func(string) (string, error)
	interactive func() bool
}

func newConfigCommand(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage global asactl user configuration.",
		Long:  "Manage the global TOML user configuration used for Apple Ads credentials and default profile selection. This is the primary packaged setup path for check-auth, plan, and apply.",
	}
	cmd.AddCommand(newConfigPathCommand(root))
	cmd.AddCommand(newConfigInitCommand(root))
	cmd.AddCommand(newConfigEditCommand(root))
	cmd.AddCommand(newConfigShowCommand(root))
	return cmd
}

func newConfigPathCommand(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "path",
		Short: "Print the resolved user config path.",
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := userconfig.ResolvePath()
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"ok": true, "path": path})
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), path)
			return err
		},
	}
}

func newConfigInitCommand(root *rootOptions) *cobra.Command {
	var profile string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create or update the global user config.",
		Long:  "Create the global TOML user config and write starter placeholder auth fields for the selected profile. Existing profiles are preserved unless --force overwrites the selected profile.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			selectedProfile := strings.TrimSpace(profile)
			if selectedProfile == "" {
				selectedProfile = "default"
			}
			loaded, err := userconfig.Load()
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			file := loaded.File
			if !loaded.Exists {
				file = userconfig.NewFile(selectedProfile)
			}
			if file.DefaultProfile == "" {
				file.DefaultProfile = selectedProfile
			}
			if _, exists := file.Profiles[selectedProfile]; exists && !force {
				err := fmt.Errorf("profile %q already exists in %s; use --force to overwrite it", selectedProfile, loaded.Path)
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			file.Profiles[selectedProfile] = userconfig.StarterProfile()
			if err := userconfig.SavePath(loaded.Path, file); err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			logger.Debug("User config initialized", "path", loaded.Path, "profile", selectedProfile, "force", force)
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"ok": true, "path": loaded.Path, "profile": selectedProfile, "created": !loaded.Exists})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Wrote %s (profile %s)\n", loaded.Path, selectedProfile)
			return err
		},
	}
	cmd.Flags().StringVar(&profile, "profile", "default", "profile name to initialize")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite the selected profile if it already exists")
	return cmd
}

func newConfigEditCommand(root *rootOptions) *cobra.Command {
	var profile string
	cmd := &cobra.Command{
		Use:   "edit",
		Short: "Open the global user config in an editor.",
		Long:  "Open the global TOML user config using $VISUAL or $EDITOR. Editor commands are executed directly without a shell, so direct executable paths and normal args are supported, but shell launchers are rejected. If neither editor variable is configured and the terminal is interactive, asactl falls back to nvim, vim, then vi. If the config file does not exist yet, a starter config with placeholder auth fields is initialized first.",
		RunE: func(cmd *cobra.Command, args []string) error {
			selectedProfile := strings.TrimSpace(profile)
			if selectedProfile == "" {
				selectedProfile = "default"
			}
			loaded, err := userconfig.Load()
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			if !loaded.Exists {
				file := userconfig.NewFile(selectedProfile)
				file.Profiles[selectedProfile] = userconfig.StarterProfile()
				if err := userconfig.SavePath(loaded.Path, file); err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
				}
			}
			streams := editorStreams{
				Stdin:  cmd.InOrStdin(),
				Stdout: cmd.OutOrStdout(),
				Stderr: cmd.ErrOrStderr(),
			}
			if root.JSONOutput {
				streams.Stdout = cmd.ErrOrStderr()
			}
			if err := root.Editor.Edit(cmd.Context(), loaded.Path, streams); err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"ok": true, "path": loaded.Path, "profile": selectedProfile})
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&profile, "profile", "default", "profile name to initialize when the config file is missing")
	return cmd
}

func newConfigShowCommand(root *rootOptions) *cobra.Command {
	var profile string
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Show the resolved user config.",
		Long:  "Show the global TOML user config and the currently selected profile. Sensitive fields are redacted in command output.",
		RunE: func(cmd *cobra.Command, args []string) error {
			loaded, err := userconfig.Load()
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			selectedProfile := strings.TrimSpace(profile)
			if selectedProfile == "" {
				selectedProfile = strings.TrimSpace(loaded.File.DefaultProfile)
			}
			var selected userconfig.Profile
			if selectedProfile != "" {
				profileValue, ok := loaded.File.Profiles[selectedProfile]
				if !ok {
					err := fmt.Errorf("profile %q was not found in %s", selectedProfile, loaded.Path)
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
				}
				selected = userconfig.RedactedProfile(profileValue)
			}
			if root.JSONOutput {
				payload := map[string]any{
					"ok":               true,
					"path":             loaded.Path,
					"exists":           loaded.Exists,
					"default_profile":  loaded.File.DefaultProfile,
					"selected_profile": selectedProfile,
				}
				if selectedProfile != "" {
					payload["profile"] = selected
				}
				return writeJSON(cmd.OutOrStdout(), payload)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Path: %s\n", loaded.Path)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Exists: %t\n", loaded.Exists)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Default profile: %s\n", loaded.File.DefaultProfile)
			if selectedProfile != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Selected profile: %s\n", selectedProfile)
				printConfigProfile(cmd.OutOrStdout(), selected)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&profile, "profile", "", "profile name to show; defaults to the config default_profile")
	return cmd
}

func printConfigProfile(output io.Writer, profile userconfig.Profile) {
	_, _ = fmt.Fprintf(output, "client_id: %s\n", profile.ClientID)
	_, _ = fmt.Fprintf(output, "team_id: %s\n", profile.TeamID)
	_, _ = fmt.Fprintf(output, "key_id: %s\n", profile.KeyID)
	_, _ = fmt.Fprintf(output, "private_key_path: %s\n", profile.PrivateKeyPath)
}

func (e envEditor) Edit(ctx context.Context, path string, streams editorStreams) error {
	editor, err := e.editorCommand()
	if err != nil {
		return err
	}
	command, args, err := editorInvocation(editor, path)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdin = streams.Stdin
	cmd.Stdout = streams.Stdout
	cmd.Stderr = streams.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("open editor: %w", err)
	}
	return nil
}

func (e envEditor) editorCommand() (string, error) {
	editor := strings.TrimSpace(os.Getenv("VISUAL"))
	if editor == "" {
		editor = strings.TrimSpace(os.Getenv("EDITOR"))
	}
	if editor != "" {
		return editor, nil
	}
	if !e.isInteractive() {
		return "", errors.New("no editor is configured; set VISUAL or EDITOR, or install nvim, vim, or vi")
	}
	for _, candidate := range []string{"nvim", "vim", "vi"} {
		if resolved, err := e.lookUpPath(candidate); err == nil && strings.TrimSpace(resolved) != "" {
			return resolved, nil
		}
	}
	return "", errors.New("no editor is configured; set VISUAL or EDITOR, or install nvim, vim, or vi")
}

func (e envEditor) lookUpPath(name string) (string, error) {
	if e.lookupPath != nil {
		return e.lookupPath(name)
	}
	return exec.LookPath(name)
}

func (e envEditor) isInteractive() bool {
	if e.interactive != nil {
		return e.interactive()
	}
	return stdioIsInteractive()
}

func stdioIsInteractive() bool {
	return isTerminalFile(os.Stdin) && isTerminalFile(os.Stdout) && isTerminalFile(os.Stderr)
}

func isTerminalFile(file *os.File) bool {
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func maybeBootstrapRuntimeConfig(input spec.Spec, profileOverride string) error {
	loaded, err := userconfig.Load()
	if err != nil {
		return err
	}

	selectedProfile := userconfig.ResolveProfileSelection(profileOverride, input.Auth.Profile, loaded.File.DefaultProfile)

	if !loaded.Exists {
		return runtimeConfigSetupError{
			path:          loaded.Path,
			profile:       selectedProfile,
			missingConfig: true,
		}
	}

	if _, ok := loaded.File.Profiles[selectedProfile]; ok {
		return nil
	}

	return runtimeConfigSetupError{
		path:           loaded.Path,
		profile:        selectedProfile,
		missingProfile: true,
	}
}

type runtimeConfigSetupError struct {
	path           string
	profile        string
	missingConfig  bool
	missingProfile bool
}

func (e runtimeConfigSetupError) Error() string {
	switch {
	case e.missingConfig && e.profile == "default":
		return fmt.Sprintf("User config is missing at %s for profile %q. Run 'asactl config init' and then 'asactl config edit', then rerun the command.", e.path, e.profile)
	case e.missingConfig:
		return fmt.Sprintf("User config is missing at %s for profile %q. Run 'asactl config init' and then 'asactl config edit'. Add profile %q or change your profile selection, then rerun the command.", e.path, e.profile, e.profile)
	case e.missingProfile:
		return fmt.Sprintf("User config exists at %s but profile %q is missing. Run 'asactl config edit', add profile %q or change your profile selection, then rerun the command.", e.path, e.profile, e.profile)
	default:
		return fmt.Sprintf("User config setup is incomplete at %s for profile %q. Run 'asactl config edit', then rerun the command.", e.path, e.profile)
	}
}

func editorInvocation(editor, path string) (string, []string, error) {
	return editorInvocationWithLookup(editor, path, exec.LookPath)
}

func editorInvocationWithLookup(editor, path string, lookPath func(string) (string, error)) (string, []string, error) {
	args, err := splitCommandArgs(editor)
	if err != nil {
		return "", nil, err
	}
	command, args, err := resolveEditorCommand(args, lookPath)
	if err != nil {
		return "", nil, err
	}
	if usesShellInterpreter(command, args) {
		return "", nil, errors.New("shell-based editor commands are not supported; set VISUAL or EDITOR to an editor executable plus args")
	}
	return command, append(args, path), nil
}

func splitCommandArgs(raw string) ([]string, error) {
	input := strings.TrimSpace(raw)
	if input == "" {
		return nil, errors.New("editor command must not be blank")
	}

	var (
		args       []string
		current    strings.Builder
		inSingle   bool
		inDouble   bool
		quotedPart bool
	)

	flush := func(force bool) {
		if !force && current.Len() == 0 && !quotedPart {
			return
		}
		args = append(args, current.String())
		current.Reset()
		quotedPart = false
	}

	for i := 0; i < len(input); i++ {
		ch := input[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
				quotedPart = true
				continue
			}
			current.WriteByte(ch)
		case inDouble:
			switch ch {
			case '"':
				inDouble = false
				quotedPart = true
			case '\\':
				if i+1 < len(input) && (input[i+1] == '"' || input[i+1] == '\\') {
					i++
					current.WriteByte(input[i])
					continue
				}
				current.WriteByte(ch)
			default:
				current.WriteByte(ch)
			}
		default:
			switch {
			case isShellSpace(ch):
				flush(false)
			case ch == '\'':
				inSingle = true
			case ch == '"':
				inDouble = true
			default:
				current.WriteByte(ch)
			}
		}
	}

	if inSingle || inDouble {
		return nil, fmt.Errorf("editor command %q has unmatched quotes", raw)
	}
	flush(false)
	return args, nil
}

func resolveEditorCommand(args []string, lookPath func(string) (string, error)) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("no editor is configured; set VISUAL or EDITOR, or install nvim, vim, or vi")
	}
	if !looksLikeExecutablePath(args[0]) {
		return args[0], args[1:], nil
	}

	for boundary := len(args); boundary >= 1; boundary-- {
		candidate := strings.Join(args[:boundary], " ")
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		if _, err := lookPath(candidate); err == nil {
			return candidate, args[boundary:], nil
		}
	}

	return "", nil, fmt.Errorf("editor executable path %q could not be resolved; quote paths with spaces or set VISUAL/EDITOR to a direct executable path", args[0])
}

func isShellSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func looksLikeExecutablePath(value string) bool {
	if strings.ContainsAny(value, `/\`) {
		return true
	}
	return len(value) >= 2 && value[1] == ':' && ((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z'))
}

func usesShellInterpreter(command string, args []string) bool {
	if commandNameKey(command) == "env" {
		wrapped, _, ok := resolveEnvWrappedCommand(args)
		if !ok {
			return false
		}
		command = wrapped
	}

	switch commandNameKey(command) {
	case "sh", "bash", "zsh", "dash", "fish", "ksh", "cmd", "powershell", "pwsh":
		return true
	default:
		return false
	}
}

func resolveEnvWrappedCommand(args []string) (string, []string, bool) {
	skipValue := false
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if skipValue {
			skipValue = false
			continue
		}
		if arg == "--" {
			if index+1 >= len(args) {
				return "", nil, false
			}
			return args[index+1], args[index+2:], true
		}
		switch arg {
		case "-u", "-C", "-S", "--unset", "--chdir", "--split-string":
			skipValue = true
			continue
		}
		if strings.HasPrefix(arg, "--unset=") || strings.HasPrefix(arg, "--chdir=") || strings.HasPrefix(arg, "--split-string=") {
			continue
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		if isEnvAssignment(arg) {
			continue
		}
		return arg, args[index+1:], true
	}
	return "", nil, false
}

func isEnvAssignment(value string) bool {
	if len(value) < 3 {
		return false
	}
	separator := strings.IndexByte(value, '=')
	if separator <= 0 {
		return false
	}
	for index := 0; index < separator; index++ {
		ch := value[index]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9' && index > 0) || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func commandNameKey(command string) string {
	key := strings.ToLower(strings.TrimSpace(command))
	if separator := strings.LastIndexAny(key, `/\`); separator >= 0 {
		key = key[separator+1:]
	}
	switch key {
	case "cmd.exe":
		return "cmd"
	case "powershell.exe":
		return "powershell"
	case "pwsh.exe":
		return "pwsh"
	default:
		return key
	}
}
