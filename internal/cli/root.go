package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	clonepkg "github.com/robaerd/asactl/internal/clone"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/logging"
	"github.com/robaerd/asactl/internal/spec"
	syncpkg "github.com/robaerd/asactl/internal/sync"
	"github.com/robaerd/asactl/internal/validate"
	"github.com/spf13/cobra"
)

type rootOptions struct {
	JSONOutput        bool
	Verbose           bool
	Prompter          applyPrompter
	Editor            configEditor
	SyncEngineOptions []syncpkg.EngineOption
}

type applyPrompter interface {
	Confirm(context.Context, io.Reader, io.Writer, string) (bool, error)
}

type stdioPrompter struct{}

type RootDependencies struct {
	SyncEngineOptions []syncpkg.EngineOption
}

func NewRootCommand(version string) *cobra.Command {
	return NewRootCommandWithDeps(version, RootDependencies{})
}

func NewRootCommandWithDeps(version string, dependencies RootDependencies) *cobra.Command {
	options := &rootOptions{
		Prompter:          stdioPrompter{},
		Editor:            envEditor{},
		SyncEngineOptions: slices.Clone(dependencies.SyncEngineOptions),
	}
	trimmedVersion := strings.TrimSpace(version)
	if trimmedVersion == "" {
		trimmedVersion = "dev"
	}
	cmd := &cobra.Command{
		Use:           "asactl",
		Short:         "Manage Apple Ads from YAML desired-state files.",
		Long:          "Manage Apple Ads from pure YAML desired-state files. Validate specs, verify auth and scope, diff against live Apple Ads state, apply changes, clone market configs, and format YAML canonically.",
		Version:       trimmedVersion,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.PersistentFlags().BoolVar(&options.JSONOutput, "json", false, "emit command output as JSON")
	cmd.PersistentFlags().BoolVar(&options.Verbose, "verbose", false, "enable verbose logs")
	cmd.AddCommand(newConfigCommand(options))
	cmd.AddCommand(newValidateCommand(options))
	cmd.AddCommand(newCheckAuthCommand(options))
	cmd.AddCommand(newPlanCommand(options))
	cmd.AddCommand(newApplyCommand(options))
	cmd.AddCommand(newCloneCommand(options))
	cmd.AddCommand(newFmtCommand(options))
	return cmd
}

func (root *rootOptions) newSyncEngine(stderr io.Writer) *syncpkg.Engine {
	return syncpkg.NewEngine(
		logging.New(stderr, logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}),
		root.SyncEngineOptions...,
	)
}

func newCheckAuthCommand(root *rootOptions) *cobra.Command {
	var rootDir string
	var profile string
	cmd := &cobra.Command{
		Use:   "check-auth <config.yaml>",
		Args:  cobra.ExactArgs(1),
		Short: "Verify Apple Ads auth, organization access, app scope, and product pages.",
		Long:  "Resolve auth configuration from the YAML spec, perform Apple Ads OAuth, confirm access to the configured organization, fetch product pages for the configured app, and summarize the currently managed campaign scope without mutating anything.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			logger.Debug("Authentication check command started", "spec_path", args[0])
			loaded, err := loadSpecArg(args[0], rootDir, cmd.InOrStdin())
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			if err := maybeBootstrapRuntimeConfig(cmd.Context(), root.Editor, loaded, profile); err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			engine := root.newSyncEngine(cmd.ErrOrStderr())
			result, err := engine.CheckAuth(cmd.Context(), loaded, profile)
			if err != nil {
				payload := map[string]any{
					"ok":                  false,
					"error":               err.Error(),
					"campaign_group_id":   result.CampaignGroupID,
					"org_name":            result.OrgName,
					"app_id":              result.AppID,
					"product_pages":       result.ProductPages,
					"product_page_count":  result.ProductPageCount,
					"scope_summary":       result.ScopeSummary,
					"managed_campaigns":   result.ManagedCampaigns,
					"other_app_campaigns": result.OtherAppCampaigns,
					"warnings":            result.Warnings,
				}
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), payload, err)
			}
			logger.Debug("Authentication check command completed", "campaign_group_id", result.CampaignGroupID, "org_name", result.OrgName, "product_page_count", result.ProductPageCount)
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), map[string]any{
					"ok":                  true,
					"campaign_group_id":   result.CampaignGroupID,
					"org_name":            result.OrgName,
					"app_id":              result.AppID,
					"product_pages":       result.ProductPages,
					"product_page_count":  result.ProductPageCount,
					"scope_summary":       result.ScopeSummary,
					"managed_campaigns":   result.ManagedCampaigns,
					"other_app_campaigns": result.OtherAppCampaigns,
					"warnings":            result.Warnings,
				})
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Authentication check passed.")
			fmt.Fprintf(cmd.OutOrStdout(), "Organization: %s (%s)\n", result.OrgName, result.CampaignGroupID)
			fmt.Fprintf(cmd.OutOrStdout(), "App ID: %s\n", result.AppID)
			fmt.Fprintf(cmd.OutOrStdout(), "Product pages: %d\n", result.ProductPageCount)
			for _, page := range result.ProductPages {
				fmt.Fprintf(cmd.OutOrStdout(), "- %s (%s)", page.Name, page.ID)
				if strings.TrimSpace(page.State) != "" {
					fmt.Fprintf(cmd.OutOrStdout(), " state=%s", page.State)
				}
				fmt.Fprintln(cmd.OutOrStdout())
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Scope: managed campaigns=%d, other-app campaigns=%d\n", result.ScopeSummary.ManagedCampaignCount, result.ScopeSummary.OtherAppCampaignCount)
			if len(result.Warnings) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "Warnings:")
				for _, warning := range result.Warnings {
					fmt.Fprintf(cmd.OutOrStdout(), "- %s\n", warning)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&rootDir, "root", "", "root directory for resolving relative includes when reading from stdin")
	cmd.Flags().StringVar(&profile, "profile", "", "user config profile to use for runtime auth and org resolution")
	return cmd
}

func newValidateCommand(root *rootOptions) *cobra.Command {
	var rootDir string
	cmd := &cobra.Command{
		Use:   "validate <config.yaml>",
		Args:  cobra.ExactArgs(1),
		Short: "Validate YAML schema and business rules.",
		Long:  "Validate the YAML desired state with strict schema decoding and Apple Ads business rules, including keyword uniqueness, targeting constraints, product-page references, and scale limits.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			logger.Debug("Validation started", "spec_path", args[0])
			loaded, err := loadSpecArg(args[0], rootDir, cmd.InOrStdin())
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			result := validate.Run(loaded)
			logger.Debug("Validation completed", "warnings", len(result.Warnings), "errors", len(result.Errors))
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"ok": result.OK(), "warnings": result.Warnings, "errors": result.Errors})
			}
			if len(result.Warnings) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "Validation warnings:")
				for _, warning := range result.Warnings {
					fmt.Fprintf(cmd.OutOrStdout(), "- %s\n", warning)
				}
			}
			if !result.OK() {
				fmt.Fprintln(cmd.OutOrStdout(), "Validation errors:")
				for _, issue := range result.Errors {
					fmt.Fprintf(cmd.OutOrStdout(), "- %s\n", issue)
				}
				return errors.New("validation failed")
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Validation passed.")
			return nil
		},
	}
	cmd.Flags().StringVar(&rootDir, "root", "", "root directory for resolving relative includes when reading from stdin")
	return cmd
}

func newPlanCommand(root *rootOptions) *cobra.Command {
	var recreate bool
	var wipeOrg bool
	var rootDir string
	var profile string
	var outPath string
	cmd := &cobra.Command{
		Use:   "plan <config.yaml>",
		Args:  cobra.ExactArgs(1),
		Short: "Fetch remote state and print a plan diff.",
		Long:  "Fetch live Apple Ads state for the configured campaign group and app scope, then compare it to the YAML desired state. By default, stale managed resources are hard-deleted; use --recreate or --wipe-org for broader rebuild modes. Use --out to save an explicit plan artifact that apply can replay without re-planning.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			scope, err := recreateScopeFromFlags(recreate, wipeOrg)
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error(), "recreate": recreate, "wipe_org": wipeOrg}, err)
			}
			logger.Debug("Plan command started", "spec_path", args[0], "recreate_scope", scope)
			loaded, err := loadSpecArg(args[0], rootDir, cmd.InOrStdin())
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error(), "recreate": recreate, "wipe_org": wipeOrg}, err)
			}
			if err := maybeBootstrapRuntimeConfig(cmd.Context(), root.Editor, loaded, profile); err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error(), "recreate": recreate, "wipe_org": wipeOrg}, err)
			}
			engine := root.newSyncEngine(cmd.ErrOrStderr())
			var result syncpkg.Result
			var saved syncpkg.SavedPlan
			if strings.TrimSpace(outPath) != "" {
				result, saved, err = engine.PlanSaved(cmd.Context(), loaded, syncpkg.Options{RecreateScope: scope, Profile: profile})
			} else {
				result, err = engine.Plan(cmd.Context(), loaded, syncpkg.Options{RecreateScope: scope, Profile: profile})
			}
			if err != nil {
				fields := map[string]any{"ok": false, "error": err.Error()}
				if strings.TrimSpace(outPath) != "" {
					fields["plan_file"] = outPath
				}
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, scope, fields), err)
			}
			if strings.TrimSpace(outPath) != "" {
				if err := writeSavedPlanFile(outPath, saved); err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, scope, map[string]any{"ok": false, "error": err.Error(), "plan_file": outPath}), err)
				}
				logger.Info("Saved plan written", "plan_file", outPath, "summary", diff.RenderSummary(result.Plan))
			}
			logger.Debug("Plan command completed", "summary", diff.RenderSummary(result.Plan), "warnings", len(result.Warnings), "plan_file", strings.TrimSpace(outPath))
			if root.JSONOutput {
				fields := map[string]any{"ok": true}
				if strings.TrimSpace(outPath) != "" {
					fields["plan_file"] = outPath
				}
				return writeJSON(cmd.OutOrStdout(), resultPayload(result, scope, fields))
			}
			printScopeSummary(cmd.OutOrStdout(), result)
			for _, warning := range result.Warnings {
				fmt.Fprintf(cmd.OutOrStdout(), "Warning: %s\n", warning)
			}
			fmt.Fprintln(cmd.OutOrStdout(), diff.RenderStyledText(result.Plan, diff.RenderOptions{Color: logging.ColorEnabled(cmd.OutOrStdout())}))
			return nil
		},
	}
	cmd.Flags().BoolVar(&recreate, "recreate", false, "delete all managed campaigns in the configured campaign_group.id + app.app_id scope before recreating the YAML-defined state")
	cmd.Flags().BoolVar(&wipeOrg, "wipe-org", false, "delete all remote campaigns visible in the configured organization before recreating the YAML-defined state")
	cmd.Flags().StringVar(&profile, "profile", "", "user config profile to use for runtime auth and org resolution")
	cmd.Flags().StringVar(&rootDir, "root", "", "root directory for resolving relative includes when reading from stdin")
	cmd.Flags().StringVar(&outPath, "out", "", "write the planned result to a saved plan file for explicit apply replay")
	cmd.MarkFlagsMutuallyExclusive("recreate", "wipe-org")
	return cmd
}

func newApplyCommand(root *rootOptions) *cobra.Command {
	var dryRun bool
	var maxChanges int
	var recreate bool
	var wipeOrg bool
	var yes bool
	var rootDir string
	var profile string
	cmd := &cobra.Command{
		Use:   "apply <config.yaml|planfile>",
		Args:  cobra.ExactArgs(1),
		Short: "Apply the diff to Apple Ads.",
		Long:  "Apply Apple Ads changes either from a fresh YAML desired state or from an explicit saved plan file produced by plan --out. Saved plan replay does not re-plan or refresh remote state, and it rejects scope/profile/root overrides.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			scope, err := recreateScopeFromFlags(recreate, wipeOrg)
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error(), "recreate": recreate, "wipe_org": wipeOrg}, err)
			}
			logger.Debug("Apply command started", "input_path", args[0], "recreate_scope", scope, "dry_run", dryRun, "max_changes", maxChanges)
			target, err := loadApplyTarget(args[0], rootDir, cmd.InOrStdin())
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error(), "recreate": recreate, "wipe_org": wipeOrg}, err)
			}
			engine := root.newSyncEngine(cmd.ErrOrStderr())

			var result syncpkg.Result
			applyScope := scope
			if target.IsSavedPlan {
				if err := validateSavedPlanApplyFlags(profile, scope, rootDir); err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(target.SavedPlan.Result(), target.SavedPlan.RecreateScope, map[string]any{"ok": false, "error": err.Error()}), err)
				}
				logger.Info("Applying saved plan", "plan_file", args[0], "recreate_scope", target.SavedPlan.RecreateScope, "summary", diff.RenderSummary(target.SavedPlan.Plan))
				savedSpec, specErr := target.SavedPlan.ResolvedSpec()
				if specErr != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(target.SavedPlan.Result(), target.SavedPlan.RecreateScope, map[string]any{"ok": false, "error": specErr.Error()}), specErr)
				}
				if err := maybeBootstrapRuntimeConfig(cmd.Context(), root.Editor, savedSpec, target.SavedPlan.Profile); err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(target.SavedPlan.Result(), target.SavedPlan.RecreateScope, map[string]any{"ok": false, "error": err.Error()}), err)
				}
				result = target.SavedPlan.Result()
				applyScope = target.SavedPlan.RecreateScope
			} else {
				if err := maybeBootstrapRuntimeConfig(cmd.Context(), root.Editor, target.Spec, profile); err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error(), "recreate": recreate, "wipe_org": wipeOrg}, err)
				}
				result, err = engine.Plan(cmd.Context(), target.Spec, syncpkg.Options{RecreateScope: scope, Profile: profile})
				if err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, scope, map[string]any{"ok": false, "error": err.Error()}), err)
				}
			}
			mutations := diff.MutatingActionCount(result.Plan)
			if root.JSONOutput && !dryRun && mutations > 0 && !yes {
				err := errors.New("--json apply requires --yes when mutations are planned")
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, applyScope, map[string]any{"ok": false, "error": err.Error()}), err)
			}
			if !root.JSONOutput {
				printScopeSummary(cmd.OutOrStdout(), result)
				for _, warning := range result.Warnings {
					fmt.Fprintf(cmd.OutOrStdout(), "Warning: %s\n", warning)
				}
				fmt.Fprintln(cmd.OutOrStdout(), diff.RenderStyledText(result.Plan, diff.RenderOptions{Color: logging.ColorEnabled(cmd.OutOrStdout())}))
			}
			if dryRun || mutations == 0 {
				if root.JSONOutput {
					return writeJSON(cmd.OutOrStdout(), resultPayload(result, applyScope, map[string]any{"ok": true, "applied": false, "dry_run": dryRun}))
				}
				if dryRun {
					fmt.Fprintln(cmd.OutOrStdout(), "Dry run complete.")
				}
				return nil
			}
			if maxChanges > 0 && mutations > maxChanges {
				err := fmt.Errorf("planned changes %d exceed max-changes %d", mutations, maxChanges)
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, applyScope, map[string]any{"ok": false, "error": err.Error()}), err)
			}
			if !yes {
				if args[0] == "-" {
					err := errors.New("interactive confirmation is not supported when reading the spec from stdin; use --yes")
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, applyScope, map[string]any{"ok": false, "error": err.Error()}), err)
				}
				confirmed, confirmErr := confirmApply(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), result, applyScope, root.Prompter)
				if confirmErr != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(result, applyScope, map[string]any{"ok": false, "error": confirmErr.Error()}), confirmErr)
				}
				if !confirmed {
					fmt.Fprintln(cmd.OutOrStdout(), "Apply cancelled.")
					return nil
				}
			}
			var applied syncpkg.Result
			var applyErr error
			if target.IsSavedPlan {
				applied, applyErr = engine.ApplySavedPlan(cmd.Context(), target.SavedPlan, syncpkg.Options{DryRun: false, MaxChanges: maxChanges})
			} else {
				applied, applyErr = engine.Apply(cmd.Context(), target.Spec, result, syncpkg.Options{DryRun: false, MaxChanges: maxChanges, RecreateScope: scope, Profile: profile})
			}
			if applyErr != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), resultPayload(applied, applyScope, map[string]any{"ok": false, "error": applyErr.Error()}), applyErr)
			}
			logger.Debug("Apply command completed", "applied", applied.Applied, "summary", diff.RenderSummary(applied.Plan))
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), resultPayload(applied, applyScope, map[string]any{"ok": true, "applied": applied.Applied}))
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Apply completed.")
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "compute the apply plan without mutating")
	cmd.Flags().BoolVar(&recreate, "recreate", false, "delete all managed campaigns in the configured campaign_group.id + app.app_id scope before recreating the YAML-defined state")
	cmd.Flags().BoolVar(&wipeOrg, "wipe-org", false, "delete all remote campaigns visible in the configured organization before recreating the YAML-defined state")
	cmd.Flags().IntVar(&maxChanges, "max-changes", 0, "abort if planned changes exceed this value")
	cmd.Flags().StringVar(&profile, "profile", "", "user config profile to use for runtime auth and org resolution")
	cmd.Flags().BoolVar(&yes, "yes", false, "skip interactive confirmation")
	cmd.Flags().StringVar(&rootDir, "root", "", "root directory for resolving relative includes when reading from stdin")
	cmd.MarkFlagsMutuallyExclusive("recreate", "wipe-org")
	return cmd
}

type applyTarget struct {
	Spec        spec.Spec
	SavedPlan   syncpkg.SavedPlan
	IsSavedPlan bool
}

func newCloneCommand(root *rootOptions) *cobra.Command {
	var storefront string
	var bidMultiplier float64
	var budgetMultiplier float64
	var rootDir string
	cmd := &cobra.Command{
		Use:   "clone <src.yaml> <dst.yaml>",
		Args:  cobra.ExactArgs(2),
		Short: "Clone a market YAML into a new storefront.",
		Long:  "Clone a YAML market config into a new storefront while scaling bids and budgets. This is intended for rolling the same structure into adjacent markets such as UK, CA, or AU.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			logger.Debug("Clone started", "src", args[0], "dst", args[1], "storefront", storefront, "bid_multiplier", bidMultiplier, "budget_multiplier", budgetMultiplier)
			document, err := loadDocumentArg(args[0], rootDir, cmd.InOrStdin())
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			if document.Type != spec.DocumentTypeConfig && document.Type != spec.DocumentTypeManifest {
				err := fmt.Errorf("clone only supports config or manifest files; %s is a %s", args[0], document.Type)
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			cloned, err := clonepkg.Spec(document.Spec, storefront, bidMultiplier, budgetMultiplier)
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			content, err := spec.Format(cloned)
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			if err := os.WriteFile(args[1], content, 0o644); err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			logger.Debug("Clone completed", "dst", args[1])
			if root.JSONOutput {
				return writeJSON(cmd.OutOrStdout(), map[string]any{"ok": true, "path": args[1]})
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Wrote %s\n", args[1])
			return nil
		},
	}
	cmd.Flags().StringVar(&storefront, "storefront", "", "target storefront code")
	cmd.Flags().Float64Var(&bidMultiplier, "bid-multiplier", 1.0, "bid scaling multiplier (> 0)")
	cmd.Flags().Float64Var(&budgetMultiplier, "budget-multiplier", 1.0, "budget scaling multiplier (> 0)")
	cmd.Flags().StringVar(&rootDir, "root", "", "root directory for resolving relative includes when reading from stdin")
	_ = cmd.MarkFlagRequired("storefront")
	return cmd
}

func newFmtCommand(root *rootOptions) *cobra.Command {
	var write bool
	var rootDir string
	cmd := &cobra.Command{
		Use:   "fmt <config.yaml>",
		Args:  cobra.ExactArgs(1),
		Short: "Format YAML canonically.",
		Long:  "Render the YAML desired state in canonical formatting with stable field ordering. Use -w to rewrite the source file in place; otherwise the formatted YAML is written to stdout.",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.Component(logging.New(cmd.ErrOrStderr(), logging.Options{JSON: root.JSONOutput, Verbose: root.Verbose}), "cli")
			logger.Debug("Format started", "spec_path", args[0], "write", write)
			if write {
				if args[0] == "-" {
					err := errors.New("fmt --write does not support stdin")
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
				}
				files, err := spec.FormatFiles(args[0])
				if err != nil {
					return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
				}
				for _, file := range files {
					if err := os.WriteFile(file.Path, file.Content, 0o644); err != nil {
						return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
					}
				}
				if root.JSONOutput {
					paths := make([]string, 0, len(files))
					for _, file := range files {
						paths = append(paths, file.Path)
					}
					return writeJSON(cmd.OutOrStdout(), map[string]any{"ok": true, "paths": paths})
				}
				for _, file := range files {
					fmt.Fprintf(cmd.OutOrStdout(), "Formatted %s\n", file.Path)
				}
				return nil
			}
			document, err := loadDocumentArg(args[0], rootDir, cmd.InOrStdin())
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			content, err := formatDocument(document)
			if err != nil {
				return render(root, cmd.OutOrStdout(), cmd.ErrOrStderr(), map[string]any{"ok": false, "error": err.Error()}, err)
			}
			_, err = cmd.OutOrStdout().Write(content)
			return err
		},
	}
	cmd.Flags().BoolVarP(&write, "write", "w", false, "write result to the source file")
	cmd.Flags().StringVar(&rootDir, "root", "", "root directory for resolving relative includes when reading from stdin")
	return cmd
}

func loadSpecArg(path, rootDir string, input io.Reader) (spec.Spec, error) {
	if path != "-" {
		return spec.LoadFile(path)
	}
	data, err := io.ReadAll(input)
	if err != nil {
		return spec.Spec{}, err
	}
	return spec.LoadSource("-", data, rootDir)
}

func loadApplyTarget(path, rootDir string, input io.Reader) (applyTarget, error) {
	if path == "-" {
		loaded, err := loadSpecArg(path, rootDir, input)
		if err != nil {
			return applyTarget{}, err
		}
		return applyTarget{Spec: loaded}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return applyTarget{}, fmt.Errorf("read spec: %w", err)
	}
	saved, ok, err := syncpkg.ParseSavedPlan(data)
	if err != nil {
		return applyTarget{}, err
	}
	if ok {
		return applyTarget{SavedPlan: saved, IsSavedPlan: true}, nil
	}
	loaded, err := spec.LoadSource(path, data, "")
	if err != nil {
		return applyTarget{}, err
	}
	return applyTarget{Spec: loaded}, nil
}

func loadDocumentArg(path, rootDir string, input io.Reader) (spec.Document, error) {
	if path != "-" {
		return spec.LoadDocumentFile(path)
	}
	data, err := io.ReadAll(input)
	if err != nil {
		return spec.Document{}, err
	}
	return spec.LoadDocumentSource("-", data, rootDir)
}

func formatDocument(document spec.Document) ([]byte, error) {
	switch document.Type {
	case spec.DocumentTypeConfig, spec.DocumentTypeManifest:
		if document.Type == spec.DocumentTypeConfig {
			return spec.Format(document.Spec)
		}
		return spec.FormatManifest(document.Manifest)
	case spec.DocumentTypeBase:
		return spec.FormatBase(document.Base)
	case spec.DocumentTypeCampaigns:
		return spec.FormatCampaignsFile(document.Campaigns)
	default:
		return nil, fmt.Errorf("unsupported document type %q", document.Type)
	}
}

func confirmationPrompt(result syncpkg.Result, scope diff.RecreateScope) string {
	summary := diff.RenderSummary(result.Plan)
	mutations := diff.MutatingActionCount(result.Plan)
	if result.Plan.Summary.Delete == 0 {
		return fmt.Sprintf("Apply %d changes? Summary: %s.", mutations, summary)
	}
	switch scope {
	case diff.RecreateScopeOrg:
		return fmt.Sprintf("Apply %d changes? Summary: %s. This includes deleting %d remote campaign(s) across the entire configured organization (%d managed, %d other-app).", mutations, summary, result.Plan.Summary.Delete, result.ScopeSummary.ManagedCampaignCount, result.ScopeSummary.OtherAppCampaignCount)
	case diff.RecreateScopeManaged:
		return fmt.Sprintf("Apply %d changes? Summary: %s. This includes deleting %d managed campaign(s) for the configured campaign_group.id + app.app_id scope before recreating them from YAML.", mutations, summary, result.Plan.Summary.Delete)
	default:
		return fmt.Sprintf("Apply %d changes? Summary: %s. This includes deleting %d stale managed resource(s).", mutations, summary, result.Plan.Summary.Delete)
	}
}

func confirmApplyInput(input io.Reader) (io.ReadCloser, error) {
	readCloser, ok := input.(io.ReadCloser)
	if !ok {
		return nil, errors.New("interactive confirmation requires a closable input reader; use --yes or pass an io.ReadCloser")
	}
	return readCloser, nil
}

func confirmApply(ctx context.Context, input io.Reader, output io.Writer, result syncpkg.Result, scope diff.RecreateScope, prompter ...applyPrompter) (bool, error) {
	readCloser, err := confirmApplyInput(input)
	if err != nil {
		return false, err
	}
	var chosen applyPrompter = stdioPrompter{}
	if len(prompter) > 0 && prompter[0] != nil {
		chosen = prompter[0]
	}
	return chosen.Confirm(ctx, readCloser, output, confirmationPrompt(result, scope))
}

func recreateScopeFromFlags(recreate, wipeOrg bool) (diff.RecreateScope, error) {
	switch {
	case recreate && wipeOrg:
		return diff.RecreateScopeNone, errors.New("--recreate and --wipe-org are mutually exclusive")
	case wipeOrg:
		return diff.RecreateScopeOrg, nil
	case recreate:
		return diff.RecreateScopeManaged, nil
	default:
		return diff.RecreateScopeNone, nil
	}
}

func validateSavedPlanApplyFlags(profile string, scope diff.RecreateScope, rootDir string) error {
	if strings.TrimSpace(profile) != "" {
		return errors.New("--profile cannot be used when applying a saved plan")
	}
	if scope != diff.RecreateScopeNone {
		return errors.New("--recreate and --wipe-org cannot be used when applying a saved plan")
	}
	if strings.TrimSpace(rootDir) != "" {
		return errors.New("--root cannot be used when applying a saved plan")
	}
	return nil
}

func render(root *rootOptions, stdout, stderr io.Writer, payload any, err error) error {
	if root.JSONOutput {
		writeErr := writeJSON(stdout, payload)
		if writeErr != nil && err == nil {
			return writeErr
		}
		return err
	}
	if err != nil {
		fmt.Fprintln(stderr, err.Error())
	}
	return err
}

func writeSavedPlanFile(path string, saved syncpkg.SavedPlan) error {
	content, err := saved.Bytes()
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write saved plan %q: %w", path, err)
	}
	return nil
}

func resultPayload(result syncpkg.Result, scope diff.RecreateScope, fields map[string]any) map[string]any {
	payload := map[string]any{
		"warnings":            result.Warnings,
		"plan":                result.Plan,
		"scope_summary":       result.ScopeSummary,
		"managed_campaigns":   result.ManagedCampaigns,
		"other_app_campaigns": result.OtherAppCampaigns,
		"recreate":            scope != diff.RecreateScopeNone,
		"wipe_org":            scope == diff.RecreateScopeOrg,
		"recreate_scope":      string(scope),
	}
	for key, value := range fields {
		payload[key] = value
	}
	return payload
}

func printScopeSummary(output io.Writer, result syncpkg.Result) {
	_, _ = fmt.Fprintf(output, "Scope: managed campaigns=%d, other-app campaigns=%d, wipe targets=%d\n", result.ScopeSummary.ManagedCampaignCount, result.ScopeSummary.OtherAppCampaignCount, result.ScopeSummary.WipeTargetCount)
	if len(result.OtherAppCampaigns) > 0 {
		fmt.Fprintln(output, "Other-app campaigns in org:")
		for _, name := range result.OtherAppCampaigns {
			fmt.Fprintf(output, "- %s\n", name)
		}
	}
}

func (stdioPrompter) Confirm(ctx context.Context, input io.Reader, output io.Writer, prompt string) (bool, error) {
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	_, _ = fmt.Fprintf(output, "%s [y/N]: ", prompt)

	type readResult struct {
		line string
		err  error
	}
	reader := bufio.NewReader(input)
	resultCh := make(chan readResult, 1)
	var stop func() bool
	if closer, ok := input.(io.Closer); ok {
		stop = context.AfterFunc(ctx, func() {
			_ = closer.Close()
		})
		defer stop()
	}
	go func() {
		line, err := reader.ReadString('\n')
		resultCh <- readResult{line: line, err: err}
	}()
	select {
	case <-ctx.Done():
		return false, context.Cause(ctx)
	case result := <-resultCh:
		if result.err != nil && result.err != io.EOF {
			return false, result.err
		}
		answer := strings.ToLower(strings.TrimSpace(result.line))
		return answer == "y" || answer == "yes", nil
	}
}

func writeJSON(writer io.Writer, payload any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(payload)
}
