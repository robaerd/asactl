package sync

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/auth"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/logging"
	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
	"github.com/robaerd/asactl/internal/validate"
)

type Options struct {
	DryRun        bool
	RecreateScope diff.RecreateScope
	Profile       string
}

type Result struct {
	Plan              diff.Plan                `json:"plan"`
	Warnings          []string                 `json:"warnings,omitempty"`
	Applied           bool                     `json:"applied"`
	ScopeSummary      appleadsapi.ScopeSummary `json:"scope_summary"`
	ManagedCampaigns  []string                 `json:"managed_campaigns,omitempty"`
	OtherAppCampaigns []string                 `json:"other_app_campaigns,omitempty"`
}

type CheckAuthResult struct {
	CampaignGroupID   string                           `json:"campaign_group_id"`
	OrgName           string                           `json:"org_name,omitempty"`
	AppID             string                           `json:"app_id"`
	ProductPages      []appleadsapi.ProductPageSummary `json:"product_pages,omitempty"`
	ProductPageCount  int                              `json:"product_page_count"`
	ScopeSummary      appleadsapi.ScopeSummary         `json:"scope_summary"`
	ManagedCampaigns  []string                         `json:"managed_campaigns,omitempty"`
	OtherAppCampaigns []string                         `json:"other_app_campaigns,omitempty"`
	Warnings          []string                         `json:"warnings,omitempty"`
}

type adsAPI interface {
	FetchState(context.Context, spec.Spec) (appleadsapi.FetchResult, error)
	CheckAuth(context.Context, spec.Spec) (appleadsapi.AuthCheckResult, error)
	ApplyPlan(context.Context, spec.Spec, diff.Plan) error
}

type apiFactory func(spec.Spec, auth.Config) (adsAPI, error)

type EngineOption func(*Engine)

type Engine struct {
	logger     *slog.Logger
	apiFactory apiFactory
	httpClient *http.Client
	tokenURL   string
	apiBaseURL string
}

type preparedPlan struct {
	runtime userconfig.Runtime
	result  Result
}

func WithHTTPClient(httpClient *http.Client) EngineOption {
	return func(engine *Engine) {
		if httpClient != nil {
			engine.httpClient = httpClient
		}
	}
}

func WithTokenURL(tokenURL string) EngineOption {
	return func(engine *Engine) {
		if trimmed := strings.TrimSpace(tokenURL); trimmed != "" {
			engine.tokenURL = trimmed
		}
	}
}

func WithAPIBaseURL(apiBaseURL string) EngineOption {
	return func(engine *Engine) {
		if trimmed := strings.TrimSpace(apiBaseURL); trimmed != "" {
			engine.apiBaseURL = trimmed
		}
	}
}

func NewEngine(logger *slog.Logger, options ...EngineOption) *Engine {
	engine := &Engine{logger: logging.Ensure(logger)}
	engine.apiFactory = engine.defaultAPIFactory
	for _, option := range options {
		if option != nil {
			option(engine)
		}
	}
	return engine
}

func (e *Engine) preparePlan(ctx context.Context, input spec.Spec, options Options) (preparedPlan, error) {
	logger := logging.Component(e.logger, "sync")
	logger.Debug("Plan started", "app_id", input.App.AppID, "recreate_scope", options.RecreateScope)
	runtime, err := userconfig.ResolveRuntime(input, options.Profile)
	if err != nil {
		return preparedPlan{}, err
	}
	input = runtime.Spec
	validation := validate.Run(input)
	warnings := slices.Clone(validation.Warnings)
	switch options.RecreateScope {
	case diff.RecreateScopeManaged:
		warnings = append(warnings, "recreate mode deletes all managed campaigns for the configured campaign_group.id + app.app_id scope before recreating the YAML-defined state")
		logger.Warn("Recreate mode enabled", "scope", options.RecreateScope)
	case diff.RecreateScopeOrg:
		warnings = append(warnings, "wipe-org mode deletes all remote campaigns visible in the configured campaign group before recreating the YAML-defined state")
		logger.Warn("Wipe-org mode enabled", "scope", options.RecreateScope)
	}
	logger.Debug("Validation completed", "warnings", len(validation.Warnings), "errors", len(validation.Errors))
	if strings.TrimSpace(input.App.AppID) == "" || strings.TrimSpace(input.App.AppID) == "REPLACE_ME" {
		return preparedPlan{
			runtime: runtime,
			result:  Result{Warnings: warnings},
		}, errors.New("app.app_id must be a real App Store app ID for plan or apply")
	}
	if !validation.OK() {
		return preparedPlan{
			runtime: runtime,
			result:  Result{Warnings: warnings},
		}, joinValidationErrors(validation)
	}
	desired := diff.BuildDesiredState(input)
	if err := diff.EnsureUnique(desired); err != nil {
		return preparedPlan{
			runtime: runtime,
			result:  Result{Warnings: warnings},
		}, err
	}
	api, err := e.apiFactory(input, runtime.AuthConfig)
	if err != nil {
		return preparedPlan{
			runtime: runtime,
			result:  Result{Warnings: warnings},
		}, err
	}
	fetched, err := api.FetchState(ctx, input)
	if err != nil {
		return preparedPlan{
			runtime: runtime,
			result:  Result{Warnings: warnings},
		}, err
	}
	if err := diff.EnsureUnique(fetched.State); err != nil {
		return preparedPlan{
			runtime: runtime,
			result:  Result{Warnings: warnings},
		}, err
	}
	logger.Debug(
		"Remote state fetched",
		"managed_campaigns", fetched.Scope.ManagedCampaignCount,
		"other_app_campaigns", fetched.Scope.OtherAppCampaignCount,
		"campaigns", len(fetched.State.Campaigns),
		"adgroups", len(fetched.State.AdGroups),
		"keywords", len(fetched.State.Keywords),
		"negative_keywords", len(fetched.State.NegativeKeywords),
		"custom_ads", len(fetched.State.CustomAds),
	)
	plan := diff.BuildPlanWithOptions(desired, fetched.State, diff.PlanOptions{
		RecreateScope:     options.RecreateScope,
		RecreateCampaigns: recreateCampaigns(options.RecreateScope, fetched),
	})
	result := Result{
		Plan:              plan,
		Warnings:          warnings,
		ScopeSummary:      scopeSummaryFor(options.RecreateScope, fetched.Scope),
		ManagedCampaigns:  slices.Clone(fetched.ManagedCampaigns),
		OtherAppCampaigns: slices.Clone(fetched.OtherAppCampaigns),
	}
	logger.Debug("Plan completed", "summary", diff.RenderSummary(plan), "action_count", len(plan.Actions))
	if requiresCurrencyForPlan(plan) && strings.TrimSpace(input.Defaults.Currency) == "" {
		return preparedPlan{
			runtime: runtime,
			result:  result,
		}, errors.New("defaults.currency must be set when plan changes include budgets or bids")
	}
	return preparedPlan{
		runtime: runtime,
		result:  result,
	}, nil
}

func (e *Engine) Plan(ctx context.Context, input spec.Spec, options Options) (Result, error) {
	prepared, err := e.preparePlan(ctx, input, options)
	return prepared.result, err
}

func (e *Engine) PlanSaved(ctx context.Context, input spec.Spec, options Options) (Result, SavedPlan, error) {
	prepared, err := e.preparePlan(ctx, input, options)
	if err != nil {
		return prepared.result, SavedPlan{}, err
	}
	saved, saveErr := newSavedPlan(prepared.runtime, prepared.result, options)
	if saveErr != nil {
		return prepared.result, SavedPlan{}, saveErr
	}
	return prepared.result, saved, nil
}

func (e *Engine) applyPlanned(ctx context.Context, input spec.Spec, profile string, planned Result, dryRun bool, scope diff.RecreateScope) (Result, error) {
	logger := logging.Component(e.logger, "sync")
	result := planned
	mutations := diff.MutatingActionCount(planned.Plan)
	logger.Debug("Apply started", "mutations", mutations, "dry_run", dryRun, "recreate_scope", scope)
	if requiresCurrencyForPlan(planned.Plan) && strings.TrimSpace(input.Defaults.Currency) == "" {
		return result, errors.New("defaults.currency must be set when plan changes include budgets or bids")
	}
	if mutations == 0 {
		logger.Debug("Apply skipped", "dry_run", dryRun, "mutations", mutations)
		return result, nil
	}
	runtime, err := userconfig.ResolveRuntime(input, profile)
	if err != nil {
		return result, err
	}
	if dryRun {
		logger.Debug("Apply skipped", "dry_run", dryRun, "mutations", mutations)
		return result, nil
	}
	api, err := e.apiFactory(runtime.Spec, runtime.AuthConfig)
	if err != nil {
		return result, err
	}
	if err := api.ApplyPlan(ctx, runtime.Spec, planned.Plan); err != nil {
		return result, err
	}
	result.Applied = true
	logger.Info("Apply completed", "summary", diff.RenderSummary(planned.Plan), "mutations", mutations)
	return result, nil
}

func (e *Engine) Apply(ctx context.Context, input spec.Spec, planned Result, options Options) (Result, error) {
	return e.applyPlanned(ctx, input, options.Profile, planned, options.DryRun, options.RecreateScope)
}

func (e *Engine) ApplySavedPlan(ctx context.Context, saved SavedPlan, options Options) (Result, error) {
	if err := saved.Validate(); err != nil {
		return saved.Result(), err
	}
	input, err := saved.ResolvedSpec()
	if err != nil {
		return saved.Result(), err
	}
	return e.applyPlanned(ctx, input, saved.Profile, saved.Result(), options.DryRun, saved.RecreateScope)
}

func (e *Engine) CheckAuth(ctx context.Context, input spec.Spec, profile string) (CheckAuthResult, error) {
	logger := logging.Component(e.logger, "sync")
	logger.Debug("Authentication check started", "app_id", input.App.AppID)
	runtime, err := userconfig.ResolveRuntime(input, profile)
	if err != nil {
		return CheckAuthResult{}, err
	}
	input = runtime.Spec
	validation := validate.Run(input)
	warnings := slices.Clone(validation.Warnings)
	if strings.TrimSpace(input.App.AppID) == "" || strings.TrimSpace(input.App.AppID) == "REPLACE_ME" {
		return CheckAuthResult{Warnings: warnings}, errors.New("app.app_id must be a real App Store app ID for check-auth")
	}
	if !validation.OK() {
		return CheckAuthResult{Warnings: warnings}, joinValidationErrors(validation)
	}
	api, err := e.apiFactory(input, runtime.AuthConfig)
	if err != nil {
		return CheckAuthResult{Warnings: warnings}, err
	}
	result, err := api.CheckAuth(ctx, input)
	if err != nil {
		return CheckAuthResult{Warnings: warnings}, err
	}
	result.Warnings = append(warnings, result.Warnings...)
	logger.Debug("Authentication check completed", "campaign_group_id", result.CampaignGroupID, "org_name", result.OrgName, "product_page_count", result.ProductPageCount, "managed_campaigns", result.Scope.ManagedCampaignCount, "other_app_campaigns", result.Scope.OtherAppCampaignCount, "warnings", len(result.Warnings))
	return CheckAuthResult{
		CampaignGroupID:   result.CampaignGroupID,
		OrgName:           result.OrgName,
		AppID:             result.AppID,
		ProductPages:      slices.Clone(result.ProductPages),
		ProductPageCount:  result.ProductPageCount,
		ScopeSummary:      result.Scope,
		ManagedCampaigns:  slices.Clone(result.ManagedCampaigns),
		OtherAppCampaigns: slices.Clone(result.OtherAppCampaigns),
		Warnings:          slices.Clone(result.Warnings),
	}, nil
}

func recreateCampaigns(scope diff.RecreateScope, fetched appleadsapi.FetchResult) []diff.Campaign {
	switch scope {
	case diff.RecreateScopeManaged:
		// Normal diffs operate on campaigns for the configured campaign_group.id + app.app_id scope.
		return slices.Clone(fetched.State.Campaigns)
	case diff.RecreateScopeOrg:
		// Org-wide wipes need every campaign visible in the configured campaign group, not just the managed subset.
		return slices.Clone(fetched.OrgCampaigns)
	default:
		return nil
	}
}

func scopeSummaryFor(scope diff.RecreateScope, summary appleadsapi.ScopeSummary) appleadsapi.ScopeSummary {
	summary.WipeTargetCount = 0
	switch scope {
	case diff.RecreateScopeManaged:
		summary.WipeTargetCount = summary.ManagedCampaignCount
	case diff.RecreateScopeOrg:
		summary.WipeTargetCount = summary.ManagedCampaignCount + summary.OtherAppCampaignCount
	}
	return summary
}

func requiresCurrencyForPlan(plan diff.Plan) bool {
	for _, action := range plan.Actions {
		switch action.Kind {
		case diff.ResourceCampaign:
			if action.Operation == diff.OperationCreate {
				return true
			}
			if action.Operation == diff.OperationUpdate && actionChangesField(action, "daily_budget") {
				return true
			}
		case diff.ResourceAdGroup:
			switch action.Operation {
			case diff.OperationCreate, diff.OperationUpdate, diff.OperationPause, diff.OperationActivate:
				return true
			}
		case diff.ResourceKeyword:
			switch action.Operation {
			case diff.OperationCreate, diff.OperationUpdate, diff.OperationPause, diff.OperationActivate:
				return true
			}
		}
	}
	return false
}

func actionChangesField(action diff.Action, field string) bool {
	for _, change := range action.Changes {
		if change.Field == field {
			return true
		}
	}
	return false
}

func joinValidationErrors(result validate.Result) error {
	if len(result.Errors) == 0 {
		return nil
	}
	errs := make([]error, 0, len(result.Errors))
	for _, issue := range result.Errors {
		errs = append(errs, errors.New(issue))
	}
	return errors.Join(errs...)
}

func (e *Engine) defaultAPIFactory(input spec.Spec, config auth.Config) (adsAPI, error) {
	tokenOptions := []auth.Option{auth.WithLogger(e.logger)}
	if strings.TrimSpace(e.tokenURL) != "" {
		tokenOptions = append(tokenOptions, auth.WithTokenURL(e.tokenURL))
	}
	tokens := auth.NewTokenProvider(config, e.httpClient, tokenOptions...)

	clientOptions := []appleadsapi.ClientOption{
		appleadsapi.WithOrgID(input.CampaignGroup.ID),
		appleadsapi.WithClientLogger(e.logger),
	}
	if e.httpClient != nil {
		clientOptions = append(clientOptions, appleadsapi.WithHTTPClient(e.httpClient))
	}
	if strings.TrimSpace(e.apiBaseURL) != "" {
		clientOptions = append(clientOptions, appleadsapi.WithBaseURL(e.apiBaseURL))
	}
	client := appleadsapi.NewClient(tokens, clientOptions...)
	return appleadsapi.NewService(client, appleadsapi.WithServiceLogger(e.logger)), nil
}
