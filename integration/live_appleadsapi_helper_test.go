//go:build liveappleads

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/auth"
	"github.com/robaerd/asactl/internal/diff"
	"github.com/robaerd/asactl/internal/spec"
	"github.com/robaerd/asactl/internal/userconfig"
)

const (
	liveEnvEnabled             = "ASACTL_LIVE"
	liveEnvKeep                = "ASACTL_LIVE_KEEP"
	liveEnvProfile             = "ASACTL_LIVE_PROFILE"
	liveEnvCampaignGroupID     = "ASACTL_LIVE_CAMPAIGN_GROUP_ID"
	liveEnvAppID               = "ASACTL_LIVE_APP_ID"
	liveEnvCurrency            = "ASACTL_LIVE_CURRENCY"
	liveEnvStorefront          = "ASACTL_LIVE_STOREFRONT"
	liveEnvCPP1ID              = "ASACTL_LIVE_CPP1_ID"
	liveEnvCPP2ID              = "ASACTL_LIVE_CPP2_ID"
	liveEnvDailyBudget         = "ASACTL_LIVE_DAILY_BUDGET"
	liveEnvLowBid              = "ASACTL_LIVE_LOW_BID"
	liveEnvHighBid             = "ASACTL_LIVE_HIGH_BID"
	liveEnvCommandTimeout      = "ASACTL_LIVE_TIMEOUT_SECONDS"
	liveDefaultProfile         = "default"
	liveDefaultCampaignGroupID = "20789793"
	liveDefaultAppID           = "1613230582"
	liveDefaultCurrency        = "EUR"
	liveDefaultStorefront      = "US"
	liveDefaultCPP1ID          = "1982a269-0f70-4480-85e8-acfc33681c94"
	liveDefaultCPP2ID          = "47d01d42-2976-4709-aea0-1a7b73aff67d"
	liveDefaultDailyBudget     = "0.05"
	liveDefaultLowBid          = "0.01"
	liveDefaultHighBid         = "0.02"
	liveDefaultTimeout         = 300 * time.Second
)

type liveSettings struct {
	Profile         string
	CampaignGroupID string
	AppID           string
	Currency        string
	Storefront      string
	CPP1ID          string
	CPP2ID          string
	DailyBudget     string
	LowBid          string
	HighBid         string
	CommandTimeout  time.Duration
	KeepResources   bool
}

type liveNames struct {
	Campaign            string
	KeywordAdGroup      string
	SearchAdGroup       string
	KeywordText         string
	CampaignNegative    string
	AdGroupNegative     string
	RulesSourceCampaign string
	RulesSourceAdGroup  string
	RulesSourceKeyword  string
	RulesTargetCampaign string
	RulesTargetAdGroup  string
}

type liveSuite struct {
	rootDir        string
	workDir        string
	settings       liveSettings
	names          liveNames
	configPath     string
	authConfig     auth.Config
	client         *appleadsapi.Client
	service        *appleadsapi.Service
	commandTimeout time.Duration
}

type cliJSONResult struct {
	OK                bool                             `json:"ok"`
	Applied           bool                             `json:"applied"`
	DryRun            bool                             `json:"dry_run"`
	CampaignGroupID   string                           `json:"campaign_group_id"`
	OrgName           string                           `json:"org_name"`
	AppID             string                           `json:"app_id"`
	ProductPages      []appleadsapi.ProductPageSummary `json:"product_pages"`
	ProductPageCount  int                              `json:"product_page_count"`
	ManagedCampaigns  []string                         `json:"managed_campaigns"`
	OtherAppCampaigns []string                         `json:"other_app_campaigns"`
	Plan              diff.Plan                        `json:"plan"`
	ScopeSummary      appleadsapi.ScopeSummary         `json:"scope_summary"`
	Warnings          []string                         `json:"warnings"`
	Error             string                           `json:"error"`
	PlanFile          string                           `json:"plan_file"`
	RecreateScope     diff.RecreateScope               `json:"recreate_scope"`
}

type cliInvocation struct {
	Args   []string
	Stdout []byte
	Stderr []byte
}

type liveAPIID string

func (id *liveAPIID) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*id = ""
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*id = liveAPIID(text)
		return nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err == nil {
		*id = liveAPIID(number.String())
		return nil
	}
	return errors.New("live api id must be a string or number")
}

type creativeSummary struct {
	ID            liveAPIID `json:"id"`
	ProductPageID string    `json:"productPageId"`
	Type          string    `json:"type"`
}

type bulkResult struct {
	Data []bulkResultItem `json:"data"`
}

type bulkResultItem struct {
	ID      liveAPIID       `json:"id"`
	Success *bool           `json:"success,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
	Errors  json.RawMessage `json:"errors,omitempty"`
}

type pagedResponse[T any] struct {
	Data       []T `json:"data"`
	Pagination struct {
		TotalResults int `json:"totalResults"`
	} `json:"pagination"`
}

type managedSnapshot struct {
	Fetch appleadsapi.FetchResult
}

var (
	liveCLIBuildOnce sync.Once
	liveCLIBuildPath string
	liveCLIBuildDir  string
	liveCLIBuildErr  error
)

func requireLiveExecution(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(liveEnvEnabled)) != "1" {
		t.Skipf("set %s=1 to run live Apple Ads integration tests", liveEnvEnabled)
	}
}

func newLiveSuite(t *testing.T) *liveSuite {
	t.Helper()
	requireLiveExecution(t)

	settings := loadLiveSettings(t)
	rootDir := repoRoot(t)
	workDir := t.TempDir()
	configPath, authConfig := copyLiveProfileConfig(t, settings)
	runID := time.Now().UTC().Format("20060102t150405")
	names := liveNames{
		Campaign:            "INT Readcap " + runID,
		KeywordAdGroup:      "Keywords",
		SearchAdGroup:       "Search Match",
		KeywordText:         "readcap " + runID,
		CampaignNegative:    "neg-" + runID,
		AdGroupNegative:     "adg-neg-" + runID,
		RulesSourceCampaign: "INT Rules Source " + runID,
		RulesSourceAdGroup:  "Source Keywords",
		RulesSourceKeyword:  "rules " + runID,
		RulesTargetCampaign: "INT Rules Target " + runID,
		RulesTargetAdGroup:  "Target Search Match",
	}

	tokenProvider := auth.NewTokenProvider(authConfig, nil)
	client := appleadsapi.NewClient(tokenProvider, appleadsapi.WithOrgID(settings.CampaignGroupID))
	suite := &liveSuite{
		rootDir:        rootDir,
		workDir:        workDir,
		settings:       settings,
		names:          names,
		configPath:     configPath,
		authConfig:     authConfig,
		client:         client,
		service:        appleadsapi.NewService(client),
		commandTimeout: settings.CommandTimeout,
	}
	if !settings.KeepResources {
		t.Cleanup(func() {
			suite.cleanupManagedScope(t)
		})
	}
	return suite
}

func loadLiveSettings(t *testing.T) liveSettings {
	t.Helper()
	timeout := liveDefaultTimeout
	if raw := strings.TrimSpace(os.Getenv(liveEnvCommandTimeout)); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", liveEnvCommandTimeout, raw, err)
		}
		timeout = time.Duration(seconds) * time.Second
	}
	return liveSettings{
		Profile:         envOrDefault(liveEnvProfile, liveDefaultProfile),
		CampaignGroupID: envOrDefault(liveEnvCampaignGroupID, liveDefaultCampaignGroupID),
		AppID:           envOrDefault(liveEnvAppID, liveDefaultAppID),
		Currency:        envOrDefault(liveEnvCurrency, liveDefaultCurrency),
		Storefront:      envOrDefault(liveEnvStorefront, liveDefaultStorefront),
		CPP1ID:          envOrDefault(liveEnvCPP1ID, liveDefaultCPP1ID),
		CPP2ID:          envOrDefault(liveEnvCPP2ID, liveDefaultCPP2ID),
		DailyBudget:     envOrDefault(liveEnvDailyBudget, liveDefaultDailyBudget),
		LowBid:          envOrDefault(liveEnvLowBid, liveDefaultLowBid),
		HighBid:         envOrDefault(liveEnvHighBid, liveDefaultHighBid),
		CommandTimeout:  timeout,
		KeepResources:   strings.TrimSpace(os.Getenv(liveEnvKeep)) == "1",
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolve repo root from %s: %v", root, err)
	}
	return root
}

func copyLiveProfileConfig(t *testing.T, settings liveSettings) (string, auth.Config) {
	t.Helper()
	loaded, err := userconfig.Load()
	if err != nil {
		t.Fatalf("load user config: %v", err)
	}
	profile, ok := loaded.File.Profiles[settings.Profile]
	if !ok {
		t.Fatalf("profile %q not found in %s", settings.Profile, loaded.Path)
	}
	authConfig, err := auth.ConfigFromInputs(auth.Inputs{
		ClientID:       strings.TrimSpace(profile.ClientID),
		TeamID:         strings.TrimSpace(profile.TeamID),
		KeyID:          strings.TrimSpace(profile.KeyID),
		PrivateKeyPath: strings.TrimSpace(profile.PrivateKeyPath),
	})
	if err != nil {
		t.Fatalf("resolve auth config for profile %q: %v", settings.Profile, err)
	}

	path := filepath.Join(t.TempDir(), "config.toml")
	file := userconfig.NewFile(settings.Profile)
	file.DefaultProfile = settings.Profile
	file.Profiles[settings.Profile] = profile
	if err := userconfig.SavePath(path, file); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path, authConfig
}

func liveCLIBinary(t *testing.T) string {
	t.Helper()
	liveCLIBuildOnce.Do(func() {
		liveCLIBuildDir, liveCLIBuildErr = os.MkdirTemp("", "asactl-live-cli-*")
		if liveCLIBuildErr != nil {
			return
		}
		liveCLIBuildPath = filepath.Join(liveCLIBuildDir, "asactl")
		cmd := exec.Command("go", "build", "-o", liveCLIBuildPath, "./cmd/asactl")
		cmd.Dir = repoRoot(t)
		output, err := cmd.CombinedOutput()
		if err != nil {
			liveCLIBuildErr = fmt.Errorf("build CLI: %w\n%s", err, output)
		}
	})
	if liveCLIBuildErr != nil {
		t.Fatal(liveCLIBuildErr)
	}
	return liveCLIBuildPath
}

func (suite *liveSuite) runCLIJSON(t *testing.T, args ...string) (cliJSONResult, cliInvocation) {
	t.Helper()
	result, invocation, err := suite.runCLIJSONAllowError(t, args...)
	if err != nil {
		t.Fatalf("command failed: %s: %v\nstdout=%s\nstderr=%s", strings.Join(invocation.Args, " "), err, invocation.Stdout, invocation.Stderr)
	}
	return result, invocation
}

func (suite *liveSuite) runCLIJSONAllowError(t *testing.T, args ...string) (cliJSONResult, cliInvocation, error) {
	t.Helper()
	commandArgs := append([]string{"--json"}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), suite.commandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, liveCLIBinary(t), commandArgs...)
	cmd.Dir = suite.rootDir
	cmd.Env = replaceEnv(os.Environ(), userconfig.OverrideEnvVar, suite.configPath)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()
	invocation := cliInvocation{
		Args:   append([]string(nil), commandArgs...),
		Stdout: stdout.Bytes(),
		Stderr: stderr.Bytes(),
	}
	if ctx.Err() != nil {
		t.Fatalf("command timed out: %s\nstdout=%s\nstderr=%s", strings.Join(commandArgs, " "), stdout.String(), stderr.String())
	}

	var result cliJSONResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode command JSON for %s: %v\nstdout=%s\nstderr=%s", strings.Join(commandArgs, " "), err, stdout.String(), stderr.String())
	}
	if err == nil && !result.OK {
		if strings.TrimSpace(result.Error) != "" {
			err = errors.New(strings.TrimSpace(result.Error))
		} else {
			err = errors.New("command returned ok=false")
		}
	}
	return result, invocation, err
}

func replaceEnv(base []string, key, value string) []string {
	prefix := key + "="
	filtered := make([]string, 0, len(base)+1)
	for _, entry := range base {
		if strings.HasPrefix(entry, prefix) {
			continue
		}
		filtered = append(filtered, entry)
	}
	filtered = append(filtered, prefix+value)
	return filtered
}

func (suite *liveSuite) preflight(t *testing.T) cliJSONResult {
	t.Helper()
	specPath := suite.writeSpecFile(t, "preflight-empty.yaml", suite.emptySpec())
	result, _ := suite.runCLIJSON(t, "check-auth", specPath)
	if result.CampaignGroupID != suite.settings.CampaignGroupID {
		t.Fatalf("expected campaign_group_id %q, got %q", suite.settings.CampaignGroupID, result.CampaignGroupID)
	}
	if result.AppID != suite.settings.AppID {
		t.Fatalf("expected app_id %q, got %q", suite.settings.AppID, result.AppID)
	}
	if result.ProductPageCount < 2 {
		t.Fatalf("expected at least two visible product pages, got %+v", result.ProductPages)
	}
	if result.ScopeSummary.OtherAppCampaignCount != 0 {
		t.Fatalf("expected dedicated testing org with zero other-app campaigns, got scope=%+v", result.ScopeSummary)
	}
	if result.ScopeSummary.ManagedCampaignCount > 0 {
		t.Logf("managed campaigns already exist in live scope; running cleanup before suite")
		suite.cleanupManagedScope(t)
		result, _ = suite.runCLIJSON(t, "check-auth", specPath)
	}
	if result.ScopeSummary.ManagedCampaignCount != 0 {
		t.Fatalf("expected empty managed scope after cleanup, got scope=%+v", result.ScopeSummary)
	}
	return result
}

func (suite *liveSuite) cleanupManagedScope(t *testing.T) {
	t.Helper()
	specPath := suite.writeSpecFile(t, "cleanup-empty.yaml", suite.emptySpec())
	result, invocation := suite.runCLIJSON(t, "apply", specPath, "--yes")
	if result.Applied {
		t.Logf("cleanup applied changes: %s", strings.TrimSpace(string(invocation.Stderr)))
	}
	suite.waitForManagedSnapshot(t, suite.emptySpec(), "empty managed scope", func(snapshot managedSnapshot) bool {
		return len(snapshot.Fetch.State.Campaigns) == 0
	})
}

func (suite *liveSuite) writeSpecFile(t *testing.T, name string, input spec.Spec) string {
	t.Helper()
	content, err := spec.Format(input)
	if err != nil {
		t.Fatalf("format spec %s: %v", name, err)
	}
	path := filepath.Join(suite.workDir, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write spec %s: %v", path, err)
	}
	return path
}

func (suite *liveSuite) writeManifestFromSpec(t *testing.T, dirName string, input spec.Spec) string {
	t.Helper()
	baseDir := filepath.Join(suite.workDir, dirName)
	campaignsDir := filepath.Join(baseDir, "campaigns")
	if err := os.MkdirAll(campaignsDir, 0o755); err != nil {
		t.Fatalf("create manifest directory %s: %v", baseDir, err)
	}

	manifestPath := filepath.Join(baseDir, "asactl.yaml")
	basePath := filepath.Join(baseDir, "base.yaml")
	mainCampaignsPath := filepath.Join(campaignsDir, "main.yaml")

	manifestContent, err := spec.FormatManifest(spec.Manifest{
		Version: input.Version,
		Kind:    spec.KindManifest,
		Base:    "base.yaml",
		Campaigns: []string{
			filepath.ToSlash(filepath.Join("campaigns", "main.yaml")),
		},
	})
	if err != nil {
		t.Fatalf("format manifest %s: %v", manifestPath, err)
	}
	baseContent, err := spec.FormatBase(manifestBaseFromSpec(input))
	if err != nil {
		t.Fatalf("format base %s: %v", basePath, err)
	}
	campaignsContent, err := spec.FormatCampaignsFile(manifestCampaignsFromSpec(input))
	if err != nil {
		t.Fatalf("format campaigns file %s: %v", mainCampaignsPath, err)
	}

	writeYAMLFile(t, manifestPath, manifestContent)
	writeYAMLFile(t, basePath, baseContent)
	writeYAMLFile(t, mainCampaignsPath, campaignsContent)
	return manifestPath
}

func (suite *liveSuite) writeRulesManifest(t *testing.T, dirName string, input spec.Spec) string {
	t.Helper()
	if len(input.Campaigns) != 2 {
		t.Fatalf("rules manifest expects exactly 2 campaigns, got %d", len(input.Campaigns))
	}
	if len(input.Generators) == 0 {
		t.Fatal("rules manifest expects at least one generator")
	}

	baseDir := filepath.Join(suite.workDir, dirName)
	campaignsDir := filepath.Join(baseDir, "campaigns")
	if err := os.MkdirAll(campaignsDir, 0o755); err != nil {
		t.Fatalf("create rules manifest directory %s: %v", baseDir, err)
	}

	manifestPath := filepath.Join(baseDir, "asactl.yaml")
	basePath := filepath.Join(baseDir, "base.yaml")
	sourcePath := filepath.Join(campaignsDir, "source.yaml")
	targetPath := filepath.Join(campaignsDir, "target.yaml")

	manifestContent, err := spec.FormatManifest(spec.Manifest{
		Version: input.Version,
		Kind:    spec.KindManifest,
		Base:    "base.yaml",
		Campaigns: []string{
			filepath.ToSlash(filepath.Join("campaigns", "source.yaml")),
			filepath.ToSlash(filepath.Join("campaigns", "target.yaml")),
		},
	})
	if err != nil {
		t.Fatalf("format rules manifest %s: %v", manifestPath, err)
	}
	baseContent, err := spec.FormatBase(manifestBaseFromSpec(input))
	if err != nil {
		t.Fatalf("format rules base %s: %v", basePath, err)
	}
	sourceContent, err := spec.FormatCampaignsFile(spec.CampaignsFile{
		Version:   input.Version,
		Kind:      spec.KindCampaigns,
		Campaigns: []spec.Campaign{input.Campaigns[0]},
	})
	if err != nil {
		t.Fatalf("format rules source campaigns file %s: %v", sourcePath, err)
	}
	targetContent, err := spec.FormatCampaignsFile(spec.CampaignsFile{
		Version:    input.Version,
		Kind:       spec.KindCampaigns,
		Generators: append([]spec.Generator(nil), input.Generators...),
		Campaigns:  []spec.Campaign{input.Campaigns[1]},
	})
	if err != nil {
		t.Fatalf("format rules target campaigns file %s: %v", targetPath, err)
	}

	writeYAMLFile(t, manifestPath, manifestContent)
	writeYAMLFile(t, basePath, baseContent)
	writeYAMLFile(t, sourcePath, sourceContent)
	writeYAMLFile(t, targetPath, targetContent)
	return manifestPath
}

func writeYAMLFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write yaml file %s: %v", path, err)
	}
}

func manifestBaseFromSpec(input spec.Spec) spec.Base {
	return spec.Base{
		Version:       input.Version,
		Kind:          spec.KindBase,
		CampaignGroup: input.CampaignGroup,
		Auth:          input.Auth,
		App:           input.App,
		Defaults:      input.Defaults,
		ProductPages:  input.ProductPages,
	}
}

func manifestCampaignsFromSpec(input spec.Spec) spec.CampaignsFile {
	return spec.CampaignsFile{
		Version:    input.Version,
		Kind:       spec.KindCampaigns,
		Generators: append([]spec.Generator(nil), input.Generators...),
		Campaigns:  append([]spec.Campaign(nil), input.Campaigns...),
	}
}

func (suite *liveSuite) emptySpec() spec.Spec {
	return spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: suite.settings.CampaignGroupID},
		Auth:          spec.Auth{Profile: suite.settings.Profile},
		App:           spec.App{Name: "Readcap", AppID: suite.settings.AppID},
		Defaults:      spec.Defaults{Currency: suite.settings.Currency, Devices: []spec.Device{spec.DeviceIPhone}},
		ProductPages: spec.ProductPageMap{
			"CPP1": {ProductPageID: suite.settings.CPP1ID, Name: "Reading Tracker", Locale: "en-US"},
			"CPP2": {ProductPageID: suite.settings.CPP2ID, Name: "Capture & Organize", Locale: "en-US"},
		},
		Campaigns: []spec.Campaign{},
	}
}

func (suite *liveSuite) baselineSpec(t *testing.T) spec.Spec {
	t.Helper()
	return spec.Spec{
		Version:       1,
		Kind:          spec.KindConfig,
		CampaignGroup: spec.CampaignGroup{ID: suite.settings.CampaignGroupID},
		Auth:          spec.Auth{Profile: suite.settings.Profile},
		App:           spec.App{Name: "Readcap", AppID: suite.settings.AppID},
		Defaults:      spec.Defaults{Currency: suite.settings.Currency, Devices: []spec.Device{spec.DeviceIPhone}},
		ProductPages: spec.ProductPageMap{
			"CPP1": {ProductPageID: suite.settings.CPP1ID, Name: "Reading Tracker", Locale: "en-US"},
			"CPP2": {ProductPageID: suite.settings.CPP2ID, Name: "Capture & Organize", Locale: "en-US"},
		},
		Campaigns: []spec.Campaign{{
			Name:        suite.names.Campaign,
			Storefronts: []string{suite.settings.Storefront},
			DailyBudget: mustDecimal(t, suite.settings.DailyBudget),
			Status:      spec.StatusActive,
			CampaignNegativeKeywords: []spec.NegativeKeyword{{
				Text:      suite.names.CampaignNegative,
				MatchType: spec.MatchTypeExact,
				Status:    spec.StatusActive,
			}},
			AdGroups: []spec.AdGroup{
				{
					Name:          suite.names.KeywordAdGroup,
					Status:        spec.StatusActive,
					DefaultCPTBid: mustDecimal(t, suite.settings.HighBid),
					ProductPage:   "CPP1",
					Targeting:     spec.TargetingKeywords,
					Keywords: []spec.Keyword{{
						Text:      suite.names.KeywordText,
						MatchType: spec.MatchTypeExact,
						Bid:       mustDecimal(t, suite.settings.LowBid),
						Status:    spec.StatusActive,
					}},
					AdGroupNegativeKeywords: []spec.NegativeKeyword{{
						Text:      suite.names.AdGroupNegative,
						MatchType: spec.MatchTypeExact,
						Status:    spec.StatusActive,
					}},
				},
				{
					Name:          suite.names.SearchAdGroup,
					Status:        spec.StatusActive,
					DefaultCPTBid: mustDecimal(t, suite.settings.HighBid),
					ProductPage:   "CPP2",
					Targeting:     spec.TargetingSearchMatch,
				},
			},
		}},
	}
}

func (suite *liveSuite) deleteSpec(t *testing.T) spec.Spec {
	t.Helper()
	input := suite.baselineSpec(t)
	input.Campaigns[0].CampaignNegativeKeywords = nil
	input.Campaigns[0].AdGroups = []spec.AdGroup{{
		Name:          suite.names.KeywordAdGroup,
		Status:        spec.StatusActive,
		DefaultCPTBid: mustDecimal(t, suite.settings.HighBid),
		ProductPage:   "CPP2",
		Targeting:     spec.TargetingKeywords,
	}}
	return input
}

func (suite *liveSuite) matchTypeChangeSpec(t *testing.T) spec.Spec {
	t.Helper()
	input := suite.baselineSpec(t)
	input.Campaigns[0].AdGroups[0].Keywords[0].MatchType = spec.MatchTypeBroad
	return input
}

func (suite *liveSuite) rulesSpec(t *testing.T) spec.Spec {
	t.Helper()
	input := suite.emptySpec()
	input.Generators = []spec.Generator{{
		Name: "rules-target-exact-overlap",
		Kind: spec.GeneratorKindKeywordToNegative,
		Spec: spec.GeneratorSpec{
			SourceRefs: spec.GeneratorSourceRefs{Campaigns: []string{suite.names.RulesSourceCampaign}},
			TargetRef:  spec.GeneratorTargetRef{Campaign: suite.names.RulesTargetCampaign},
			Filters:    spec.GeneratorFilters{KeywordMatchTypes: []spec.MatchType{spec.MatchTypeExact}},
			Generate: spec.GeneratorGenerate{
				CampaignNegativeKeywords: spec.GeneratorNegativeKeywordSpec{
					MatchType: spec.MatchTypeExact,
					Status:    spec.StatusActive,
				},
			},
		},
	}}
	input.Campaigns = []spec.Campaign{
		{
			Name:        suite.names.RulesSourceCampaign,
			Storefronts: []string{suite.settings.Storefront},
			DailyBudget: mustDecimal(t, suite.settings.DailyBudget),
			Status:      spec.StatusActive,
			AdGroups: []spec.AdGroup{{
				Name:          suite.names.RulesSourceAdGroup,
				Status:        spec.StatusActive,
				DefaultCPTBid: mustDecimal(t, suite.settings.HighBid),
				ProductPage:   "CPP1",
				Targeting:     spec.TargetingKeywords,
				Keywords: []spec.Keyword{{
					Text:      suite.names.RulesSourceKeyword,
					MatchType: spec.MatchTypeExact,
					Bid:       mustDecimal(t, suite.settings.LowBid),
					Status:    spec.StatusActive,
				}},
			}},
		},
		{
			Name:        suite.names.RulesTargetCampaign,
			Storefronts: []string{suite.settings.Storefront},
			DailyBudget: mustDecimal(t, suite.settings.DailyBudget),
			Status:      spec.StatusActive,
			AdGroups: []spec.AdGroup{{
				Name:          suite.names.RulesTargetAdGroup,
				Status:        spec.StatusActive,
				DefaultCPTBid: mustDecimal(t, suite.settings.HighBid),
				ProductPage:   "CPP2",
				Targeting:     spec.TargetingSearchMatch,
			}},
		},
	}
	return input
}

func mustDecimal(t *testing.T, value string) spec.Decimal {
	t.Helper()
	decimal, err := spec.ParseDecimal(value)
	if err != nil {
		t.Fatalf("parse decimal %q: %v", value, err)
	}
	return decimal
}

func (suite *liveSuite) fetchManagedSnapshot(t *testing.T, input spec.Spec) managedSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suite.commandTimeout)
	defer cancel()
	fetched, err := suite.service.FetchState(ctx, input)
	if err != nil {
		t.Fatalf("fetch managed state: %v", err)
	}
	return managedSnapshot{Fetch: fetched}
}

func (snapshot managedSnapshot) campaign(t *testing.T, name string) diff.Campaign {
	t.Helper()
	for _, campaign := range snapshot.Fetch.State.Campaigns {
		if strings.TrimSpace(campaign.Name) == strings.TrimSpace(name) {
			return campaign
		}
	}
	t.Fatalf("campaign %q not found in %+v", name, snapshot.Fetch.State.Campaigns)
	return diff.Campaign{}
}

func (snapshot managedSnapshot) adGroup(t *testing.T, campaignName, adGroupName string) diff.AdGroup {
	t.Helper()
	for _, adGroup := range snapshot.Fetch.State.AdGroups {
		if strings.TrimSpace(adGroup.CampaignName) == strings.TrimSpace(campaignName) && strings.TrimSpace(adGroup.Name) == strings.TrimSpace(adGroupName) {
			return adGroup
		}
	}
	t.Fatalf("adgroup %q in campaign %q not found in %+v", adGroupName, campaignName, snapshot.Fetch.State.AdGroups)
	return diff.AdGroup{}
}

func (snapshot managedSnapshot) keyword(t *testing.T, campaignName, adGroupName, text string) diff.Keyword {
	t.Helper()
	for _, keyword := range snapshot.Fetch.State.Keywords {
		if strings.TrimSpace(keyword.CampaignName) == strings.TrimSpace(campaignName) && strings.TrimSpace(keyword.AdGroupName) == strings.TrimSpace(adGroupName) && strings.TrimSpace(keyword.Text) == strings.TrimSpace(text) {
			return keyword
		}
	}
	t.Fatalf("keyword %q not found in %+v", text, snapshot.Fetch.State.Keywords)
	return diff.Keyword{}
}

func (snapshot managedSnapshot) negativeKeyword(t *testing.T, scope diff.NegativeKeywordScope, campaignName, adGroupName, text string) diff.NegativeKeyword {
	t.Helper()
	for _, negative := range snapshot.Fetch.State.NegativeKeywords {
		if negative.Scope != scope {
			continue
		}
		if strings.TrimSpace(negative.CampaignName) != strings.TrimSpace(campaignName) {
			continue
		}
		if strings.TrimSpace(negative.AdGroupName) != strings.TrimSpace(adGroupName) {
			continue
		}
		if strings.TrimSpace(negative.Text) != strings.TrimSpace(text) {
			continue
		}
		return negative
	}
	t.Fatalf("negative keyword %q not found in %+v", text, snapshot.Fetch.State.NegativeKeywords)
	return diff.NegativeKeyword{}
}

func (snapshot managedSnapshot) customAd(t *testing.T, campaignName, adGroupName, productPage string) diff.CustomAd {
	t.Helper()
	for _, customAd := range snapshot.Fetch.State.CustomAds {
		if strings.TrimSpace(customAd.CampaignName) == strings.TrimSpace(campaignName) && strings.TrimSpace(customAd.AdGroupName) == strings.TrimSpace(adGroupName) && strings.TrimSpace(customAd.ProductPage) == strings.TrimSpace(productPage) {
			return customAd
		}
	}
	t.Fatalf("custom ad %q/%q/%q not found in %+v", campaignName, adGroupName, productPage, snapshot.Fetch.State.CustomAds)
	return diff.CustomAd{}
}

func (snapshot managedSnapshot) hasKeyword(campaignName, adGroupName, text string, matchType spec.MatchType) bool {
	for _, keyword := range snapshot.Fetch.State.Keywords {
		if strings.TrimSpace(keyword.CampaignName) != strings.TrimSpace(campaignName) {
			continue
		}
		if strings.TrimSpace(keyword.AdGroupName) != strings.TrimSpace(adGroupName) {
			continue
		}
		if strings.TrimSpace(keyword.Text) != strings.TrimSpace(text) {
			continue
		}
		if keyword.MatchType != matchType {
			continue
		}
		return true
	}
	return false
}

func (snapshot managedSnapshot) hasNegativeKeyword(scope diff.NegativeKeywordScope, campaignName, adGroupName, text string, matchType spec.MatchType) bool {
	for _, negative := range snapshot.Fetch.State.NegativeKeywords {
		if negative.Scope != scope {
			continue
		}
		if strings.TrimSpace(negative.CampaignName) != strings.TrimSpace(campaignName) {
			continue
		}
		if strings.TrimSpace(negative.AdGroupName) != strings.TrimSpace(adGroupName) {
			continue
		}
		if strings.TrimSpace(negative.Text) != strings.TrimSpace(text) {
			continue
		}
		if negative.MatchType != matchType {
			continue
		}
		return true
	}
	return false
}

func (suite *liveSuite) listCreatives(t *testing.T) []creativeSummary {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suite.commandTimeout)
	defer cancel()
	return listAll[creativeSummary](t, ctx, suite.client, "/creatives")
}

func listAll[T any](t *testing.T, ctx context.Context, client *appleadsapi.Client, path string) []T {
	t.Helper()
	offset := 0
	items := make([]T, 0)
	for {
		var page pagedResponse[T]
		query := url.Values{
			"offset": []string{strconv.Itoa(offset)},
			"limit":  []string{"1000"},
		}
		if err := client.Get(ctx, path, query, &page); err != nil {
			t.Fatalf("list %s: %v", path, err)
		}
		items = append(items, page.Data...)
		if len(page.Data) == 0 {
			return items
		}
		total := page.Pagination.TotalResults
		if total > 0 && len(items) >= total {
			return items
		}
		if total == 0 && len(page.Data) < 1000 {
			return items
		}
		offset += len(page.Data)
	}
}

func (suite *liveSuite) assertCPPReachability(t *testing.T, before, after []creativeSummary) {
	t.Helper()
	beforeSet := cppCreativeSet(before, suite.settings.CPP1ID, suite.settings.CPP2ID)
	afterSet := cppCreativeSet(after, suite.settings.CPP1ID, suite.settings.CPP2ID)
	if beforeSet[suite.settings.CPP1ID] && beforeSet[suite.settings.CPP2ID] {
		t.Log("CPP1 and CPP2 already had creatives before create; POST /creatives coverage remained conditional")
		return
	}
	if !afterSet[suite.settings.CPP1ID] || !afterSet[suite.settings.CPP2ID] {
		t.Fatalf("expected create flow to leave both CPP creatives available; before=%v after=%v", beforeSet, afterSet)
	}
}

func cppCreativeSet(creatives []creativeSummary, cpp1, cpp2 string) map[string]bool {
	result := map[string]bool{
		cpp1: false,
		cpp2: false,
	}
	for _, creative := range creatives {
		if !strings.EqualFold(strings.TrimSpace(creative.Type), "CUSTOM_PRODUCT_PAGE") {
			continue
		}
		switch strings.TrimSpace(creative.ProductPageID) {
		case cpp1:
			result[cpp1] = true
		case cpp2:
			result[cpp2] = true
		}
	}
	return result
}

func (suite *liveSuite) mutateRemoteDrift(t *testing.T, input spec.Spec) {
	t.Helper()
	snapshot := suite.fetchManagedSnapshot(t, input)
	campaign := snapshot.campaign(t, suite.names.Campaign)
	adGroup := snapshot.adGroup(t, suite.names.Campaign, suite.names.KeywordAdGroup)
	keyword := snapshot.keyword(t, suite.names.Campaign, suite.names.KeywordAdGroup, suite.names.KeywordText)
	campaignNegative := snapshot.negativeKeyword(t, diff.ScopeCampaign, suite.names.Campaign, "", suite.names.CampaignNegative)
	adGroupNegative := snapshot.negativeKeyword(t, diff.ScopeAdGroup, suite.names.Campaign, suite.names.KeywordAdGroup, suite.names.AdGroupNegative)
	customAd := snapshot.customAd(t, suite.names.Campaign, suite.names.KeywordAdGroup, "CPP1")

	suite.putSingle(t, "/campaigns/"+campaign.ID+"/adgroups/"+adGroup.ID, map[string]any{
		"name":                   adGroup.Name,
		"defaultBidAmount":       map[string]string{"amount": suite.settings.LowBid, "currency": suite.settings.Currency},
		"automatedKeywordsOptIn": false,
		"status":                 "ENABLED",
	})
	suite.putBulk(t, "/campaigns/"+campaign.ID+"/adgroups/"+adGroup.ID+"/targetingkeywords/bulk", []map[string]any{{
		"id":        keyword.ID,
		"bidAmount": map[string]string{"amount": suite.settings.HighBid, "currency": suite.settings.Currency},
		"status":    "ACTIVE",
	}})
	suite.putBulk(t, "/campaigns/"+campaign.ID+"/negativekeywords/bulk", []map[string]any{{
		"id":     campaignNegative.ID,
		"status": "PAUSED",
	}})
	suite.putBulk(t, "/campaigns/"+campaign.ID+"/adgroups/"+adGroup.ID+"/negativekeywords/bulk", []map[string]any{{
		"id":     adGroupNegative.ID,
		"status": "PAUSED",
	}})
	suite.putSingle(t, "/campaigns/"+campaign.ID+"/adgroups/"+adGroup.ID+"/ads/"+customAd.ID, map[string]any{
		"status": "PAUSED",
	})
	suite.putSingle(t, "/campaigns/"+campaign.ID, map[string]any{
		"campaign": map[string]any{
			"status": "PAUSED",
		},
	})
}

func (suite *liveSuite) putSingle(t *testing.T, path string, payload any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suite.commandTimeout)
	defer cancel()
	if err := suite.client.Put(ctx, path, payload, nil); err != nil {
		t.Fatalf("PUT %s: %v", path, err)
	}
}

func (suite *liveSuite) putBulk(t *testing.T, path string, payload any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suite.commandTimeout)
	defer cancel()
	var result bulkResult
	if err := suite.client.Put(ctx, path, payload, &result); err != nil {
		t.Fatalf("PUT %s: %v", path, err)
	}
	if err := validateBulkResult(result); err != nil {
		t.Fatalf("PUT %s item failure: %v", path, err)
	}
}

func validateBulkResult(result bulkResult) error {
	if result.Data == nil {
		return errors.New("bulk response missing data")
	}
	for _, item := range result.Data {
		if item.Success != nil && !*item.Success {
			return fmt.Errorf("item %q failed", item.ID)
		}
		if bulkHasDetail(item.Error) || bulkHasDetail(item.Errors) {
			return fmt.Errorf("item %q failed", item.ID)
		}
	}
	return nil
}

func bulkHasDetail(value json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(value))
	return trimmed != "" && trimmed != "null" && trimmed != "[]" && trimmed != "{}"
}

func countActions(plan diff.Plan, operation diff.Operation, kind diff.ResourceKind) int {
	count := 0
	for _, action := range plan.Actions {
		if action.Operation == operation && action.Kind == kind {
			count++
		}
	}
	return count
}

func assertActionCountAtLeast(t *testing.T, plan diff.Plan, operation diff.Operation, kind diff.ResourceKind, minimum int) {
	t.Helper()
	if got := countActions(plan, operation, kind); got < minimum {
		t.Fatalf("expected at least %d %s %s action(s), got %d in %+v", minimum, operation, kind, got, plan.Actions)
	}
}

func assertNoMutations(t *testing.T, plan diff.Plan) {
	t.Helper()
	if got := diff.MutatingActionCount(plan); got != 0 {
		t.Fatalf("expected no mutating actions, got %d in %+v", got, plan.Actions)
	}
}

func assertSingleManagedCampaign(t *testing.T, snapshot managedSnapshot, campaignName string) {
	t.Helper()
	if snapshot.Fetch.Scope.ManagedCampaignCount != 1 {
		t.Fatalf("expected one managed campaign, got scope=%+v", snapshot.Fetch.Scope)
	}
	campaign := snapshot.campaign(t, campaignName)
	if campaign.Name != campaignName {
		t.Fatalf("unexpected campaign %+v", campaign)
	}
}

func (suite *liveSuite) waitForManagedSnapshot(t *testing.T, input spec.Spec, description string, predicate func(managedSnapshot) bool) managedSnapshot {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		snapshot := suite.fetchManagedSnapshot(t, input)
		if predicate(snapshot) {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; last snapshot=%+v", description, snapshot.Fetch.State)
		}
		time.Sleep(3 * time.Second)
	}
}

func (suite *liveSuite) waitForNoMutationsPlan(t *testing.T, specPath string) cliJSONResult {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		result, _ := suite.runCLIJSON(t, "plan", specPath)
		if diff.MutatingActionCount(result.Plan) == 0 {
			return result
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for zero mutating actions; last plan=%+v", result.Plan.Actions)
		}
		time.Sleep(3 * time.Second)
	}
}
