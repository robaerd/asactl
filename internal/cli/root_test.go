package cli_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robaerd/asactl/internal/cli"
	syncpkg "github.com/robaerd/asactl/internal/sync"
	"github.com/robaerd/asactl/internal/userconfig"
	"github.com/spf13/cobra"
)

func newConfiguredRootCommand(version, tokenURL, apiBaseURL string) *cobra.Command {
	dependencies := cli.RootDependencies{}
	if strings.TrimSpace(tokenURL) != "" || strings.TrimSpace(apiBaseURL) != "" {
		dependencies.SyncEngineOptions = []syncpkg.EngineOption{
			syncpkg.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}),
			syncpkg.WithTokenURL(tokenURL),
			syncpkg.WithAPIBaseURL(apiBaseURL),
		}
	}
	return cli.NewRootCommandWithDeps(version, dependencies)
}

func TestValidateCommandRuns(t *testing.T) {
	cmd := cli.NewRootCommand("test-version")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"validate", "../../examples/starter.yaml"})
	err := cmd.Execute()
	if err != nil {
		t.Fatalf("validate command: %v stderr=%s", err, stderr.String())
	}
}

func TestRootVersionFlagOutputsVersion(t *testing.T) {
	cmd := cli.NewRootCommand("1.2.3-test")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"--version"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("version command: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "1.2.3-test") {
		t.Fatalf("expected version output, got %q", stdout.String())
	}
}

func TestHelpIncludesLongDescription(t *testing.T) {
	cmd := cli.NewRootCommand("test-version")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"plan", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("help command: %v stderr=%s", err, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "Fetch live Apple Ads state for the configured campaign group and app scope") {
		t.Fatalf("expected long help text, got %q", output)
	}
	if !strings.Contains(output, "--recreate") || !strings.Contains(output, "--wipe-org") {
		t.Fatalf("expected recreate and wipe-org flags in help output, got %q", output)
	}
}

func TestCheckAuthCommandRuns(t *testing.T) {
	cmd := cli.NewRootCommand("test-version")

	checkAuthCmd, _, err := cmd.Find([]string{"check-auth"})
	if err != nil {
		t.Fatalf("find check-auth command: %v", err)
	}
	if checkAuthCmd == nil {
		t.Fatal("expected check-auth command to exist")
	}
}

func TestCheckAuthJSONOutputsExpectedFields(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/acls":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"orgId":       "20744842",
					"orgName":     "Readcap - EN",
					"displayName": "Readcap - EN",
					"currency":    "EUR",
					"timeZone":    "UTC",
					"roleNames":   []string{"API Campaign Manager"},
				}},
				"error":      nil,
				"pagination": nil,
			})
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":     "pp1",
					"adamId": 123456,
					"name":   "Reading Tracker",
					"state":  "AVAILABLE",
				}},
			})
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{
					"id":                 1,
					"adamId":             123456,
					"name":               "US - Brand - Exact",
					"countriesOrRegions": []string{"US"},
					"dailyBudgetAmount":  map[string]any{"amount": "1.50", "currency": "EUR"},
					"status":             "ENABLED",
				}},
				"pagination": map[string]any{"itemsPerPage": 1, "startIndex": 0, "totalResults": 1},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"--json", "check-auth", specPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("check-auth command: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}

	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("decode json output: %v output=%s", err, stdout.String())
	}
	if err := decoder.Decode(&map[string]any{}); err != io.EOF {
		t.Fatalf("expected a single JSON document, got extra output: %s", stdout.String())
	}
	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}
	if payload["campaign_group_id"] != "20744842" {
		t.Fatalf("expected campaign_group_id=20744842, got %v", payload["campaign_group_id"])
	}
	if payload["org_name"] != "Readcap - EN" {
		t.Fatalf("expected org_name, got %v", payload["org_name"])
	}
	if payload["app_id"] != "123456" {
		t.Fatalf("expected app_id=123456, got %v", payload["app_id"])
	}
	if payload["product_page_count"] != float64(1) {
		t.Fatalf("expected product_page_count=1, got %v", payload["product_page_count"])
	}
	if _, ok := payload["scope_summary"]; !ok {
		t.Fatalf("expected scope_summary in payload, got %v", payload)
	}
}

func TestValidateCommandRunsWithManifest(t *testing.T) {
	manifestPath := writeManifestFixture(t)

	cmd := cli.NewRootCommand("test-version")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"validate", manifestPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("validate manifest command: %v stderr=%s", err, stderr.String())
	}
}

func TestPlanAndApplyExposeRecreateFlag(t *testing.T) {
	cmd := cli.NewRootCommand("test-version")

	planCmd, _, err := cmd.Find([]string{"plan"})
	if err != nil {
		t.Fatalf("find plan command: %v", err)
	}
	if planCmd.Flags().Lookup("recreate") == nil {
		t.Fatal("plan command missing recreate flag")
	}
	if planCmd.Flags().Lookup("wipe-org") == nil {
		t.Fatal("plan command missing wipe-org flag")
	}

	applyCmd, _, err := cmd.Find([]string{"apply"})
	if err != nil {
		t.Fatalf("find apply command: %v", err)
	}
	if applyCmd.Flags().Lookup("recreate") == nil {
		t.Fatal("apply command missing recreate flag")
	}
	if applyCmd.Flags().Lookup("wipe-org") == nil {
		t.Fatal("apply command missing wipe-org flag")
	}
}

func TestPlanRejectsMutuallyExclusiveRecreateFlags(t *testing.T) {
	cmd := cli.NewRootCommand("test-version")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"plan", "../../examples/starter.yaml", "--recreate", "--wipe-org"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected mutually exclusive flag error")
	}
	if !strings.Contains(err.Error(), "if any flags in the group") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloneSupportsManifest(t *testing.T) {
	manifestPath := writeManifestFixture(t)
	dstPath := filepath.Join(t.TempDir(), "cloned.yaml")

	cmd := cli.NewRootCommand("test-version")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"clone", manifestPath, dstPath, "--storefront", "GB"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("clone manifest command: %v stderr=%s", err, stderr.String())
	}
	cloned, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read cloned config: %v", err)
	}
	if !strings.Contains(string(cloned), "kind: Config\n") {
		t.Fatalf("expected standalone config output, got %s", cloned)
	}
	if !strings.Contains(string(cloned), "name: UK - Brand") {
		t.Fatalf("expected cloned campaign names, got %s", cloned)
	}
}

func TestFmtWriteFormatsManifest(t *testing.T) {
	manifestPath := writeUnformattedManifestFixture(t)

	cmd := cli.NewRootCommand("test-version")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"fmt", manifestPath, "--write"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("fmt manifest command: %v stderr=%s", err, stderr.String())
	}

	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(manifestBytes), "campaigns:\n  - campaigns/us.yaml\n") {
		t.Fatalf("expected formatted manifest campaigns, got %s", manifestBytes)
	}

	baseBytes, err := os.ReadFile(filepath.Join(filepath.Dir(manifestPath), "base.yaml"))
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	if !strings.Contains(string(baseBytes), "kind: Base\n") {
		t.Fatalf("expected formatted base kind, got %s", baseBytes)
	}

	fragmentBytes, err := os.ReadFile(filepath.Join(filepath.Dir(manifestPath), "campaigns", "us.yaml"))
	if err != nil {
		t.Fatalf("read fragment: %v", err)
	}
	if !strings.Contains(string(fragmentBytes), "kind: Campaigns\n") {
		t.Fatalf("expected formatted fragment kind, got %s", fragmentBytes)
	}
}

func TestApplyJSONOutputsOnlyJSONForMutations(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data":       []map[string]any{},
					"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
				})
			case http.MethodPost:
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
			default:
				t.Fatalf("unexpected method %s for /campaigns", r.Method)
			}
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{},
			})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	privateKeyPath := filepath.Join(t.TempDir(), "appleads-private.pem")
	if err := writePrivateKeyFile(privateKeyPath); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	writeProfileConfig(t, privateKeyPath)
	specPath := filepath.Join(t.TempDir(), "spec.yaml")
	specContents := []byte(`version: 1
kind: Config
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: Readcap
  app_id: "123456"
defaults:
  currency: EUR
  devices: [IPHONE]
product_pages:
  CPP1:
    product_page_id: pp1
    name: Reading Tracker
    locale: en-US
campaigns:
  - name: US - Brand - Exact
    storefronts: [US]
    daily_budget: 1.50
    status: ACTIVE
    adgroups: []
`)
	if err := os.WriteFile(specPath, specContents, 0o600); err != nil {
		t.Fatalf("write spec: %v", err)
	}

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"--json", "apply", specPath, "--yes"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("apply command: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}

	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("decode json output: %v output=%s", err, stdout.String())
	}
	if err := decoder.Decode(&map[string]any{}); err != io.EOF {
		t.Fatalf("expected a single JSON document, got extra output: %s", stdout.String())
	}
	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}
	if payload["applied"] != true {
		t.Fatalf("expected applied=true, got %v", payload["applied"])
	}
	if _, ok := payload["scope_summary"]; !ok {
		t.Fatalf("expected scope_summary in payload, got %v", payload)
	}
}

func TestApplyConfirmationPromptRepeatsCompactSummary(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns":
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data":       []map[string]any{},
					"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
				})
			case http.MethodPost:
				t.Fatalf("apply should not mutate after a negative confirmation")
			default:
				t.Fatalf("unexpected method %s for /campaigns", r.Method)
			}
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{},
			})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetIn(io.NopCloser(bytes.NewBufferString("n\n")))
	cmd.SetArgs([]string{"apply", specPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("apply command: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}

	output := stdout.String()
	if !strings.Contains(output, "Summary: delete=0 create=1 update=0 pause=0 activate=0 noop=0 total=1") {
		t.Fatalf("expected rendered plan summary in output, got %s", output)
	}
	if strings.Contains(output, "\x1b[") {
		t.Fatalf("expected no ANSI escapes for non-tty stdout, got %q", output)
	}
	if !strings.Contains(output, "Apply 1 changes? Summary: delete=0 create=1 update=0 pause=0 activate=0 noop=0 total=1.") {
		t.Fatalf("expected confirmation prompt to repeat compact summary, got %s", output)
	}
	if !strings.Contains(output, "Apply cancelled.") {
		t.Fatalf("expected cancelled message, got %s", output)
	}
	if !slices.Equal(requests, []string{"GET /creatives", "GET /campaigns"}) {
		t.Fatalf("unexpected request order: %v", requests)
	}
}

func TestApplyConfirmationHonorsContextCancellation(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{},
			})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)
	reader, writer := io.Pipe()
	defer writer.Close()

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := newPromptSignalWriter("[y/N]: ")
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetIn(reader)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"apply", specPath})

	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Execute()
	}()

	select {
	case <-stdout.Signal():
		cancel()
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
		}
		return
	case <-time.After(2 * time.Second):
		t.Fatal("apply command did not reach confirmation before timeout")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("apply command did not return after context cancellation")
	}
}

type promptSignalWriter struct {
	want string
	done chan struct{}
	once sync.Once
	mu   sync.Mutex
	buf  bytes.Buffer
}

func newPromptSignalWriter(want string) *promptSignalWriter {
	return &promptSignalWriter{
		want: want,
		done: make(chan struct{}),
	}
}

func (w *promptSignalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	if strings.Contains(w.buf.String(), w.want) {
		w.once.Do(func() { close(w.done) })
	}
	return n, err
}

func (w *promptSignalWriter) Signal() <-chan struct{} {
	return w.done
}

func (w *promptSignalWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestApplyRejectsNonClosableInteractiveInput(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetIn(strings.NewReader("n\n"))
	cmd.SetArgs([]string{"apply", specPath})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected non-closable interactive input error")
	}
	if !strings.Contains(err.Error(), "closable input reader") {
		t.Fatalf("unexpected error: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}
}

func TestPlanJSONVerboseKeepsStdoutJSONAndLogsToStderr(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{},
			})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"--json", "--verbose", "plan", specPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("plan command: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}

	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("decode json output: %v output=%s", err, stdout.String())
	}
	if err := decoder.Decode(&map[string]any{}); err != io.EOF {
		t.Fatalf("expected a single JSON document, got extra output: %s", stdout.String())
	}
	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}

	logLines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(logLines) == 0 || strings.TrimSpace(logLines[0]) == "" {
		t.Fatalf("expected verbose logs on stderr, got %q", stderr.String())
	}
	var logEntry map[string]any
	if err := json.Unmarshal([]byte(logLines[0]), &logEntry); err != nil {
		t.Fatalf("decode first log entry: %v stderr=%s", err, stderr.String())
	}
	if logEntry["level"] != "DEBUG" {
		t.Fatalf("expected debug log level, got %v", logEntry["level"])
	}
}

func TestPlanJSONWithOutWritesSavedPlanFile(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)
	planPath := filepath.Join(t.TempDir(), "saved-plan.json")

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"--json", "plan", specPath, "--out", planPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("plan --out command: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}

	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("decode json output: %v output=%s", err, stdout.String())
	}
	if err := decoder.Decode(&map[string]any{}); err != io.EOF {
		t.Fatalf("expected a single JSON document, got extra output: %s", stdout.String())
	}
	if payload["ok"] != true {
		t.Fatalf("expected ok=true, got %v", payload["ok"])
	}
	if payload["plan_file"] != planPath {
		t.Fatalf("expected plan_file=%q, got %v", planPath, payload["plan_file"])
	}
	if !strings.Contains(stderr.String(), `"msg":"Saved plan written"`) {
		t.Fatalf("expected saved plan write log on stderr, got %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), `"plan_file":"`+planPath+`"`) {
		t.Fatalf("expected saved plan log to include plan file path, got %s", stderr.String())
	}

	content, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read saved plan: %v", err)
	}
	saved, ok, err := syncpkg.ParseSavedPlan(content)
	if err != nil {
		t.Fatalf("parse saved plan: %v", err)
	}
	if !ok {
		t.Fatal("expected saved plan artifact to be recognized")
	}
	if saved.Kind != syncpkg.SavedPlanKind || saved.Version != syncpkg.SavedPlanVersion {
		t.Fatalf("unexpected saved plan header: %#v", saved)
	}
	if saved.Profile != "default" {
		t.Fatalf("expected saved plan profile=default, got %q", saved.Profile)
	}
	if !strings.Contains(saved.SpecYAML, "campaign_group:\n  id: \"20744842\"") {
		t.Fatalf("expected resolved spec yaml in saved plan, got %s", saved.SpecYAML)
	}
}

func TestApplySavedPlanUsesExplicitPlanWithoutRefetch(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	requests := []string{}
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/campaigns":
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"data":       []map[string]any{},
					"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
				})
			case http.MethodPost:
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
			default:
				t.Fatalf("unexpected method %s for /campaigns", r.Method)
			}
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)
	planPath := writeSavedPlanFixture(t, specPath, tokenServer.URL, apiServer.URL)

	requests = nil

	cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"--json", "apply", planPath, "--yes"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("apply saved plan: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}

	if !slices.Equal(requests, []string{"POST /campaigns"}) {
		t.Fatalf("expected explicit saved plan apply to avoid refetches, got requests %v", requests)
	}
	if !strings.Contains(stderr.String(), `"msg":"Applying saved plan"`) {
		t.Fatalf("expected saved plan apply log on stderr, got %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), `"plan_file":"`+planPath+`"`) {
		t.Fatalf("expected saved plan apply log to include plan file path, got %s", stderr.String())
	}
}

func TestApplySavedPlanRejectsConfigOnlyFlags(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/campaigns":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		case "/apps/123456/product-pages":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
		case "/creatives":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]any{},
				"pagination": map[string]any{"itemsPerPage": 0, "startIndex": 0, "totalResults": 0},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer apiServer.Close()

	specPath := writeApplySpec(t)
	planPath := writeSavedPlanFixture(t, specPath, tokenServer.URL, apiServer.URL)

	testCases := []struct {
		name        string
		args        []string
		expectError string
	}{
		{name: "profile", args: []string{"apply", planPath, "--yes", "--profile", "other"}, expectError: "--profile cannot be used when applying a saved plan"},
		{name: "recreate", args: []string{"apply", planPath, "--yes", "--recreate"}, expectError: "--recreate and --wipe-org cannot be used when applying a saved plan"},
		{name: "wipe-org", args: []string{"apply", planPath, "--yes", "--wipe-org"}, expectError: "--recreate and --wipe-org cannot be used when applying a saved plan"},
		{name: "root", args: []string{"apply", planPath, "--yes", "--root", t.TempDir()}, expectError: "--root cannot be used when applying a saved plan"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cmd := newConfiguredRootCommand("test-version", tokenServer.URL, apiServer.URL)
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			cmd.SetOut(stdout)
			cmd.SetErr(stderr)
			cmd.SetArgs(testCase.args)

			err := cmd.Execute()
			if err == nil {
				t.Fatal("expected saved plan flag rejection error")
			}
			if !strings.Contains(err.Error(), testCase.expectError) {
				t.Fatalf("expected error containing %q, got %v stderr=%s stdout=%s", testCase.expectError, err, stderr.String(), stdout.String())
			}
		})
	}
}

func writeApplySpec(t *testing.T) string {
	t.Helper()
	privateKeyPath := filepath.Join(t.TempDir(), "appleads-private.pem")
	if err := writePrivateKeyFile(privateKeyPath); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	writeProfileConfig(t, privateKeyPath)

	specPath := filepath.Join(t.TempDir(), "spec.yaml")
	specContents := []byte(`version: 1
kind: Config
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: Readcap
  app_id: "123456"
defaults:
  currency: EUR
  devices: [IPHONE]
product_pages:
  CPP1:
    product_page_id: pp1
    name: Reading Tracker
    locale: en-US
campaigns:
  - name: US - Brand - Exact
    storefronts: [US]
    daily_budget: 1.50
    status: ACTIVE
    adgroups: []
`)
	if err := os.WriteFile(specPath, specContents, 0o600); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return specPath
}

func writeSavedPlanFixture(t *testing.T, specPath, tokenURL, apiBaseURL string) string {
	t.Helper()
	planPath := filepath.Join(t.TempDir(), "saved-plan.json")

	cmd := newConfiguredRootCommand("test-version", tokenURL, apiBaseURL)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"plan", specPath, "--out", planPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("plan --out fixture: %v stderr=%s stdout=%s", err, stderr.String(), stdout.String())
	}
	return planPath
}

func writePrivateKeyFile(path string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600)
}

func writeProfileConfig(t *testing.T, privateKeyPath string) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.toml")
	t.Setenv(userconfig.OverrideEnvVar, configPath)
	content := `version = 1
default_profile = "default"

[profiles.default]
client_id = "client-id"
team_id = "team-id"
key_id = "key-id"
private_key_path = "` + filepath.ToSlash(privateKeyPath) + `"
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func writeManifestFixture(t *testing.T) string {
	t.Helper()
	return writeManifestFixtureFiles(t, false)
}

func writeUnformattedManifestFixture(t *testing.T) string {
	t.Helper()
	return writeManifestFixtureFiles(t, true)
}

func writeManifestFixtureFiles(t *testing.T, unformatted bool) string {
	t.Helper()
	tempDir := t.TempDir()
	campaignsDir := filepath.Join(tempDir, "campaigns")
	if err := os.MkdirAll(campaignsDir, 0o755); err != nil {
		t.Fatalf("mkdir campaigns dir: %v", err)
	}

	manifest := `version: 1
kind: Manifest
base: base.yaml
campaigns:
  - campaigns/us.yaml
`
	base := `version: 1
kind: Base
campaign_group:
  id: "20744842"
auth:
  profile: default
app:
  name: Readcap
  app_id: "123456"
defaults:
  currency: EUR
  devices: [IPHONE]
product_pages:
  CPP1:
    product_page_id: pp1
    name: Reading Tracker
    locale: en-US
`
	fragment := `version: 1
kind: Campaigns
generators:
  - name: discovery-block-brand-exact
    kind: KeywordToNegative
    spec:
      source_refs:
        campaigns:
          - US - Brand
      target_ref:
        campaign: US - Discovery
      filters:
        keyword_match_types:
          - EXACT
      generate:
        campaign_negative_keywords:
          match_type: EXACT
          status: ACTIVE
campaigns:
  - name: US - Brand
    storefronts: [US]
    daily_budget: 1.00
    status: ACTIVE
    adgroups:
      - name: Brand
        status: ACTIVE
        default_cpt_bid: 0.50
        product_page: CPP1
        targeting: KEYWORDS
        keywords:
          - text: readcap
            match_type: EXACT
            bid: 0.50
            status: ACTIVE
  - name: US - Discovery
    storefronts: [US]
    daily_budget: 1.00
    status: ACTIVE
    adgroups:
      - name: Discovery - Search Match
        status: ACTIVE
        default_cpt_bid: 0.50
        targeting: SEARCH_MATCH
`
	if unformatted {
		manifest = "version: 1\nkind: Manifest\nbase: base.yaml\ncampaigns: [campaigns/us.yaml]\n"
		base = "version: 1\nkind: Base\ncampaign_group: {id: \"20744842\"}\nauth: {profile: default}\napp: {name: Readcap, app_id: \"123456\"}\ndefaults: {currency: EUR, devices: [IPHONE]}\nproduct_pages: {CPP1: {product_page_id: pp1, name: Reading Tracker, locale: en-US}}\n"
		fragment = "version: 1\nkind: Campaigns\ngenerators: [{name: discovery-block-brand-exact, kind: KeywordToNegative, spec: {source_refs: {campaigns: [US - Brand]}, target_ref: {campaign: US - Discovery}, filters: {keyword_match_types: [EXACT]}, generate: {campaign_negative_keywords: {match_type: EXACT, status: ACTIVE}}}}]\ncampaigns: [{name: US - Brand, storefronts: [US], daily_budget: 1.00, status: ACTIVE, adgroups: [{name: Brand, status: ACTIVE, default_cpt_bid: 0.50, product_page: CPP1, targeting: KEYWORDS, keywords: [{text: readcap, match_type: EXACT, bid: 0.50, status: ACTIVE}]}]}, {name: US - Discovery, storefronts: [US], daily_budget: 1.00, status: ACTIVE, adgroups: [{name: Discovery - Search Match, status: ACTIVE, default_cpt_bid: 0.50, targeting: SEARCH_MATCH}]}]\n"
	}

	manifestPath := filepath.Join(tempDir, "asactl.yaml")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "base.yaml"), []byte(base), 0o600); err != nil {
		t.Fatalf("write base: %v", err)
	}
	if err := os.WriteFile(filepath.Join(campaignsDir, "us.yaml"), []byte(fragment), 0o600); err != nil {
		t.Fatalf("write fragment: %v", err)
	}
	return manifestPath
}
