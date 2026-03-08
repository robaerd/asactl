package appleadsapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/robaerd/asactl/internal/appleadsapi"
	"github.com/robaerd/asactl/internal/auth"
)

func TestClientRetries429(t *testing.T) {
	var tokenCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()
	attempt := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "1"}}})
	}))
	defer apiServer.Close()
	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, tokenServer.Client(), auth.WithTokenURL(tokenServer.URL))
	client := appleadsapi.NewClient(provider, appleadsapi.WithOrgID("org"), appleadsapi.WithBaseURL(apiServer.URL))
	var out map[string]any
	if err := client.Get(context.Background(), "/campaigns", nil, &out); err != nil {
		t.Fatalf("client get: %v", err)
	}
	if attempt != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempt)
	}
}

func TestClientRetries5xx(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	attempt := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt++
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporary","credential":"top-secret"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"id": "1"}}})
	}))
	defer apiServer.Close()

	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, tokenServer.Client(), auth.WithTokenURL(tokenServer.URL))
	client := appleadsapi.NewClient(provider, appleadsapi.WithOrgID("org"), appleadsapi.WithBaseURL(apiServer.URL))
	var out map[string]any
	if err := client.Get(context.Background(), "/campaigns", nil, &out); err != nil {
		t.Fatalf("client get: %v", err)
	}
	if attempt != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempt)
	}
}

func TestClientAPIErrorsDoNotEchoRawPayload(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid","client_secret":"super-secret-value"}`))
	}))
	defer apiServer.Close()

	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, tokenServer.Client(), auth.WithTokenURL(tokenServer.URL))
	client := appleadsapi.NewClient(provider, appleadsapi.WithOrgID("org"), appleadsapi.WithBaseURL(apiServer.URL))

	err := client.Get(context.Background(), "/campaigns", nil, nil)
	if err == nil {
		t.Fatal("expected API error")
	}
	if !strings.Contains(err.Error(), "Bad Request") || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("expected sanitized status message, got: %v", err)
	}
	if strings.Contains(err.Error(), "super-secret-value") || strings.Contains(err.Error(), "client_secret") {
		t.Fatalf("error leaked raw payload: %v", err)
	}
}

func TestClientExtractsNestedAPIErrors(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"errors":[{"field":"startTime","message":"Start time must be in the future","messageCode":"START_TIME_INVALID"}]}}`))
	}))
	defer apiServer.Close()

	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, tokenServer.Client(), auth.WithTokenURL(tokenServer.URL))
	client := appleadsapi.NewClient(provider, appleadsapi.WithOrgID("org"), appleadsapi.WithBaseURL(apiServer.URL))

	err := client.Get(context.Background(), "/campaigns", nil, nil)
	if err == nil {
		t.Fatal("expected API error")
	}
	text := err.Error()
	for _, want := range []string{"Bad Request", "startTime", "Start time must be in the future", "START_TIME_INVALID"} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected %q in error, got %v", want, err)
		}
	}
}

func TestClientPostDoesNotRetry429(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	var attempts atomic.Int32
	var payloads []string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		payloads = append(payloads, string(body))
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 1}})
	}))
	defer apiServer.Close()

	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, tokenServer.Client(), auth.WithTokenURL(tokenServer.URL))
	client := appleadsapi.NewClient(provider, appleadsapi.WithOrgID("org"), appleadsapi.WithBaseURL(apiServer.URL))

	err := client.Post(context.Background(), "/campaigns", map[string]any{"name": "US - Brand - Exact"}, nil)
	if err == nil {
		t.Fatal("expected rate-limit error")
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts.Load())
	}
	if len(payloads) != 1 {
		t.Fatalf("expected one payload, got %v", payloads)
	}
	if !strings.Contains(err.Error(), "Too Many Requests") {
		t.Fatalf("expected rate-limit message, got %v", err)
	}
}

func TestClientPostRefreshesTokenAfter401(t *testing.T) {
	var tokenCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := tokenCalls.Add(1)
		token := "token-1"
		if call > 1 {
			token = "token-2"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "expires_in": 3600})
	}))
	defer tokenServer.Close()

	var authHeaders []string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		if len(authHeaders) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"expired"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"id": 1}})
	}))
	defer apiServer.Close()

	tokenConfig := testAuthConfig(t)
	provider := auth.NewTokenProvider(tokenConfig, tokenServer.Client(), auth.WithTokenURL(tokenServer.URL))
	client := appleadsapi.NewClient(provider, appleadsapi.WithOrgID("org"), appleadsapi.WithBaseURL(apiServer.URL))

	if err := client.Post(context.Background(), "/campaigns", map[string]any{"name": "US - Brand - Exact"}, nil); err != nil {
		t.Fatalf("client post: %v", err)
	}
	if len(authHeaders) != 2 {
		t.Fatalf("expected 2 auth attempts, got %d", len(authHeaders))
	}
	if authHeaders[0] == authHeaders[1] {
		t.Fatalf("expected refreshed token on retry, got %v", authHeaders)
	}
}
