package appleadsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/robaerd/asactl/internal/auth"
	"github.com/robaerd/asactl/internal/logging"
)

type Client struct {
	baseURL    string
	orgID      string
	httpClient *http.Client
	tokens     tokenSource
	maxRetries int
	backoff    time.Duration
	randFloat  func() float64
	sleep      func(time.Duration)
	logger     *slog.Logger
}

type ClientOption func(*Client)

type tokenSource interface {
	AccessToken(context.Context, bool) (string, error)
}

type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	if e.StatusCode == 0 {
		return e.Message
	}
	return fmt.Sprintf("api error: status=%d message=%s", e.StatusCode, e.Message)
}

type requestOptions struct {
	includeOrgHeaders bool
}

func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(client *Client) {
		client.logger = logging.Component(logger, "appleadsapi.client")
	}
}

func WithOrgID(orgID string) ClientOption {
	return func(client *Client) {
		client.orgID = strings.TrimSpace(orgID)
	}
}

func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(client *Client) {
		if httpClient != nil {
			client.httpClient = httpClient
		}
	}
}

func WithBaseURL(baseURL string) ClientOption {
	return func(client *Client) {
		if trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/"); trimmed != "" {
			client.baseURL = trimmed
		}
	}
}

func NewClient(tokenProvider *auth.TokenProvider, options ...ClientOption) *Client {
	return newClient(tokenProvider, options...)
}

func newClient(tokenProvider tokenSource, options ...ClientOption) *Client {
	client := &Client{
		baseURL:    auth.DefaultAPIBaseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		tokens:     tokenProvider,
		maxRetries: 3,
		backoff:    300 * time.Millisecond,
		randFloat:  rand.Float64,
		sleep:      time.Sleep,
		logger:     logging.Component(nil, "appleadsapi.client"),
	}
	for _, option := range options {
		if option != nil {
			option(client)
		}
	}
	return client
}

func (c *Client) Get(ctx context.Context, path string, query url.Values, out any) error {
	payload, err := c.requestJSON(ctx, http.MethodGet, path, query, nil)
	if err != nil {
		return err
	}
	return unmarshalPayload(payload, out)
}

func (c *Client) GetUnscoped(ctx context.Context, path string, query url.Values, out any) error {
	payload, err := c.requestJSONWithOptions(ctx, http.MethodGet, path, query, nil, requestOptions{includeOrgHeaders: false})
	if err != nil {
		return err
	}
	return unmarshalPayload(payload, out)
}

func (c *Client) Post(ctx context.Context, path string, body any, out any) error {
	payload, err := c.requestJSON(ctx, http.MethodPost, path, nil, body)
	if err != nil {
		return err
	}
	return unmarshalPayload(payload, out)
}

func (c *Client) Put(ctx context.Context, path string, body any, out any) error {
	payload, err := c.requestJSON(ctx, http.MethodPut, path, nil, body)
	if err != nil {
		return err
	}
	return unmarshalPayload(payload, out)
}

func (c *Client) Delete(ctx context.Context, path string, query url.Values, out any) error {
	payload, err := c.requestJSON(ctx, http.MethodDelete, path, query, nil)
	if err != nil {
		return err
	}
	return unmarshalPayload(payload, out)
}

func (c *Client) requestJSON(ctx context.Context, method, path string, query url.Values, body any) ([]byte, error) {
	return c.requestJSONWithOptions(ctx, method, path, query, body, requestOptions{includeOrgHeaders: true})
}

func (c *Client) requestJSONWithOptions(ctx context.Context, method, path string, query url.Values, body any, options requestOptions) ([]byte, error) {
	requestURL := c.baseURL + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	attempts := c.maxRetries + 1
	refreshed := false
	retryable := isRetryableMethod(method)
	for attempt := 1; attempt <= attempts; attempt++ {
		logger := c.logger.With("method", method, "path", path, "attempt", attempt)
		if len(query) > 0 {
			logger = logger.With("query", query.Encode())
		}
		var reader io.Reader
		if body != nil {
			payload, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			reader = bytes.NewReader(payload)
			logger.Debug("Preparing API request", "body_bytes", len(payload))
		} else {
			logger.Debug("Preparing API request")
		}
		token, err := c.tokens.AccessToken(ctx, false)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, requestURL, reader)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		if options.includeOrgHeaders {
			req.Header.Set("X-AP-Context", "orgId="+c.orgID)
			req.Header.Set("X-AP-Org-Id", c.orgID)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			if !retryable || attempt == attempts {
				return nil, err
			}
			delay := c.delay(attempt)
			logger.Warn("Retrying API request", "reason", "transport_error", "delay", delay, "error", err)
			c.sleep(delay)
			continue
		}
		payload, err := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshed {
			logger.Debug("Refreshing access token after unauthorized response", "status_code", resp.StatusCode)
			if _, err := c.tokens.AccessToken(ctx, true); err != nil {
				return nil, err
			}
			refreshed = true
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if !retryable || attempt == attempts {
				return nil, &APIError{StatusCode: resp.StatusCode, Message: apiErrorMessage(resp.StatusCode, payload)}
			}
			delay := c.retryDelay(attempt, resp.Header.Get("Retry-After"))
			logger.Warn("Retrying API request", "status_code", resp.StatusCode, "delay", delay, "retry_after", strings.TrimSpace(resp.Header.Get("Retry-After")))
			c.sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			logger.Debug("API request failed", "status_code", resp.StatusCode)
			return nil, &APIError{StatusCode: resp.StatusCode, Message: apiErrorMessage(resp.StatusCode, payload)}
		}
		logger.Debug("API request completed", "status_code", resp.StatusCode, "response_bytes", len(payload))
		return payload, nil
	}
	return nil, &APIError{Message: "request exhausted retries"}
}

func isRetryableMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func (c *Client) delay(attempt int) time.Duration {
	base := c.backoff * time.Duration(1<<(attempt-1))
	jitter := time.Duration(float64(base) * 0.2 * c.randFloat())
	return base + jitter
}

func (c *Client) retryDelay(attempt int, retryAfterHeader string) time.Duration {
	if retryAfter, ok := parseRetryAfter(retryAfterHeader, time.Now()); ok {
		return retryAfter
	}
	return c.delay(attempt)
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}

	seconds, err := strconv.Atoi(trimmed)
	if err == nil {
		if seconds <= 0 {
			return 0, true
		}
		return time.Duration(seconds) * time.Second, true
	}

	retryAt, err := http.ParseTime(trimmed)
	if err != nil {
		return 0, false
	}

	delay := retryAt.Sub(now)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func apiErrorMessage(statusCode int, payload []byte) string {
	statusText := strings.TrimSpace(http.StatusText(statusCode))
	detail := extractAPIErrorDetail(payload)
	switch {
	case statusText != "" && detail != "":
		return statusText + ": " + detail
	case detail != "":
		return detail
	case statusText != "":
		return statusText
	default:
		return "request failed"
	}
}

func extractAPIErrorDetail(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return ""
	}
	details := collectErrorDetails(decoded)
	if len(details) == 0 {
		return ""
	}
	return strings.Join(details, "; ")
}

func collectErrorDetails(value any) []string {
	seen := map[string]struct{}{}
	details := make([]string, 0)
	add := func(message string) {
		trimmed := strings.TrimSpace(message)
		if trimmed == "" {
			return
		}
		if _, ok := seen[trimmed]; ok {
			return
		}
		seen[trimmed] = struct{}{}
		details = append(details, trimmed)
	}
	var visit func(any)
	visit = func(item any) {
		switch typed := item.(type) {
		case map[string]any:
			for _, key := range []string{"error", "message", "detail", "reason", "messageCode", "field"} {
				if text, ok := typed[key].(string); ok {
					add(text)
				}
			}
			for _, key := range []string{"error", "errors"} {
				if nested, ok := typed[key]; ok {
					visit(nested)
				}
			}
			for _, nested := range typed {
				switch nested.(type) {
				case map[string]any, []any:
					visit(nested)
				}
			}
		case []any:
			for _, nested := range typed {
				visit(nested)
			}
		}
	}
	visit(value)
	return details
}

func unmarshalPayload(payload []byte, out any) error {
	if len(payload) == 0 || out == nil {
		return nil
	}
	return json.Unmarshal(payload, out)
}
