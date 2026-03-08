package appleadsapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientClosesResponseBodyOnEachRetry(t *testing.T) {
	var attempts atomic.Int32
	var closes atomic.Int32
	client := newClient(staticTokenSource{token: "token"})
	client.httpClient = &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			attempt := attempts.Add(1)
			status := http.StatusTooManyRequests
			body := `{"error":"rate limited"}`
			if attempt == 2 {
				status = http.StatusOK
				body = `{"data":[{"id":"1"}]}`
			}
			return &http.Response{
				StatusCode: status,
				Header:     make(http.Header),
				Body: &countingReadCloser{
					Reader: strings.NewReader(body),
					onClose: func() {
						closes.Add(1)
					},
				},
			}, nil
		}),
		Timeout: 5 * time.Second,
	}
	client.sleep = func(time.Duration) {}
	client.randFloat = func() float64 { return 0 }

	var out map[string]any
	if err := client.Get(context.Background(), "/campaigns", nil, &out); err != nil {
		t.Fatalf("client get: %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
	if closes.Load() != 2 {
		t.Fatalf("expected 2 body closes, got %d", closes.Load())
	}
}

func TestClientUsesRetryAfterHeaderWhenRetrying(t *testing.T) {
	var attempts atomic.Int32
	client := newClient(staticTokenSource{token: "token"})
	client.httpClient = &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			attempt := attempts.Add(1)
			status := http.StatusTooManyRequests
			body := `{"error":"rate limited"}`
			if attempt == 2 {
				status = http.StatusOK
				body = `{"data":[{"id":"1"}]}`
			}
			header := make(http.Header)
			if attempt == 1 {
				header.Set("Retry-After", "2")
			}
			return &http.Response{
				StatusCode: status,
				Header:     header,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
		Timeout: 5 * time.Second,
	}

	var sleeps []time.Duration
	client.sleep = func(duration time.Duration) {
		sleeps = append(sleeps, duration)
	}
	client.randFloat = func() float64 { return 0 }

	var out map[string]any
	if err := client.Get(context.Background(), "/campaigns", nil, &out); err != nil {
		t.Fatalf("client get: %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
	if len(sleeps) != 1 {
		t.Fatalf("expected one sleep duration, got %d", len(sleeps))
	}
	if sleeps[0] != 2*time.Second {
		t.Fatalf("expected Retry-After delay of 2s, got %s", sleeps[0])
	}
}

func TestClientRetriesNetworkFailure(t *testing.T) {
	var attempts atomic.Int32
	client := newClient(staticTokenSource{token: "token"})
	client.maxRetries = 2
	client.httpClient = &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			attempts.Add(1)
			return nil, errors.New("temporary network failure")
		}),
		Timeout: 5 * time.Second,
	}

	sleepCount := 0
	client.sleep = func(time.Duration) {
		sleepCount++
	}
	client.randFloat = func() float64 { return 0 }

	err := client.Get(context.Background(), "/campaigns", nil, nil)
	if err == nil {
		t.Fatal("expected network error")
	}
	if !strings.Contains(err.Error(), "temporary network failure") {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
	if sleepCount != 2 {
		t.Fatalf("expected 2 backoff sleeps, got %d", sleepCount)
	}
}

func TestClientPostDoesNotRetryNetworkFailure(t *testing.T) {
	var attempts atomic.Int32
	client := newClient(staticTokenSource{token: "token"})
	client.maxRetries = 2
	client.httpClient = &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			attempts.Add(1)
			return nil, errors.New("temporary network failure")
		}),
		Timeout: 5 * time.Second,
	}

	sleepCount := 0
	client.sleep = func(time.Duration) {
		sleepCount++
	}
	client.randFloat = func() float64 { return 0 }

	err := client.Post(context.Background(), "/campaigns", map[string]any{"name": "US - Brand - Exact"}, nil)
	if err == nil {
		t.Fatal("expected network error")
	}
	if !strings.Contains(err.Error(), "temporary network failure") {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts.Load())
	}
	if sleepCount != 0 {
		t.Fatalf("expected 0 backoff sleeps, got %d", sleepCount)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name     string
		value    string
		want     time.Duration
		wantOkay bool
	}{
		{name: "blank", value: "", want: 0, wantOkay: false},
		{name: "seconds", value: "2", want: 2 * time.Second, wantOkay: true},
		{name: "zero seconds", value: "0", want: 0, wantOkay: true},
		{name: "future http date", value: now.Add(3 * time.Second).Format(http.TimeFormat), want: 3 * time.Second, wantOkay: true},
		{name: "past http date", value: now.Add(-3 * time.Second).Format(http.TimeFormat), want: 0, wantOkay: true},
		{name: "invalid", value: "tomorrow-ish", want: 0, wantOkay: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got, ok := parseRetryAfter(testCase.value, now)
			if ok != testCase.wantOkay {
				t.Fatalf("expected ok=%t, got %t", testCase.wantOkay, ok)
			}
			if got != testCase.want {
				t.Fatalf("expected duration %s, got %s", testCase.want, got)
			}
		})
	}
}

type staticTokenSource struct {
	token string
	err   error
}

func (source staticTokenSource) AccessToken(context.Context, bool) (string, error) {
	if source.err != nil {
		return "", source.err
	}
	return source.token, nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type countingReadCloser struct {
	io.Reader
	onClose func()
}

func (reader *countingReadCloser) Close() error {
	if reader.onClose != nil {
		reader.onClose()
	}
	return nil
}
