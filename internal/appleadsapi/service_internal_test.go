package appleadsapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestListAllStopsAtPaginationGuardByPageCount(t *testing.T) {
	restoreListGuard := setListGuardLimits(t, 2, listPageSize*4)
	defer restoreListGuard()

	var requests int
	client := newClient(
		staticTokenSource{token: "token"},
		WithHTTPClient(&http.Client{
			Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
				requests++
				offset, err := strconv.Atoi(request.URL.Query().Get("offset"))
				if err != nil {
					t.Fatalf("parse offset: %v", err)
				}
				if offset != (requests-1)*listPageSize {
					t.Fatalf("unexpected offset %d for request %d", offset, requests)
				}
				if got := request.URL.Query().Get("limit"); got != strconv.Itoa(listPageSize) {
					t.Fatalf("unexpected limit %q", got)
				}
				page := make([]map[string]any, listPageSize)
				for index := range page {
					page[index] = map[string]any{"id": offset + index + 1}
				}
				return jsonResponse(t, map[string]any{
					"data": page,
					"pagination": map[string]any{
						"itemsPerPage": listPageSize,
						"startIndex":   offset,
						"totalResults": listPageSize * 5,
					},
				}), nil
			}),
			Timeout: 5 * time.Second,
		}),
		WithBaseURL("https://appleadsapi.example"),
		WithOrgID("org"),
	)

	_, err := listAll[map[string]any](context.Background(), client, "/campaigns")
	if err == nil || !strings.Contains(err.Error(), "pagination guard triggered") {
		t.Fatalf("expected pagination guard error, got %v", err)
	}
	if requests != 2 {
		t.Fatalf("expected 2 requests before guard, got %d", requests)
	}
}

func TestListAllStopsAtPaginationGuardByRowCount(t *testing.T) {
	restoreListGuard := setListGuardLimits(t, 5, listPageSize)
	defer restoreListGuard()

	var requests int
	client := newClient(
		staticTokenSource{token: "token"},
		WithHTTPClient(&http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				requests++
				page := make([]map[string]any, listPageSize)
				for index := range page {
					page[index] = map[string]any{"id": index + 1}
				}
				return jsonResponse(t, map[string]any{
					"data": page,
					"pagination": map[string]any{
						"itemsPerPage": listPageSize,
						"startIndex":   0,
						"totalResults": listPageSize + 1,
					},
				}), nil
			}),
			Timeout: 5 * time.Second,
		}),
		WithBaseURL("https://appleadsapi.example"),
		WithOrgID("org"),
	)

	_, err := listAll[map[string]any](context.Background(), client, "/campaigns")
	if err == nil || !strings.Contains(err.Error(), "pagination guard triggered") {
		t.Fatalf("expected pagination guard error, got %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected 1 request before row guard, got %d", requests)
	}
}

func setListGuardLimits(t *testing.T, maxPages, maxRows int) func() {
	t.Helper()
	previousPages := listMaxPages
	previousRows := listMaxRows
	listMaxPages = maxPages
	listMaxRows = maxRows
	return func() {
		listMaxPages = previousPages
		listMaxRows = previousRows
	}
}

func jsonResponse(t *testing.T, payload any) *http.Response {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}
}
