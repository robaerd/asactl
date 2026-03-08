package appleadsapi

import (
	"strings"
	"testing"
)

func TestParseItemLevelErrorSurfacesItemErrors(t *testing.T) {
	testCases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "failed item with error string",
			body:    `{"data":[{"id":"1","success":false,"error":"duplicate keyword"}]}`,
			wantErr: `item "1" failed: duplicate keyword`,
		},
		{
			name:    "item with error object",
			body:    `{"data":[{"id":"2","error":{"message":"bad keyword"}}]}`,
			wantErr: `item "2" failed: {"message":"bad keyword"}`,
		},
		{
			name:    "item with errors payload",
			body:    `{"data":[{"id":"3","errors":[{"message":"blocked"}]}]}`,
			wantErr: `item "3" failed: [{"message":"blocked"}]`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := parseItemLevelError([]byte(testCase.body))
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("expected error containing %q, got %v", testCase.wantErr, err)
			}
		})
	}
}

func TestParseItemLevelErrorRejectsMalformedJSON(t *testing.T) {
	err := parseItemLevelError([]byte(`{"data":[`))
	if err == nil || !strings.Contains(err.Error(), "parse bulk response") {
		t.Fatalf("expected parse error, got %v", err)
	}
}

func TestParseItemLevelErrorRejectsMissingData(t *testing.T) {
	err := parseItemLevelError([]byte(`{"unexpected":true}`))
	if err == nil || !strings.Contains(err.Error(), "missing data") {
		t.Fatalf("expected missing data error, got %v", err)
	}
}

func TestParseItemLevelErrorReturnsNilForSuccessfulBulkResponse(t *testing.T) {
	err := parseItemLevelError([]byte(`{"data":[{"id":"1","success":true}]}`))
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestDecodeCreatedIDSupportsCommonResponseShapes(t *testing.T) {
	testCases := []struct {
		name    string
		body    string
		wantID  string
		wantErr string
	}{
		{
			name:   "direct object",
			body:   `{"id":"123"}`,
			wantID: "123",
		},
		{
			name:   "wrapped object with numeric id",
			body:   `{"data":{"id":456}}`,
			wantID: "456",
		},
		{
			name:   "wrapped list",
			body:   `{"data":[{"id":"789"}]}`,
			wantID: "789",
		},
		{
			name:    "missing id",
			body:    `{"data":{}}`,
			wantErr: "response missing resource id",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			gotID, err := decodeCreatedID([]byte(testCase.body))
			if testCase.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
					t.Fatalf("expected error containing %q, got %v", testCase.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode created id: %v", err)
			}
			if gotID != testCase.wantID {
				t.Fatalf("expected id %q, got %q", testCase.wantID, gotID)
			}
		})
	}
}
