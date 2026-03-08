package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
)

func TestParseExpiresIn(t *testing.T) {
	testCases := []struct {
		name    string
		raw     string
		want    int
		wantErr string
	}{
		{name: "number", raw: `3600`, want: 3600},
		{name: "quoted number", raw: `"3600"`, want: 3600},
		{name: "quoted decimal", raw: `"3600.0"`, want: 3600},
		{name: "null", raw: `null`, want: int(defaultAccessTokenTTL.Seconds())},
		{name: "invalid", raw: `"abc"`, wantErr: `parse expires_in "abc"`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value := json.RawMessage(testCase.raw)
			got, err := parseExpiresIn(value)
			if testCase.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
					t.Fatalf("expected error containing %q, got %v", testCase.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse expires_in: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("expected %d, got %d", testCase.want, got)
			}
		})
	}
}

func TestSignES256ProducesFixedWidthRawJOSESignature(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate private key: %v", err)
	}

	digest := sha256.Sum256([]byte("search-ads"))
	signature, err := signES256(privateKey, digest[:])
	if err != nil {
		t.Fatalf("sign ES256: %v", err)
	}
	if len(signature) != 64 {
		t.Fatalf("unexpected signature length %d", len(signature))
	}

	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])
	if !ecdsa.Verify(&privateKey.PublicKey, digest[:], r, s) {
		t.Fatal("expected JOSE signature to verify")
	}
}
