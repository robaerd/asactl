package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robaerd/asactl/internal/auth"
)

func TestTokenProviderCachesTokenAndUsesAppleRequestShape(t *testing.T) {
	fixedNow := time.Unix(1_700_000_000, 0).UTC()
	privateKey, privateKeyPEM := sec1PrivateKey(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("unexpected grant_type %q", got)
		}
		if got := r.Form.Get("client_id"); got != "client-id" {
			t.Fatalf("unexpected client_id %q", got)
		}
		if got := r.Form.Get("scope"); got != "searchadsorg" {
			t.Fatalf("unexpected scope %q", got)
		}

		secret := r.Form.Get("client_secret")
		if secret == "" {
			t.Fatal("expected client_secret JWT")
		}
		parts := strings.Split(secret, ".")
		if len(parts) != 3 {
			t.Fatalf("expected JWT with 3 parts, got %d", len(parts))
		}

		header := decodeSegment[tJWTHeader](t, parts[0])
		if header.Algorithm != "ES256" {
			t.Fatalf("unexpected alg %q", header.Algorithm)
		}
		if header.KeyID != "key-id" {
			t.Fatalf("unexpected kid %q", header.KeyID)
		}

		claims := decodeSegment[tJWTClaims](t, parts[1])
		if claims.Issuer != "team-id" {
			t.Fatalf("unexpected iss %q", claims.Issuer)
		}
		if claims.Subject != "client-id" {
			t.Fatalf("unexpected sub %q", claims.Subject)
		}
		if claims.Audience != "https://appleid.apple.com" {
			t.Fatalf("unexpected aud %q", claims.Audience)
		}
		if claims.IssuedAt != fixedNow.Unix() {
			t.Fatalf("unexpected iat %d", claims.IssuedAt)
		}
		if ttl := claims.Expires - claims.IssuedAt; ttl > int64((180 * 24 * time.Hour).Seconds()) {
			t.Fatalf("client secret ttl too large: %d", ttl)
		}
		signature, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			t.Fatalf("decode signature: %v", err)
		}
		if len(signature) != 64 {
			t.Fatalf("unexpected signature length %d", len(signature))
		}
		digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		rInt := new(big.Int).SetBytes(signature[:32])
		sInt := new(big.Int).SetBytes(signature[32:])
		if !ecdsa.Verify(&privateKey.PublicKey, digest[:], rInt, sInt) {
			t.Fatal("JWT signature verification failed")
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token-1", "expires_in": 3600})
	}))
	defer server.Close()

	provider := auth.NewTokenProvider(auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: privateKeyPEM,
	}, server.Client(),
		auth.WithClock(func() time.Time { return fixedNow }),
		auth.WithTokenURL(server.URL),
	)

	ctx := context.Background()
	token1, err := provider.AccessToken(ctx, false)
	if err != nil {
		t.Fatalf("access token: %v", err)
	}
	token2, err := provider.AccessToken(ctx, false)
	if err != nil {
		t.Fatalf("access token (cached): %v", err)
	}
	if token1 != token2 {
		t.Fatalf("expected cached token, got %q and %q", token1, token2)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one token request, got %d", calls.Load())
	}
}

func TestConfigFromInputsReadsPrivateKeyAndDefaultsTokenURL(t *testing.T) {
	privateKeyPath := filepath.Join(t.TempDir(), "appleads-private.pem")
	if err := os.WriteFile(privateKeyPath, []byte(pkcs8PrivateKeyPEM(t)), 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}

	config, err := auth.ConfigFromInputs(auth.Inputs{
		ClientID:       "client-id",
		TeamID:         "team-id",
		KeyID:          "key-id",
		PrivateKeyPath: privateKeyPath,
	})
	if err != nil {
		t.Fatalf("config from inputs: %v", err)
	}
	if config.ClientID != "client-id" || config.TeamID != "team-id" || config.KeyID != "key-id" {
		t.Fatalf("unexpected config: %+v", config)
	}
	if !strings.Contains(config.PrivateKeyPEM, "BEGIN PRIVATE KEY") {
		t.Fatalf("expected PKCS#8 PEM, got %q", config.PrivateKeyPEM)
	}
}

func TestConfigFromInputsRejectsInsecurePrivateKeyPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}
	privateKeyPath := filepath.Join(t.TempDir(), "appleads-private.pem")
	if err := os.WriteFile(privateKeyPath, []byte(pkcs8PrivateKeyPEM(t)), 0o644); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	if err := os.Chmod(privateKeyPath, 0o644); err != nil {
		t.Fatalf("chmod private key: %v", err)
	}

	_, err := auth.ConfigFromInputs(auth.Inputs{
		ClientID:       "client-id",
		TeamID:         "team-id",
		KeyID:          "key-id",
		PrivateKeyPath: privateKeyPath,
	})
	if err == nil {
		t.Fatal("expected insecure permission error")
	}
	if !strings.Contains(err.Error(), "permissions are too open") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTokenProviderReturnsErrorForMalformedTokenResponse(t *testing.T) {
	privateKeyPEM := pkcs8PrivateKeyPEM(t)
	testCases := []struct {
		name string
		body string
	}{
		{
			name: "invalid json",
			body: `{"access_token":"token"`,
		},
		{
			name: "unsupported expires_in type",
			body: `{"access_token":"token","expires_in":{"seconds":3600}}`,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(testCase.body))
			}))
			defer server.Close()

			provider := auth.NewTokenProvider(auth.Config{
				ClientID:      "client-id",
				TeamID:        "team-id",
				KeyID:         "key-id",
				PrivateKeyPEM: privateKeyPEM,
			}, server.Client(), auth.WithTokenURL(server.URL))

			_, err := provider.AccessToken(context.Background(), false)
			if err == nil {
				t.Fatal("expected malformed token response error")
			}
		})
	}
}

func TestTokenProviderDoesNotEchoTokenErrorBody(t *testing.T) {
	privateKeyPEM := pkcs8PrivateKeyPEM(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_client","client_secret":"super-secret-value"}`))
	}))
	defer server.Close()

	provider := auth.NewTokenProvider(auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: privateKeyPEM,
	}, server.Client(), auth.WithTokenURL(server.URL))

	_, err := provider.AccessToken(context.Background(), false)
	if err == nil {
		t.Fatal("expected token request error")
	}
	if !strings.Contains(err.Error(), "token request failed with HTTP 400") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(err.Error(), "super-secret-value") || strings.Contains(err.Error(), "client_secret") {
		t.Fatalf("error leaked sensitive payload: %v", err)
	}
}

func TestTokenProviderTreatsZeroExpiresInAsImmediateExpiry(t *testing.T) {
	privateKeyPEM := pkcs8PrivateKeyPEM(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token-" + strconv.Itoa(int(call)), "expires_in": 0})
	}))
	defer server.Close()

	provider := auth.NewTokenProvider(auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: privateKeyPEM,
	}, server.Client(), auth.WithTokenURL(server.URL))

	token1, err := provider.AccessToken(context.Background(), false)
	if err != nil {
		t.Fatalf("access token: %v", err)
	}
	token2, err := provider.AccessToken(context.Background(), false)
	if err != nil {
		t.Fatalf("access token second call: %v", err)
	}
	if token1 == token2 {
		t.Fatalf("expected immediate expiry to force refresh, got identical token %q", token1)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected two token requests, got %d", calls.Load())
	}
}

func TestTokenProviderHonorsFractionalExpiresIn(t *testing.T) {
	privateKeyPEM := pkcs8PrivateKeyPEM(t)
	fixedNow := time.Unix(1_700_000_000, 0).UTC()
	currentTime := fixedNow
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call := calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "token-" + strconv.Itoa(int(call)), "expires_in": 0.9})
	}))
	defer server.Close()

	provider := auth.NewTokenProvider(auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: privateKeyPEM,
	}, server.Client(),
		auth.WithClock(func() time.Time { return currentTime }),
		auth.WithTokenURL(server.URL),
		auth.WithAccessTokenRefreshSkew(0),
	)

	token1, err := provider.AccessToken(context.Background(), false)
	if err != nil {
		t.Fatalf("access token: %v", err)
	}
	currentTime = fixedNow.Add(800 * time.Millisecond)
	token2, err := provider.AccessToken(context.Background(), false)
	if err != nil {
		t.Fatalf("cached access token: %v", err)
	}
	if token1 != token2 {
		t.Fatalf("expected token reuse before fractional expiry, got %q and %q", token1, token2)
	}
	currentTime = fixedNow.Add(time.Second)
	token3, err := provider.AccessToken(context.Background(), false)
	if err != nil {
		t.Fatalf("refreshed access token: %v", err)
	}
	if token3 == token2 {
		t.Fatalf("expected refresh after fractional expiry, got %q", token3)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected two token requests, got %d", calls.Load())
	}
}

func TestTokenProviderReturnsNetworkError(t *testing.T) {
	provider := auth.NewTokenProvider(auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: pkcs8PrivateKeyPEM(t),
	}, &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, context.DeadlineExceeded
		}),
	}, auth.WithTokenURL("https://appleid.test/token"))

	_, err := provider.AccessToken(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("expected network error, got %v", err)
	}
}

func TestTokenProviderRejectsMalformedPrivateKey(t *testing.T) {
	provider := auth.NewTokenProvider(auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: "not-a-pem",
	}, nil, auth.WithTokenURL("https://appleid.test/token"))

	_, err := provider.AccessToken(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "private key PEM is invalid") {
		t.Fatalf("expected malformed private key error, got %v", err)
	}
}

type tJWTHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
}

type tJWTClaims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Audience string `json:"aud"`
	IssuedAt int64  `json:"iat"`
	Expires  int64  `json:"exp"`
}

func decodeSegment[T any](t *testing.T, segment string) T {
	t.Helper()
	payload, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("decode segment: %v", err)
	}
	var decoded T
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal segment: %v", err)
	}
	return decoded
}

func sec1PrivateKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal EC private key: %v", err)
	}
	return key, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
}

func pkcs8PrivateKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS#8 private key: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}
