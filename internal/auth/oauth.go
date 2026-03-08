package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robaerd/asactl/internal/logging"
)

const (
	DefaultTokenURL        = "https://appleid.apple.com/auth/oauth2/token"
	DefaultAPIBaseURL      = "https://api.searchads.apple.com/api/v5"
	tokenAudience          = "https://appleid.apple.com"
	tokenScope             = "searchadsorg"
	defaultAccessTokenTTL  = time.Hour
	defaultClientSecretTTL = 179 * 24 * time.Hour
)

type Config struct {
	ClientID      string
	TeamID        string
	KeyID         string
	PrivateKeyPEM string
}

type Inputs struct {
	ClientID       string
	TeamID         string
	KeyID          string
	PrivateKeyPEM  string
	PrivateKeyPath string
}

type Option func(*TokenProvider)

type TokenProvider struct {
	config                  Config
	tokenURL                string
	httpClient              *http.Client
	now                     func() time.Time
	accessToken             string
	accessTokenExpiresAt    time.Time
	clientSecret            string
	clientSecretExpiresAt   time.Time
	accessTokenRefreshSkew  time.Duration
	clientSecretRefreshSkew time.Duration
	logger                  *slog.Logger
	mu                      sync.Mutex
}

type tokenResponse struct {
	AccessToken string          `json:"access_token"`
	ExpiresIn   json.RawMessage `json:"expires_in"`
}

type jwtHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
}

type jwtClaims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Audience string `json:"aud"`
	IssuedAt int64  `json:"iat"`
	Expires  int64  `json:"exp"`
}

func WithClock(now func() time.Time) Option {
	return func(provider *TokenProvider) {
		if now != nil {
			provider.now = now
		}
	}
}

func WithAccessTokenRefreshSkew(skew time.Duration) Option {
	return func(provider *TokenProvider) {
		if skew >= 0 {
			provider.accessTokenRefreshSkew = skew
		}
	}
}

func WithClientSecretRefreshSkew(skew time.Duration) Option {
	return func(provider *TokenProvider) {
		if skew >= 0 {
			provider.clientSecretRefreshSkew = skew
		}
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(provider *TokenProvider) {
		provider.logger = logging.Component(logger, "auth.oauth")
	}
}

func WithTokenURL(tokenURL string) Option {
	return func(provider *TokenProvider) {
		if trimmed := strings.TrimSpace(tokenURL); trimmed != "" {
			provider.tokenURL = trimmed
		}
	}
}

func ConfigFromInputs(input Inputs) (Config, error) {
	clientID := strings.TrimSpace(input.ClientID)
	if clientID == "" {
		return Config{}, errors.New("client_id is not configured")
	}
	teamID := strings.TrimSpace(input.TeamID)
	if teamID == "" {
		return Config{}, errors.New("team_id is not configured")
	}
	keyID := strings.TrimSpace(input.KeyID)
	if keyID == "" {
		return Config{}, errors.New("key_id is not configured")
	}
	privateKeyPEM := strings.TrimSpace(input.PrivateKeyPEM)
	privateKeyPath := strings.TrimSpace(input.PrivateKeyPath)
	if privateKeyPEM != "" && privateKeyPath != "" {
		return Config{}, errors.New("private key content and private key path are mutually exclusive")
	}
	switch {
	case privateKeyPEM != "":
		privateKeyPEM = normalizePrivateKeyPEM(privateKeyPEM)
	case privateKeyPath != "":
		if err := validatePrivateKeyFilePermissions(privateKeyPath); err != nil {
			return Config{}, err
		}
		data, err := os.ReadFile(privateKeyPath)
		if err != nil {
			return Config{}, fmt.Errorf("read private key %q: %w", privateKeyPath, err)
		}
		privateKeyPEM = normalizePrivateKeyPEM(string(data))
	default:
		return Config{}, errors.New("private key input is not configured")
	}
	return Config{
		ClientID:      clientID,
		TeamID:        teamID,
		KeyID:         keyID,
		PrivateKeyPEM: privateKeyPEM,
	}, nil
}

func normalizePrivateKeyPEM(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), `\n`, "\n")
}

func validatePrivateKeyFilePermissions(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat private key %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("private key path must reference a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("private key file permissions are too open; remove all group/other access (expected 0600 or stricter)")
	}
	return nil
}

func NewTokenProvider(config Config, httpClient *http.Client, options ...Option) *TokenProvider {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	provider := &TokenProvider{
		config:                  config,
		tokenURL:                DefaultTokenURL,
		httpClient:              httpClient,
		now:                     time.Now,
		accessTokenRefreshSkew:  30 * time.Second,
		clientSecretRefreshSkew: 5 * time.Minute,
		logger:                  logging.Component(nil, "auth.oauth"),
	}
	for _, option := range options {
		if option != nil {
			option(provider)
		}
	}
	return provider
}

func (p *TokenProvider) AccessToken(ctx context.Context, forceRefresh bool) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	if !forceRefresh && p.accessToken != "" && now.Before(p.accessTokenExpiresAt.Add(-p.accessTokenRefreshSkew)) {
		p.logger.Debug("Reusing cached access token", "expires_at", p.accessTokenExpiresAt)
		return p.accessToken, nil
	}

	clientSecret, err := p.clientSecretLocked(now)
	if err != nil {
		return "", err
	}
	p.logger.Debug("Requesting access token", "force_refresh", forceRefresh)

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", p.config.ClientID)
	form.Set("client_secret", clientSecret)
	form.Set("scope", tokenScope)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	payloadBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("token request failed with HTTP %d", resp.StatusCode)
	}

	var payload tokenResponse
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", errors.New("token response missing access_token")
	}

	expiresIn, err := parseExpiresIn(payload.ExpiresIn)
	if err != nil {
		return "", err
	}
	if expiresIn <= 0 {
		expiresIn = int(defaultAccessTokenTTL.Seconds())
	}

	p.accessToken = payload.AccessToken
	p.accessTokenExpiresAt = now.Add(time.Duration(expiresIn) * time.Second)
	p.logger.Debug("Access token refreshed", "expires_in_seconds", expiresIn, "expires_at", p.accessTokenExpiresAt)
	return p.accessToken, nil
}

func (p *TokenProvider) clientSecretLocked(now time.Time) (string, error) {
	if p.clientSecret != "" && now.Before(p.clientSecretExpiresAt.Add(-p.clientSecretRefreshSkew)) {
		p.logger.Debug("Reusing cached client secret", "expires_at", p.clientSecretExpiresAt)
		return p.clientSecret, nil
	}

	privateKey, err := parsePrivateKey(p.config.PrivateKeyPEM)
	if err != nil {
		return "", err
	}

	issuedAt := now.UTC()
	expiresAt := issuedAt.Add(defaultClientSecretTTL)

	headerJSON, err := json.Marshal(jwtHeader{Algorithm: "ES256", KeyID: p.config.KeyID})
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(jwtClaims{
		Issuer:   p.config.TeamID,
		Subject:  p.config.ClientID,
		Audience: tokenAudience,
		IssuedAt: issuedAt.Unix(),
		Expires:  expiresAt.Unix(),
	})
	if err != nil {
		return "", err
	}

	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := encodedHeader + "." + encodedClaims

	hash := sha256.Sum256([]byte(signingInput))
	signature, err := signES256(privateKey, hash[:])
	if err != nil {
		return "", fmt.Errorf("sign client secret: %w", err)
	}

	p.clientSecret = signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
	p.clientSecretExpiresAt = expiresAt
	p.logger.Debug("Client secret regenerated", "expires_at", p.clientSecretExpiresAt)
	return p.clientSecret, nil
}

func parsePrivateKey(rawPEM string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(rawPEM))
	if block == nil {
		return nil, errors.New("private key PEM is invalid")
	}

	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		if key.Curve != elliptic.P256() {
			return nil, errors.New("private key must use P-256")
		}
		return key, nil
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("private key must be an EC P-256 key in SEC1 or PKCS#8 PEM format")
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key must be ECDSA")
	}
	if key.Curve != elliptic.P256() {
		return nil, errors.New("private key must use P-256")
	}
	return key, nil
}

func parseExpiresIn(value json.RawMessage) (int, error) {
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" || trimmed == "null" {
		return int(defaultAccessTokenTTL.Seconds()), nil
	}

	var numeric json.Number
	if err := json.Unmarshal(value, &numeric); err == nil {
		return parseExpiresInSeconds(numeric.String())
	}

	var quoted string
	if err := json.Unmarshal(value, &quoted); err == nil {
		if strings.TrimSpace(quoted) == "" {
			return int(defaultAccessTokenTTL.Seconds()), nil
		}
		return parseExpiresInSeconds(quoted)
	}

	return 0, errors.New("unsupported expires_in payload type")
}

func parseExpiresInSeconds(value string) (int, error) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("parse expires_in %q: %w", value, err)
	}
	if parsed < 0 {
		return 0, errors.New("expires_in must be >= 0")
	}
	return int(parsed), nil
}

func signES256(privateKey *ecdsa.PrivateKey, digest []byte) ([]byte, error) {
	asn1Signature, err := ecdsa.SignASN1(rand.Reader, privateKey, digest)
	if err != nil {
		return nil, err
	}

	var signature struct {
		R, S *big.Int
	}
	rest, err := asn1.Unmarshal(asn1Signature, &signature)
	if err != nil {
		return nil, fmt.Errorf("decode ASN.1 ES256 signature: %w", err)
	}
	if len(rest) != 0 {
		return nil, errors.New("ASN.1 ES256 signature contains trailing data")
	}
	if signature.R == nil || signature.S == nil {
		return nil, errors.New("ASN.1 ES256 signature is missing coordinates")
	}

	size := privateKey.Curve.Params().BitSize / 8
	return packJOSEES256Signature(signature.R, signature.S, size), nil
}

func packJOSEES256Signature(r, s *big.Int, size int) []byte {
	signature := make([]byte, size*2)
	r.FillBytes(signature[:size])
	s.FillBytes(signature[size:])
	return signature
}
