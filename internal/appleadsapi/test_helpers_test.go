package appleadsapi_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/robaerd/asactl/internal/auth"
)

func testAuthConfig(t *testing.T) auth.Config {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}

	return auth.Config{
		ClientID:      "client-id",
		TeamID:        "team-id",
		KeyID:         "key-id",
		PrivateKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})),
	}
}
