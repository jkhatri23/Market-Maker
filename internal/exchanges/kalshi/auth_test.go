package kalshi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/pem"
	"crypto/x509"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestSign_VerifiesWithMatchingPublicKey(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	const ts int64 = 1714576800123
	method, path := "GET", "/portfolio/positions"
	got, err := Sign(priv, ts, method, path)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	sig, err := base64.StdEncoding.DecodeString(got)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}

	pre := strconv.FormatInt(ts, 10) + method + path
	h := sha256.Sum256([]byte(pre))
	if err := rsa.VerifyPSS(&priv.PublicKey, crypto.SHA256, h[:], sig, &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
	}); err != nil {
		t.Errorf("signature did not verify: %v", err)
	}
}

func TestSign_NilKey(t *testing.T) {
	if _, err := Sign(nil, 1, "GET", "/x"); err == nil {
		t.Errorf("expected error with nil key")
	}
}

func TestLoadPrivateKey_PKCS8(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("MarshalPKCS8: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	path := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loaded, err := LoadPrivateKey(path)
	if err != nil {
		t.Fatalf("LoadPrivateKey: %v", err)
	}
	if loaded.N.Cmp(priv.N) != 0 {
		t.Errorf("loaded modulus does not match original")
	}
}

func TestLoadPrivateKey_PKCS1(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	der := x509.MarshalPKCS1PrivateKey(priv)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})

	path := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := LoadPrivateKey(path); err != nil {
		t.Errorf("LoadPrivateKey PKCS1: %v", err)
	}
}

func TestLoadPrivateKey_Garbage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key.pem")
	_ = os.WriteFile(path, []byte("not a pem file"), 0o600)
	if _, err := LoadPrivateKey(path); err == nil {
		t.Errorf("expected error on non-PEM input")
	}
}
