package polymarket

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestSign_DeterministicAndVerifiable(t *testing.T) {
	// Round-trip: independently compute the expected HMAC, compare.
	rawSecret := []byte("super-secret-bytes-32-bytes-long")
	encoded := base64.URLEncoding.EncodeToString(rawSecret)

	got, err := Sign(encoded, "1714576800", "POST", "/orders", `{"size":"1"}`)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mac := hmac.New(sha256.New, rawSecret)
	mac.Write([]byte("1714576800" + "POST" + "/orders" + `{"size":"1"}`))
	want := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestSign_FallsBackToStdEncodingSecret(t *testing.T) {
	rawSecret := []byte("another-secret-32-bytes-padding!")
	encoded := base64.StdEncoding.EncodeToString(rawSecret)
	if _, err := Sign(encoded, "1", "GET", "/x", ""); err != nil {
		t.Fatalf("Sign should accept std-encoded secrets, got %v", err)
	}
}

func TestSign_EmptySecret(t *testing.T) {
	if _, err := Sign("", "1", "GET", "/x", ""); err == nil {
		t.Errorf("expected error on empty secret")
	}
}

func TestSign_RejectsGarbageSecret(t *testing.T) {
	if _, err := Sign("not-base64!!", "1", "GET", "/x", ""); err == nil {
		t.Errorf("expected error on undecodable secret")
	}
}
