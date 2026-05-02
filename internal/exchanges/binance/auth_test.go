package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestSign_RoundTripsAgainstStdlib(t *testing.T) {
	const (
		secret  = "abcdef0123456789"
		payload = "symbol=SOLUSDT&side=BUY&type=LIMIT&timestamp=1714576800000"
	)
	got, err := Sign(secret, payload)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	want := hex.EncodeToString(mac.Sum(nil))
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestSign_EmptySecret(t *testing.T) {
	if _, err := Sign("", "x=1"); err == nil {
		t.Errorf("expected error on empty secret")
	}
}
