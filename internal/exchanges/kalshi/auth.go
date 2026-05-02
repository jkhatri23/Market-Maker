// Package kalshi implements the perps client for Kalshi "Timeless".
// Timeless launched April 27 2026; the API surface is still being
// documented. This package provides:
//
//   - The auth scheme (RSA-PSS-SHA256, identical to the existing Kalshi
//     binary API and reused for Timeless).
//   - A client scaffold with the exchange.Exchange interface implemented
//     as TODO stubs.
//
// When Timeless docs publish, fill in the WS channel name + message
// format, the order placement path/body, position/funding endpoints.
// The auth/transport layer stays.
package kalshi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Sign produces the KALSHI-ACCESS-SIGNATURE header value.
//
// Pre-image: timestampMS + method + path. Signed with RSA-PSS using
// SHA-256, salt length equal to hash length (Kalshi's documented choice).
// Result is base64-encoded (std encoding).
//
// Method should be upper-case ("GET","POST"). Path is the URL path only
// (no query string in the signed pre-image; if the API evolves to require
// it, expose a separate signWithQuery helper).
func Sign(priv *rsa.PrivateKey, timestampMS int64, method, path string) (string, error) {
	if priv == nil {
		return "", errors.New("kalshi: nil private key")
	}
	pre := strconv.FormatInt(timestampMS, 10) + method + path
	h := sha256.Sum256([]byte(pre))
	sig, err := rsa.SignPSS(rand.Reader, priv, crypto.SHA256, h[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
	})
	if err != nil {
		return "", fmt.Errorf("kalshi: sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// LoadPrivateKey reads a PEM-encoded RSA private key from disk. Accepts
// PKCS#8 and PKCS#1 PEM blocks (Kalshi key downloads have used both
// formats over time).
func LoadPrivateKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("kalshi: not a PEM file")
	}

	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rsaKey, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("kalshi: PKCS8 key is %T, want RSA", k)
		}
		return rsaKey, nil
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	return nil, errors.New("kalshi: failed to parse private key (tried PKCS8 and PKCS1)")
}
