// Package polymarket implements the perps client for Polymarket. As of
// May 1 2026, the perps API is not yet publicly documented — only the
// existing CLOB (binary options) API is. This package provides:
//
//   - The auth scheme (HMAC-SHA256, identical to the CLOB API and
//     expected to extend to perps).
//   - A client scaffold with the exchange.Exchange interface implemented
//     as TODO stubs.
//
// When perps docs publish, fill in: SubscribeBook (WS channel name),
// PlaceOrder (POST path + body shape), GetPosition (GET path),
// GetFundingRate (GET path), and friends. The auth/transport layer
// stays.
package polymarket

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// Sign produces the POLY-SIGNATURE header value for one request.
//
// Pre-image is the concatenation timestamp+method+requestPath+body, signed
// with HMAC-SHA256 using the base64-decoded secret. The result is
// base64url-encoded (the CLOB convention).
//
// The caller supplies timestamp as a string (seconds since epoch).
// Method is upper-case ("GET", "POST"). RequestPath is the URL path
// including any query string. Body is the raw request body, or "" for
// empty.
func Sign(secret, timestamp, method, requestPath, body string) (string, error) {
	if secret == "" {
		return "", fmt.Errorf("polymarket: empty secret")
	}
	secretBytes, err := base64.URLEncoding.DecodeString(secret)
	if err != nil {
		// Some Polymarket key dumps use std encoding — try that as fallback.
		secretBytes, err = base64.StdEncoding.DecodeString(secret)
		if err != nil {
			return "", fmt.Errorf("polymarket: decode secret: %w", err)
		}
	}
	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte(timestamp + method + requestPath + body))
	return base64.URLEncoding.EncodeToString(mac.Sum(nil)), nil
}
