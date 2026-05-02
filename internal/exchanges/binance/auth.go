// Package binance implements exchange.Exchange against Binance USD-M
// Futures (fapi.binance.com). Used as the hedge venue: market-order out
// the position the maker venue just opened.
//
// Authentication is HMAC-SHA256 over the request query string with the
// API secret, hex-encoded. X-MBX-APIKEY carries the public key.
package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// Sign returns the hex HMAC-SHA256 of payload with secret. Binance
// signed endpoints append `&signature=<this>` to the query string.
func Sign(secret, payload string) (string, error) {
	if secret == "" {
		return "", errors.New("binance: empty api secret")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil)), nil
}
