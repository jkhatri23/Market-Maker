// Package hyperliquid implements exchange.Exchange against the
// Hyperliquid L1 perp DEX (api.hyperliquid.xyz).
//
// Authenticated requests use EIP-712 typed-data signatures over a
// "phantom agent" struct whose connectionId hashes the msgpack-encoded
// action + nonce + vault marker. The signing scheme matches the official
// hyperliquid-python-sdk; see action_hash + sign_l1_action.
package hyperliquid

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	secp "github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/crypto/sha3"
)

// Signature is the hex-encoded ECDSA signature Hyperliquid expects.
type Signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V int    `json:"v"`
}

// SignAction produces the signature for an L1 exchange action.
//
//	action  — the typed action struct (Order, Cancel, …); msgpack-encoded
//	          per Hyperliquid spec (struct field order matters).
//	nonce   — millisecond timestamp; must monotonically increase per key.
//	vault   — optional vault address (hex with 0x). Empty for direct accounts.
//	mainnet — true for api.hyperliquid.xyz, false for testnet.
func SignAction(priv *secp.PrivateKey, action interface{}, nonce int64, vault string, mainnet bool) (Signature, error) {
	if priv == nil {
		return Signature{}, errors.New("hyperliquid: nil private key")
	}
	connID, err := actionHash(action, nonce, vault)
	if err != nil {
		return Signature{}, err
	}
	digest := agentDigest(connID, mainnet)
	return signDigest(priv, digest)
}

// actionHash = keccak256( msgpack(action) || nonce_be8 || vault_marker )
//
// vault_marker = 0x00                          (no vault)
//              | 0x01 || 20 bytes address      (vault)
func actionHash(action interface{}, nonce int64, vault string) ([]byte, error) {
	encoded, err := msgpackAction(action)
	if err != nil {
		return nil, fmt.Errorf("encode action: %w", err)
	}
	buf := make([]byte, 0, len(encoded)+8+21)
	buf = append(buf, encoded...)
	nonceBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nonceBytes, uint64(nonce))
	buf = append(buf, nonceBytes...)
	if vault == "" {
		buf = append(buf, 0x00)
	} else {
		raw, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(vault), "0x"))
		if err != nil {
			return nil, fmt.Errorf("decode vault: %w", err)
		}
		if len(raw) != 20 {
			return nil, fmt.Errorf("vault must be 20 bytes, got %d", len(raw))
		}
		buf = append(buf, 0x01)
		buf = append(buf, raw...)
	}
	return keccak256(buf), nil
}

// agentDigest returns the EIP-712 digest for a Hyperliquid Agent struct.
//
//	domain = Exchange / 1 / chainId 1337 / 0x0
//	type   = Agent(string source, bytes32 connectionId)
//	source = "a" (mainnet) | "b" (testnet)
func agentDigest(connectionID []byte, mainnet bool) []byte {
	source := "b"
	if mainnet {
		source = "a"
	}
	domainTypeHash := keccak256([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	nameHash := keccak256([]byte("Exchange"))
	versionHash := keccak256([]byte("1"))
	chainID := pad32(big.NewInt(1337).Bytes())
	verifyingContract := make([]byte, 32) // zero address padded
	domainSep := keccak256(concat(domainTypeHash, nameHash, versionHash, chainID, verifyingContract))

	agentTypeHash := keccak256([]byte("Agent(string source,bytes32 connectionId)"))
	sourceHash := keccak256([]byte(source))
	structHash := keccak256(concat(agentTypeHash, sourceHash, connectionID))

	return keccak256(concat([]byte{0x19, 0x01}, domainSep, structHash))
}

func signDigest(priv *secp.PrivateKey, digest []byte) (Signature, error) {
	if len(digest) != 32 {
		return Signature{}, fmt.Errorf("digest must be 32 bytes, got %d", len(digest))
	}
	// SignCompact returns [recoveryByte || R(32) || S(32)] = 65 bytes.
	// With isCompressedKey=false the recovery byte is already 27 or 28 —
	// matching Hyperliquid's V convention.
	sigCompact := ecdsa.SignCompact(priv, digest, false)
	if len(sigCompact) != 65 {
		return Signature{}, fmt.Errorf("compact sig len = %d, want 65", len(sigCompact))
	}
	return Signature{
		R: "0x" + hex.EncodeToString(sigCompact[1:33]),
		S: "0x" + hex.EncodeToString(sigCompact[33:65]),
		V: int(sigCompact[0]),
	}, nil
}

// LoadPrivateKeyHex parses a hex-encoded secp256k1 private key (with or
// without 0x prefix). 64 hex chars → 32-byte scalar.
func LoadPrivateKeyHex(s string) (*secp.PrivateKey, error) {
	s = strings.TrimPrefix(strings.TrimSpace(strings.ToLower(s)), "0x")
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode hex key: %w", err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(raw))
	}
	return secp.PrivKeyFromBytes(raw), nil
}

// AddressFromKey derives the Ethereum-style address (last 20 bytes of
// keccak256 of the uncompressed public key without the 0x04 prefix).
func AddressFromKey(priv *secp.PrivateKey) string {
	pub := priv.PubKey().SerializeUncompressed()[1:] // strip 0x04 prefix
	h := keccak256(pub)
	return "0x" + hex.EncodeToString(h[12:])
}

// msgpackAction marshals an action via vmihailenco/msgpack v5. v5
// preserves struct field declaration order, which Hyperliquid requires.
func msgpackAction(action interface{}) ([]byte, error) {
	var b []byte
	enc := msgpack.NewEncoder(newAppendWriter(&b))
	enc.SetSortMapKeys(false)
	enc.UseCompactInts(false)
	enc.UseCompactFloats(false)
	if err := enc.Encode(action); err != nil {
		return nil, err
	}
	return b, nil
}

func keccak256(data ...[]byte) []byte {
	h := sha3.NewLegacyKeccak256()
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum(nil)
}

func concat(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func pad32(b []byte) []byte {
	if len(b) >= 32 {
		return b[:32]
	}
	out := make([]byte, 32)
	copy(out[32-len(b):], b)
	return out
}

// appendWriter satisfies io.Writer by appending to a byte slice.
type appendWriter struct{ p *[]byte }

func newAppendWriter(p *[]byte) *appendWriter { return &appendWriter{p: p} }
func (w *appendWriter) Write(b []byte) (int, error) {
	*w.p = append(*w.p, b...)
	return len(b), nil
}
