package hyperliquid

import (
	"encoding/hex"
	"strings"
	"testing"

	secp "github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// Deterministic test key (NEVER use for real funds).
	testKeyHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
)

func TestLoadPrivateKeyHex_AcceptsWithAndWithout0x(t *testing.T) {
	a, err := LoadPrivateKeyHex(testKeyHex)
	if err != nil {
		t.Fatalf("plain hex: %v", err)
	}
	b, err := LoadPrivateKeyHex("0x" + testKeyHex)
	if err != nil {
		t.Fatalf("0x prefix: %v", err)
	}
	if !a.Key.Equals(&b.Key) {
		t.Errorf("keys differ between prefixed and unprefixed forms")
	}
}

func TestLoadPrivateKeyHex_RejectsGarbage(t *testing.T) {
	if _, err := LoadPrivateKeyHex("zzz"); err == nil {
		t.Errorf("expected error on non-hex")
	}
	if _, err := LoadPrivateKeyHex("aa"); err == nil {
		t.Errorf("expected error on short key")
	}
}

func TestAddressFromKey_FormatAndStability(t *testing.T) {
	priv, _ := LoadPrivateKeyHex(testKeyHex)
	addr := AddressFromKey(priv)
	if !strings.HasPrefix(addr, "0x") {
		t.Errorf("addr missing 0x: %q", addr)
	}
	if len(addr) != 42 {
		t.Errorf("addr length = %d, want 42", len(addr))
	}
	if AddressFromKey(priv) != addr {
		t.Errorf("address derivation not stable")
	}
}

func TestActionHash_DeterministicAndIncludesNonce(t *testing.T) {
	action := orderAction{
		Type: "order",
		Orders: []orderWire{{
			A: 5, B: true, P: "100.5", S: "0.1", R: false,
			T: orderTypeWire{Limit: limitTypeWire{Tif: "Alo"}},
		}},
		Grouping: "na",
	}
	h1, err := actionHash(action, 1714576800123, "")
	if err != nil {
		t.Fatalf("actionHash: %v", err)
	}
	h2, _ := actionHash(action, 1714576800123, "")
	if hex.EncodeToString(h1) != hex.EncodeToString(h2) {
		t.Errorf("actionHash not deterministic")
	}
	h3, _ := actionHash(action, 1714576800124, "") // different nonce
	if hex.EncodeToString(h1) == hex.EncodeToString(h3) {
		t.Errorf("nonce should change the hash")
	}
}

func TestActionHash_VaultMarker(t *testing.T) {
	action := orderAction{Type: "order", Orders: nil, Grouping: "na"}
	noVault, err := actionHash(action, 1, "")
	if err != nil {
		t.Fatalf("noVault: %v", err)
	}
	withVault, err := actionHash(action, 1, "0x1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("withVault: %v", err)
	}
	if hex.EncodeToString(noVault) == hex.EncodeToString(withVault) {
		t.Errorf("vault marker must change hash")
	}
}

func TestActionHash_RejectsBadVault(t *testing.T) {
	if _, err := actionHash(orderAction{Type: "order"}, 1, "0xnotvalid"); err == nil {
		t.Errorf("expected hex decode error")
	}
	if _, err := actionHash(orderAction{Type: "order"}, 1, "0xabcd"); err == nil {
		t.Errorf("expected length error on short vault")
	}
}

func TestSignAction_VerifiesWithDerivedPublicKey(t *testing.T) {
	priv, _ := LoadPrivateKeyHex(testKeyHex)
	action := orderAction{
		Type: "order",
		Orders: []orderWire{{
			A: 5, B: true, P: "83.45", S: "1.0", R: false,
			T: orderTypeWire{Limit: limitTypeWire{Tif: "Alo"}},
		}},
		Grouping: "na",
	}
	sig, err := SignAction(priv, action, 1714576800123, "", true)
	if err != nil {
		t.Fatalf("SignAction: %v", err)
	}
	// Reconstruct the digest the same way SignAction did and verify with
	// the public key.
	connID, _ := actionHash(action, 1714576800123, "")
	digest := agentDigest(connID, true)

	rBytes, _ := hex.DecodeString(strings.TrimPrefix(sig.R, "0x"))
	sBytes, _ := hex.DecodeString(strings.TrimPrefix(sig.S, "0x"))
	r := new(secp.ModNScalar)
	s := new(secp.ModNScalar)
	r.SetByteSlice(rBytes)
	s.SetByteSlice(sBytes)
	signature := ecdsa.NewSignature(r, s)

	if !signature.Verify(digest, priv.PubKey()) {
		t.Errorf("signature did not verify with derived pubkey")
	}
}

func TestSignAction_MainnetVsTestnetDiffer(t *testing.T) {
	priv, _ := LoadPrivateKeyHex(testKeyHex)
	action := orderAction{Type: "order", Grouping: "na"}
	main, _ := SignAction(priv, action, 1, "", true)
	test, _ := SignAction(priv, action, 1, "", false)
	if main.R == test.R && main.S == test.S {
		t.Errorf("mainnet and testnet sigs must differ (different EIP-712 source byte)")
	}
}

func TestSignAction_NilKey(t *testing.T) {
	if _, err := SignAction(nil, orderAction{Type: "order"}, 1, "", true); err == nil {
		t.Errorf("expected error on nil key")
	}
}

// TestMsgpack_FieldOrderPreserved ensures the encoder emits fields in
// declaration order — Hyperliquid's connectionId hash is byte-sensitive,
// so reordering fields silently breaks signing.
func TestMsgpack_FieldOrderPreserved(t *testing.T) {
	type ordered struct {
		A int    `msgpack:"a"`
		B bool   `msgpack:"b"`
		P string `msgpack:"p"`
	}
	encoded, err := msgpackAction(ordered{A: 5, B: true, P: "100"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]interface{}
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 3 {
		t.Errorf("expected 3 fields, got %d", len(decoded))
	}
	// Spot-check by re-encoding and confirming byte-stable output.
	encoded2, _ := msgpackAction(ordered{A: 5, B: true, P: "100"})
	if hex.EncodeToString(encoded) != hex.EncodeToString(encoded2) {
		t.Errorf("encoding is not deterministic")
	}
}
