package hyperliquid

// Action types are msgpack-encoded as maps with string keys. Field order
// here matches the Hyperliquid Python SDK exactly — the wire format is
// position-sensitive because the connectionId hash includes the encoded
// bytes verbatim.

// orderAction is the body of a {"type":"order"} request.
type orderAction struct {
	Type     string      `msgpack:"type"`
	Orders   []orderWire `msgpack:"orders"`
	Grouping string      `msgpack:"grouping"`
}

// orderWire mirrors the SDK's order_wire dict. Fields:
//
//	a — asset index (int)
//	b — is_buy
//	p — price (string)
//	s — size  (string)
//	r — reduce_only
//	t — order type wrapper
type orderWire struct {
	A int            `msgpack:"a"`
	B bool           `msgpack:"b"`
	P string         `msgpack:"p"`
	S string         `msgpack:"s"`
	R bool           `msgpack:"r"`
	T orderTypeWire  `msgpack:"t"`
}

type orderTypeWire struct {
	Limit limitTypeWire `msgpack:"limit"`
}

type limitTypeWire struct {
	Tif string `msgpack:"tif"` // "Alo" | "Gtc" | "Ioc"
}

// cancelAction cancels by venue order ID.
type cancelAction struct {
	Type    string       `msgpack:"type"`
	Cancels []cancelWire `msgpack:"cancels"`
}

type cancelWire struct {
	A int   `msgpack:"a"` // asset index
	O int64 `msgpack:"o"` // order id
}
