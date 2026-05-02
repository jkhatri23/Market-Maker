package risk

// SkewParams is the input to Avellaneda-Stoikov mid-shift.
type SkewParams struct {
	NetPosition float64 // signed; positive = long
	MaxPosition float64 // absolute limit
	SpreadBps   float64 // base spread in bps
	SkewFactor  float64 // 0..1 typical; 0 disables, 1 = full-width shift at max position
}

// Skew returns mid-shift in bps.
//
// Sign: when long (NetPosition > 0), shift is NEGATIVE — both bid and ask
// move down, making sells fill faster and buys fill slower. Mean-reverts
// the position toward zero. Symmetric for short.
//
// Magnitude: capped so |shift| ≤ SkewFactor × SpreadBps even if position
// somehow exceeds MaxPosition.
func Skew(p SkewParams) float64 {
	if p.MaxPosition <= 0 || p.SpreadBps <= 0 || p.SkewFactor <= 0 {
		return 0
	}
	ratio := p.NetPosition / p.MaxPosition
	if ratio > 1 {
		ratio = 1
	} else if ratio < -1 {
		ratio = -1
	}
	return -p.SkewFactor * ratio * p.SpreadBps
}
