package observation

// RolloutMode is the immutable admission behavior frozen for one compiled
// Temporal run. It is deliberately separate from PolicyV1: the policy is
// user-approved task intent, while rollout is an operator-controlled release
// decision.
type RolloutMode string

const (
	RolloutOff       RolloutMode = "off"
	RolloutShadow    RolloutMode = "shadow"
	RolloutAuthority RolloutMode = "authority"
)

// Valid reports whether m is a supported durable rollout decision.
func (m RolloutMode) Valid() bool {
	switch m {
	case RolloutOff, RolloutShadow, RolloutAuthority:
		return true
	default:
		return false
	}
}
