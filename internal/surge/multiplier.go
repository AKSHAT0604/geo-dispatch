package surge

// Step is one point in the ratio-to-multiplier step function: at Ratio and
// above, Multiplier applies, until a higher step's threshold is reached.
type Step struct {
	Ratio      float64
	Multiplier float64
}

// MultiplierConfig maps a supply/demand ratio to a surge multiplier via an
// ascending step function, clamped to [Min, Max]. Steps must be sorted
// ascending by Ratio, with Steps[0].Ratio == 0 as the baseline.
type MultiplierConfig struct {
	Steps    []Step
	Min, Max float64
}

// DefaultMultiplierConfig is a modest four-step curve clamped to 1.0x-3.0x:
// demand has to reach 1.5x supply before surge kicks in at all, and an 8x
// ratio hits the ceiling.
var DefaultMultiplierConfig = MultiplierConfig{
	Steps: []Step{
		{Ratio: 0, Multiplier: 1.0},
		{Ratio: 1.5, Multiplier: 1.2},
		{Ratio: 3, Multiplier: 1.75},
		{Ratio: 5, Multiplier: 2.5},
		{Ratio: 8, Multiplier: 3.0},
	},
	Min: 1.0,
	Max: 3.0,
}

// Multiplier returns the multiplier of the highest step whose threshold
// does not exceed ratio, clamped to [Min, Max]. A ratio of +Inf (demand
// with zero supply) hits the ceiling.
func (c MultiplierConfig) Multiplier(ratio float64) float64 {
	m := c.Min
	for _, s := range c.Steps {
		if ratio >= s.Ratio {
			m = s.Multiplier
		}
	}
	if m < c.Min {
		m = c.Min
	}
	if m > c.Max {
		m = c.Max
	}
	return m
}
