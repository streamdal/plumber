package plumber

import "time"

// Modified from https://blog.gopheracademy.com/advent-2014/backoff/

type BackoffPolicy struct {
	Seconds []time.Duration
}

var ForemanReconnectPolicy = BackoffPolicy{
	[]time.Duration{
		3 * time.Second,
		10 * time.Second,
		30 * time.Second,
		60 * time.Second,
		5 * time.Minute,
	},
}

func (b BackoffPolicy) Duration(n int) time.Duration {
	if n >= len(b.Seconds) {
		n = len(b.Seconds) - 1
	}

	return b.Seconds[n]
}
