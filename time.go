package consul

import (
	"math"
	"strconv"
	"time"
)

// The Seconds type is a helper for generating valid representations of
// durations because Consul only supports seconds precisions (represented by a
// number of seconds an a "s" value).
type Seconds string

// S converts a time.Duration to its Seconds representation.
//
// The duration is rounded to the closest second greater than its value.
func S(d time.Duration) Seconds {
	return Seconds(strconv.FormatInt(int64(math.Ceil(d.Seconds())), 10) + "s")
}

// Duration converts a Seconds back to a time.Duration representation.
func (s Seconds) Duration() (d time.Duration, err error) {
	return time.ParseDuration(string(s))
}
