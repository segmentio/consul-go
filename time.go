package consul

import (
	"math"
	"strconv"
	"time"
)

// Converts a time.Duration to its seconds representation.
func seconds(d time.Duration) string {
	return strconv.FormatInt(int64(math.Ceil(d.Seconds())), 10) + "s"
}

// Converts a duration string representation in seconds to a time.Duration.
func fromSeconds(v string) time.Duration {
	d, err := time.ParseDuration(v)
	if err != nil {
		f, _ := strconv.ParseFloat(v, 64)
		d = time.Duration(f)
	}
	return d
}
