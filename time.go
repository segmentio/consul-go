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
		i, _ := strconv.ParseInt(v, 10, 64)
		d = time.Duration(i)
	}
	return d
}
