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
