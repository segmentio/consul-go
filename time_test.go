package consul

import (
	"testing"
	"time"
)

func TestSeconds(t *testing.T) {
	d := 1500 * time.Millisecond
	s := seconds(d)

	if s != "2s" {
		t.Error(s)
	}
}
