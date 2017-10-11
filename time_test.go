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

func TestFromSeconds(t *testing.T) {
	if d := fromSeconds("30s"); d != 30*time.Second {
		t.Error("duration should be 30 seconds:", d.Seconds())
	}

	if d := fromSeconds("15000000000"); d != 15*time.Second {
		t.Error("duration should be 15 seconds:", d.Seconds())
	}
}
