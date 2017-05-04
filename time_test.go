package consul

import (
	"testing"
	"time"
)

func TestSeconds(t *testing.T) {
	d := 1500 * time.Millisecond
	s := S(d)

	if s != "2s" {
		t.Error(s)
	}

	if d, err := s.Duration(); err != nil {
		t.Error(err)
	} else if d != (2 * time.Second) {
		t.Error(err)
	}
}
