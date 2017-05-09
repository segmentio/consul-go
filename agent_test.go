package consul

import (
	"context"
	"testing"
)

func TestAgent(t *testing.T) {
	t.Run("ensure the node name of the consul agent is not and empty string", func(t *testing.T) {
		node, err := DefaultAgent.NodeName(context.Background())

		if err != nil {
			t.Error(err)
		}

		if len(node) == 0 {
			t.Error("bad empty node name returned for the consul agent")
		}
	})
}
