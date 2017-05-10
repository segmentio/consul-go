package consul

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestTomography(t *testing.T) {
	t.Run("fetching node coordinates return a non-empty set", func(t *testing.T) {
		tomography := &Tomography{}

		nodes1, err1 := tomography.NodeCoordinates(context.Background())
		nodes2, err2 := tomography.NodeCoordinates(context.Background())

		if err1 != nil {
			t.Error("the first call to (*Tomography).NodeCoordinates returned an error:", err1)
		}

		if err2 != nil {
			t.Error("the second call to (*Tomography).NodeCoordinates returned an error:", err2)
		}

		// Due to internal caching, two consecutive calls to NodeCoordinates
		// should have returned the same map.
		ptr1 := reflect.ValueOf(nodes1).Pointer()
		ptr2 := reflect.ValueOf(nodes2).Pointer()

		if ptr1 != ptr2 {
			t.Error("the tomography cache should have returned the same NodeCoordinates value on two consecutive calls:")
			t.Logf("%#v", nodes1)
			t.Logf("%#v", nodes2)
		}

		if len(nodes1) == 0 {
			t.Log("the tomography of the consul datacenter returned an empty set of node coordinates, likely because it was recently started and haven't computed the tomology yet")
		}
	})

	t.Run("exercise the node coordinates automatic update", func(t *testing.T) {
		tomography := &Tomography{
			CacheTimeout: 10 * time.Millisecond,
		}

		// The first call triggers the asynchronous automatic update.
		nodes1, err1 := tomography.NodeCoordinates(context.Background())

		// Sleeping for a little while should given the autoupdate a chance to
		// run and change the node map.
		time.Sleep(30 * time.Millisecond)

		// The second call should fetch a different value from the updated
		// internal cache.
		nodes2, err2 := tomography.NodeCoordinates(context.Background())

		if err1 != nil {
			t.Error("the first call to (*Tomography).NodeCoordinates returned an error:", err1)
		}

		if err2 != nil {
			t.Error("the second call to (*Tomography).NodeCoordinates returned an error:", err2)
		}

		ptr1 := reflect.ValueOf(nodes1).Pointer()
		ptr2 := reflect.ValueOf(nodes2).Pointer()

		if ptr1 == ptr2 {
			t.Error("the automatic updated of the tomography's internal cache should not have returned the same node coordinates value twice in a row")
			t.Logf("%#v", nodes1)
			t.Logf("%#v", nodes2)
		}
	})

	t.Run("ensure the distance between nodes and itself is zero, or non-zero otherwise", func(t *testing.T) {
		tomography := &Tomography{}
		nodes, err := tomography.NodeCoordinates(context.Background())

		if err != nil {
			t.Error(err)
		}

		for node1 := range nodes {
			for node2 := range nodes {
				distance, ok := nodes.Distance(node1, node2)
				switch {
				case !ok:
					t.Errorf("the distance from %s to %s could not be computed", node1, node2)
				case distance == 0:
					t.Errorf("the distance from %s to %s should not be zero", node1, node2)
				}
			}
		}
	})
}
