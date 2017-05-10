package consul

import (
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestShuffle(t *testing.T) {
	const N = 100

	list1 := make([]Endpoint, N)
	list2 := make([]Endpoint, N)

	for i := 0; i != N; i++ {
		list1[i].Node = strconv.Itoa(i)
	}

	copy(list2, list1)
	Shuffle(list2)

	if reflect.DeepEqual(list1, list2) {
		t.Error("the shuffled service list did not differ from the original")
	}
}

func TestWeightedShuffleOnRTT(t *testing.T) {
	const N = 100

	list1 := make([]Endpoint, N)
	list2 := make([]Endpoint, N)

	for i := 0; i != N; i++ {
		list1[i].Node = strconv.Itoa(i)
		list1[i].RTT = time.Duration(rand.Int63())
	}

	copy(list2, list1)
	WeightedShuffleOnRTT(list2)

	if reflect.DeepEqual(list1, list2) {
		t.Error("the shuffled service list did not differ from the original")
	}
}
