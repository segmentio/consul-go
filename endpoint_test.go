package consul

import (
	"math/rand"
	"reflect"
	"sort"
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

func TestShuffleDistribution(t *testing.T) {
	t.Run("Shuffle", func(t *testing.T) { testShuffleDistribution(t, Shuffle) })
	t.Run("WeightedShuffleOnRTT", func(t *testing.T) { testShuffleDistribution(t, WeightedShuffleOnRTT) })
}

func testShuffleDistribution(t *testing.T, shuffle func([]Endpoint)) {
	const N = 1000

	type counter struct {
		index int
		value int
	}

	base := []Endpoint{
		{ID: "0", RTT: 250 * time.Microsecond},
		{ID: "1", RTT: 500 * time.Microsecond},
		{ID: "2", RTT: 1 * time.Millisecond},
		{ID: "3", RTT: 2 * time.Millisecond},
		{ID: "4", RTT: 4 * time.Millisecond},
		{ID: "5", RTT: 8 * time.Millisecond},
		{ID: "6", RTT: 16 * time.Millisecond},
		{ID: "7", RTT: 32 * time.Millisecond},
		{ID: "8", RTT: 64 * time.Millisecond},
		{ID: "9", RTT: 128 * time.Millisecond},
	}

	counters := make([]counter, len(base))
	for i := range counters {
		counters[i].index = i
	}

	endpoints := make([]Endpoint, len(base))
	for i := 0; i != N; i++ {
		copy(endpoints, base)
		shuffle(endpoints)

		for i := range base {
			if endpoints[0].ID == base[i].ID {
				counters[i].value++
				break
			}
		}
	}

	sort.Slice(counters, func(i int, j int) bool {
		return counters[i].value > counters[j].value
	})

	for _, c := range counters {
		endpoint := base[c.index]
		t.Logf("RTT = % 5s: % 3d\t(%g%%)", endpoint.RTT, c.value, float64(c.value)*100.0/N)
	}
}
