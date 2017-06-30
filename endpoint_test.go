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
	const M = 300
	const N = 2000

	type counter struct {
		index int
		value int
	}

	base := make([]Endpoint, M)
	rtt := 200 * time.Microsecond

	for i := 0; i != M; i++ {
		base[i].ID = strconv.Itoa(i)
		base[i].RTT = rtt
		rtt += 10 * time.Microsecond

		if i > (M / 2) {
			rtt += time.Millisecond
		}
	}

	counters := make([]counter, M)
	for i := range counters {
		counters[i].index = i
	}

	endpoints := make([]Endpoint, M)
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
		t.Logf("ID = %  s, RTT = % 5s: % 3d\t(%g%%)", endpoint.ID, endpoint.RTT, c.value, float64(c.value)*100.0/N)
	}
}
