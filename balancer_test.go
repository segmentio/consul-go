package consul

import (
	"sort"
	"testing"
)

var balancers = []struct {
	name string
	new  func() Balancer
}{
	{
		name: "NullBalancer",
		new:  func() Balancer { return &NullBalancer{} },
	},

	{
		name: "RoundRobin",
		new:  func() Balancer { return &RoundRobin{} },
	},

	{
		name: "PreferTags(us-west-2a)",
		new:  func() Balancer { return PreferTags{"us-west-2a"} },
	},

	{
		name: "PreferTags(us-west-2b)",
		new:  func() Balancer { return PreferTags{"us-west-2b"} },
	},

	{
		name: "PreferTags(us-west-2a)+RoundRobin",
		new: func() Balancer {
			return MultiBalancer(PreferTags{"us-west-2a"}, &RoundRobin{})
		},
	},

	{
		name: "PreferTags(us-west-2b)+RoundRobin",
		new: func() Balancer {
			return MultiBalancer(PreferTags{"us-west-2b"}, &RoundRobin{})
		},
	},

	{
		name: "Shuffler",
		new:  func() Balancer { return &Shuffler{} },
	},

	{
		name: "WeightedShufflerOnRTT",
		new:  func() Balancer { return &WeightedShuffler{WeightOf: WeightRTT} },
	},

	{
		name: "LoadBlancer+RoundRobin",
		new:  func() Balancer { return &LoadBalancer{New: func() Balancer { return &RoundRobin{} }} },
	},
}

func TestBalancer(t *testing.T) {
	for _, balancer := range balancers {
		t.Run(balancer.name, func(t *testing.T) {
			testBalancer(t, balancer.new())
		})
	}
}

func testBalancer(t *testing.T, balancer Balancer) {
	const endpointsCount = 30
	const draws = 200

	type counter struct {
		index int
		value int
	}

	base := generateTestEndpoints(endpointsCount)
	counters := make([]counter, endpointsCount)
	for i := range counters {
		counters[i].index = i
	}

	endpoints := make([]Endpoint, endpointsCount)
	for i := 0; i != draws; i++ {
		copy(endpoints, base)
		balanced := balancer.Balance("test-service", endpoints)

		for i := range base {
			if balanced[0].ID == base[i].ID {
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
		t.Logf("ID = %  s, RTT = % 5s, Tags = %s: % 3d\t(%g%%)", endpoint.ID, endpoint.RTT, endpoint.Tags, c.value, float64(c.value)*100.0/draws)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for _, balancer := range balancers {
		b.Run(balancer.name, func(b *testing.B) {
			benchmarkBalancer(b, balancer.new())
		})
	}
}

func benchmarkBalancer(b *testing.B, balancer Balancer) {
	endpoints := generateTestEndpoints(300)

	for i := 0; i != b.N; i++ {
		balancer.Balance("service-A", endpoints)
	}
}
