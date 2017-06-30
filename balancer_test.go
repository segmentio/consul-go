package consul

import (
	"sort"
	"testing"
)

var balancers = []struct {
	name string
	impl Balancer
}{
	{
		name: "RoundRobin",
		impl: &RoundRobin{},
	},

	{
		name: "PreferTags",
		impl: PreferTags{"us-west-2a"},
	},

	{
		name: "PreferTags+RoundRobin",
		impl: MultiBalancer(
			PreferTags{"us-west-2a"},
			&RoundRobin{},
		),
	},
}

func TestBalancer(t *testing.T) {
	for _, balancer := range balancers {
		t.Run(balancer.name, func(t *testing.T) {
			testBalancer(t, balancer.impl)
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
		endpoints = balancer.Balance("test-service", endpoints)

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
		t.Logf("ID = %  s, RTT = % 5s: % 3d\t(%g%%)", endpoint.ID, endpoint.RTT, c.value, float64(c.value)*100.0/draws)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for _, balancer := range balancers {
		b.Run(balancer.name, func(b *testing.B) {
			benchmarkBalancer(b, balancer.impl)
		})
	}
}

func benchmarkBalancer(b *testing.B, balancer Balancer) {
	endpoints := generateTestEndpoints(300)

	for i := 0; i != b.N; i++ {
		balancer.Balance("service-A", endpoints)
	}
}
