package protocol

import (
	"math/rand"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// Extract queries the KeySpace for journal Assignments, initializes the Route,
// and attaches broker endpoints. KeySpace must already be read-locked.
func (m *Route) Extract(ks *keyspace.KeySpace, journal Journal) {
	var assignments = ks.KeyValues.Prefixed(
		v3_allocator.ItemAssignmentsPrefix(ks, journal.String()))

	m.Init(assignments)
	m.AttachEndpoints(ks)
}

// Initialize Route with the provided allocator Assignments.
func (m *Route) Init(assignments keyspace.KeyValues) {
	*m = Route{Primary: -1, Brokers: m.Brokers[:0]}

	for _, kv := range assignments {
		var a = kv.Decoded.(v3_allocator.Assignment)
		if a.Slot == 0 {
			m.Primary = int32(len(m.Brokers))
		}

		m.Brokers = append(m.Brokers, BrokerSpec{
			Id: BrokerSpec_ID{
				Zone:   a.MemberZone,
				Suffix: a.MemberSuffix,
			},
		})
	}
}

// AttachEndpoints maps Route members through the KeySpace to their respective
// BrokerSpecs, and attaches the associated Endpoint of each to the Route.
// KeySpace must already be read-locked.
func (m *Route) AttachEndpoints(ks *keyspace.KeySpace) {
	for i, b := range m.Brokers {
		var member, ok = v3_allocator.LookupMember(ks, b.Id.Zone, b.Id.Suffix)

		if !ok {
			continue // Assignment with missing Member. Ignore.
		}
		var s = member.MemberValue.(*BrokerSpec)
		m.Brokers[i].Endpoint = s.Endpoint
	}
}

// Validate returns an error if the Route is not well-formed.
func (m Route) Validate() error {
	for i, b := range m.Brokers {
		if err := b.Validate(); err != nil {
			return ExtendContext(err, "Replicas[%d]", i)
		}
	}
	if m.Primary < -1 || m.Primary >= int32(len(m.Brokers)) {
		return NewValidationError("invalid Primary (%+v; expected -1 <= Primary < %d)",
			m.Primary, len(m.Brokers))
	}
	return nil
}

// Replica returns a random BrokerSpec for read operations, preferring
// a broker in |zone|. It panics if the Route has no Brokers.
func (m Route) RandomReplica(zone string) BrokerSpec {
	var p = rand.Perm(len(m.Brokers))

	for _, i := range p {
		if m.Brokers[i].Id.Zone == zone {
			return m.Brokers[i]
		}
	}
	return m.Brokers[p[0]]
}

// Equivalent returns true if the Routes have equivalent broker Names, Zones,
// and current Primary. It does not compare broker Endpoints.
func (m Route) Equivalent(other *Route) bool {
	if other == nil {
		return false
	} else if m.Primary != other.Primary {
		return false
	} else if len(m.Brokers) != len(other.Brokers) {
		return false
	}
	for i := range m.Brokers {
		if m.Brokers[i].Id != other.Brokers[i].Id {
			return false
		}
	}
	return true
}
