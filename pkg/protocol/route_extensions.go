package protocol

import (
	"math/rand"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// Initialize Route with the provided allocator Assignments.
func (m *Route) Init(assignments keyspace.KeyValues) {
	*m = Route{Primary: -1, Brokers: m.Brokers[:0]}

	for _, kv := range assignments {
		var a = kv.Decoded.(v3_allocator.Assignment)
		if a.Slot == 0 {
			m.Primary = int32(len(m.Brokers))
		}

		m.Brokers = append(m.Brokers, BrokerSpec_ID{
			Zone:   a.MemberZone,
			Suffix: a.MemberSuffix,
		})
	}
}

// Copy returns a deep copy of the Route.
func (m Route) Copy() Route {
	return Route{
		Brokers:   append([]BrokerSpec_ID(nil), m.Brokers...),
		Primary:   m.Primary,
		Endpoints: append([]Endpoint(nil), m.Endpoints...),
	}
}

// AttachEndpoints maps Route members through the KeySpace to their respective
// BrokerSpecs, and attaches the associated Endpoint of each to the Route.
// KeySpace must already be read-locked.
func (m *Route) AttachEndpoints(ks *keyspace.KeySpace) {
	m.Endpoints = make([]Endpoint, len(m.Brokers))

	for i, b := range m.Brokers {
		if member, ok := v3_allocator.LookupMember(ks, b.Zone, b.Suffix); !ok {
			continue // Assignment with missing Member. Ignore.
		} else {
			m.Endpoints[i] = member.MemberValue.(*BrokerSpec).Endpoint
		}
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

	if l := len(m.Endpoints); l != 0 && l != len(m.Endpoints) {
		return NewValidationError("len(Endpoints) != 0, and != len(Brokers) (%d vs %d)",
			l, len(m.Brokers))
	}

	for i, ep := range m.Endpoints {
		if ep == "" {
			continue
		} else if err := ep.Validate(); err != nil {
			return ExtendContext(err, "Endpoints[%d]", i)
		}
	}
	return nil
}

// MarshalString returns the marshaled encoding of the JournalSpec as a string.
func (m Route) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// Replica returns a random BrokerSpec for read operations, preferring
// a broker in |zone|. It panics if the Route has no Brokers.
func (m Route) RandomReplica(zone string) BrokerSpec_ID {
	var p = rand.Perm(len(m.Brokers))

	for _, i := range p {
		if m.Brokers[i].Zone == zone {
			return m.Brokers[i]
		}
	}
	return m.Brokers[p[0]]
}

// Endpoint returns the endpoint of |id| within the Route, or an empty string
// if no endpoint is available.
/*
func (m Route) Endpoint(id BrokerSpec_ID) string {
	for i, b := range m.Brokers {
		if b == id {
			return m.Endpoints[i]
		}
	}
}
*/

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
		if m.Brokers[i] != other.Brokers[i] {
			return false
		}
	}
	return true
}
