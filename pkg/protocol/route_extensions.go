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
	if len(m.Brokers) != 0 {
		m.Endpoints = make([]Endpoint, len(m.Brokers))
	}
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
			return ExtendContext(err, "Brokers[%d]", i)
		}
		if i != 0 && !m.Brokers[i-1].Less(b) {
			return NewValidationError("Brokers not in unique, sorted order (index %d; %+v <= %+v)",
				i, m.Brokers[i-1], m.Brokers[i])
		}
	}

	if m.Primary < -1 || m.Primary >= int32(len(m.Brokers)) {
		return NewValidationError("invalid Primary (%+v; expected -1 <= Primary < %d)",
			m.Primary, len(m.Brokers))
	}

	if l := len(m.Endpoints); l != 0 && l != len(m.Brokers) {
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

// MarshalString returns the marshaled encoding of the JournalSpec as a string.
func (m Route) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// SelectReplica returns an index of |Brokers|, preferring:
//  * |id| if present in |Brokers|, falling back to
//  * A randomly selected broker sharing |id.Zone|, falling back to
//  * A randomly selected broker.
//  It panics if the Route has no Brokers.
func (m Route) SelectReplica(id BrokerSpec_ID) int {
	for i, b := range m.Brokers {
		if b == id {
			return i
		}
	}
	var p = rand.Perm(len(m.Brokers))

	for _, i := range p {
		if m.Brokers[i].Zone == id.Zone {
			return i
		}
	}
	return p[0]
}
