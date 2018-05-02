package protocol

import (
	"net/url"
	"path"

	"github.com/LiveRamp/gazette/pkg/keyspace"
	"github.com/LiveRamp/gazette/pkg/v3.allocator"
)

// Journal uniquely identifies a journal brokered by Gazette.
// By convention, journals are named using a forward-slash notation which
// captures their hierarchical relationships into organizations, topics and
// partitions. For example, a Journal might be:
// "company-journals/interesting-topic/part-1234"
type Journal string

// Validate returns an error if the Journal is not well-formed. It must be of
// the base64 alphabet, a clean path (as defined by path.Clean), and must not
// begin with a '/'.
func (n Journal) Validate() error {
	if err := validateB64Str(n.String(), minJournalNameLen, maxJournalNameLen); err != nil {
		return err
	} else if path.Clean(n.String()) != n.String() {
		return NewValidationError("must be a clean path: %+v", n)
	} else if n[0] == '/' {
		return NewValidationError("cannot begin with '/'")
	}
	return nil
}

// String returns the Journal as a string.
func (n Journal) String() string { return string(n) }

// Validate returns an error if the JournalSpec is not well-formed.
func (m *JournalSpec) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return ExtendContext(err, "Name")
	} else if m.Replication < 1 || m.Replication > maxJournalReplication {
		return NewValidationError("invalid Replication (%d; expected 1 <= Replication <= %d)",
			m.Replication, maxJournalReplication)
	} else if m.FragmentRetention < 0 {
		return NewValidationError("invalid FragmentRetention (%s; expected >= 0)", m.FragmentRetention)
	} else if err = m.CompressionCodec.Validate(); err != nil {
		return ExtendContext(err, "CompressionCodec")
	} else if err = m.Labels.Validate(); err != nil {
		return ExtendContext(err, "Labels")
	}

	for _, store := range m.FragmentStores {
		if url, err := url.Parse(store); err != nil {
			return ExtendContext(&ValidationError{Err: err}, "FragmentStore")
		} else if url.Scheme == "" {
			return NewValidationError("FragmentStore missing Scheme")
		}
	}
	return nil
}

// MarshalString returns the marshaled encoding of the JournalSpec as a string.
func (m *JournalSpec) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// v3_allocator.ItemValue implementation.
func (m *JournalSpec) DesiredReplication() int { return int(m.Replication) }

// IsConsistent returns true if all |assignments| agree on the current Route.
// |assignments| must not be empty or IsConsistent panics.
func (m *JournalSpec) IsConsistent(_ keyspace.KeyValue, assignments keyspace.KeyValues) bool {
	var r1 = assignments[0].Decoded.(v3_allocator.Assignment).AssignmentValue.(*Route)

	for _, a := range assignments[1:] {
		var r2 = a.Decoded.(v3_allocator.Assignment).AssignmentValue.(*Route)
		if !r1.Equivalent(r2) {
			return false
		}
	}
	return true
}

const (
	minJournalNameLen     = 4
	maxJournalNameLen     = 512
	maxJournalReplication = 5
)
