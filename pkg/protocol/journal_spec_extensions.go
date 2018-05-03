package protocol

import (
	"path"
	"time"

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
	} else if err = m.Labels.Validate(); err != nil {
		return ExtendContext(err, "Labels")
	} else if err = m.Fragment.Validate(); err != nil {
		return ExtendContext(err, "Fragment")
	} else if m.TransactionLength < minTxnLen || m.TransactionLength > maxTxnLen {
		return NewValidationError("invalid TransactionLength (%s; expected %s <= size <= %s)",
			m.TransactionLength, minTxnLen, maxTxnLen)
	}

	// m.ReadOnly has no checks.
	return nil
}

// Validate returns an error if the JournalSpec_Fragment is not well-formed.
func (m *JournalSpec_Fragment) Validate() error {
	if m.Length < minFragmentLen || m.Length > maxFragmentLen {
		return NewValidationError("invalid Length (%s; expected %s <= length <= %s)",
			m.Length, minFragmentLen, maxFragmentLen)
	} else if err := m.CompressionCodec.Validate(); err != nil {
		return ExtendContext(err, "CompressionCodec")
	} else if m.RefreshInterval < minRefreshInterval || m.RefreshInterval > maxRefreshInterval {
		return NewValidationError("invalid RefreshInterval (%s; expected %s <= interval <= %s)",
			m.RefreshInterval, minRefreshInterval, maxRefreshInterval)
	} else if m.Retention < 0 {
		return NewValidationError("invalid Retention (%s; expected >= 0)", m.Retention)
	}
	for i, store := range m.Stores {
		if err := store.Validate(); err != nil {
			return ExtendContext(err, "Store[%d]", i)
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
	minJournalNameLen, maxJournalNameLen   = 4, 512
	maxJournalReplication                  = 5
	minRefreshInterval, maxRefreshInterval = time.Second, time.Hour * 24
	minFragmentLen, maxFragmentLen         = 1 << 10, 1 << 34 // 1024 => 17,179,869,184
	minTxnLen, maxTxnLen                   = 1 << 10, 1 << 30 // 1,024 => 1,073,741,824
)
