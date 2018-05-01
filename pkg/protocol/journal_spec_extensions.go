package protocol

import (
	"net/url"
	"path"

	"github.com/LiveRamp/gazette/pkg/keyspace"
)

// Journal uniquely identifies a journal brokered by Gazette.
// By convention, journals are named using a forward-slash notation which
// captures their hierarchical relationships into organizations, topics and
// partitions. For example, a Journal might be:
// "company-journals/interesting-topic/part-1234"
type Journal string

// Validate returns an error if the Journal is not well-formed.
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

// v3_allocator.ItemValue implementation.
func (m *JournalSpec) DesiredReplication() int { return int(m.Replication) }

// IsConsistent returns true if all |assignments| agree on the current Route.
func (m *JournalSpec) IsConsistent(_ keyspace.KeyValue, assignments keyspace.KeyValues) bool {
	// TODO(johnny).
	return true
}

const (
	minJournalNameLen     = 4
	maxJournalNameLen     = 512
	maxJournalReplication = 5
)
