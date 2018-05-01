package protocol

import (
	"net/url"
)

// Validate returns an error if the BrokerSpec_ID is not well-formed.
func (m *BrokerSpec_ID) Validate() error {
	if err := validateB64Str(m.Zone, minZoneLen, maxZoneLen); err != nil {
		return ExtendContext(err, "Zone")
	} else if err := validateB64Str(m.Suffix, minBrokerSuffixLen, maxBrokerSuffixLen); err != nil {
		return ExtendContext(err, "Suffix")
	}
	return nil
}

// Validate returns an error if the BrokerSpec is not well-formed.
func (m *BrokerSpec) Validate() error {
	if err := m.Id.Validate(); err != nil {
		return ExtendContext(err, "Id")
	} else if url, err := url.Parse(m.Endpoint); err != nil {
		return ExtendContext(&ValidationError{Err: err}, "Endpoint")
	} else if url.Scheme == "" || url.Host == "" {
		return NewValidationError("invalid Endpoint: %+v", m.Endpoint)
	} else if m.JournalLimit < 0 || m.JournalLimit > maxBrokerJournalLimit {
		return NewValidationError("invalid JournalLimit (%d; expected 0 <= JournalLimit <= %d)",
			m.JournalLimit, maxJournalReplication)
	}
	return nil
}

// v3_allocator.MemberValue implementation.
func (m *BrokerSpec) ItemLimit() int { return int(m.JournalLimit) }

const (
	minZoneLen            = 1
	maxZoneLen            = 16
	minBrokerSuffixLen    = 4
	maxBrokerSuffixLen    = 128
	maxBrokerJournalLimit = 1 << 18
)
