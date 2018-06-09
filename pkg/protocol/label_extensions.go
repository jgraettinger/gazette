package protocol

import (
	"strings"
)

// Validate returns an error if the Label is not well-formed.
func (m Label) Validate() error {
	if err := validateB64Str(m.Name, minLabelLen, maxLabelLen); err != nil {
		return ExtendContext(err, "Name")
	} else if err = validateB64Str(m.Value, 0, maxLabelValueLen); err != nil {
		return ExtendContext(err, "Value")
	}
	return nil
}

// Validate returns an error if the LabelSet is not well-formed.
func (m LabelSet) Validate() error {
	for i := range m.Labels {
		if err := m.Labels[i].Validate(); err != nil {
			return ExtendContext(err, "Labels[%d]", i)
		}
		if i != 0 && m.Labels[i].Name <= m.Labels[i-1].Name {
			return NewValidationError("Labels not in unique, sorted order (index %d; %+v <= %+v)",
				i, m.Labels[i].Name, m.Labels[i-1].Name)
		}
	}
	return nil
}

// Validate returns an error if the LabelSelector is not well-formed.
func (m LabelSelector) Validate() error {
	if err := m.Include.Validate(); err != nil {
		return ExtendContext(err, "Include")
	} else if err := m.Exclude.Validate(); err != nil {
		return ExtendContext(err, "Exclude")
	}
	return nil
}

// Matches returns whether the LabelSet is matched by the LabelSelector.
func (m LabelSelector) Matches(s LabelSet) bool {
	if matchLabels(m.Exclude.Labels, s.Labels) {
		return false
	} else if len(m.Include.Labels) != 0 && !matchLabels(m.Include.Labels, s.Labels) {
		return false
	}
	return true
}

func matchLabels(s, o []Label) bool {
	for len(s) != 0 && len(o) != 0 {
		if s[0].Name < o[0].Name {
			s = s[1:]
		} else if s[0].Name > o[0].Name {
			o = o[1:]
		} else if s[0].Value == o[0].Value || s[0].Value == "" {
			return true
		} else {
			s, o = s[1:], o[1:]
		}
	}
	return false
}

func validateB64Str(n string, min, max int) error {
	if l := len(n); l < min || l > max {
		return NewValidationError("invalid length (%d; expected %d <= length <= %d)", l, min, max)
	} else if len(strings.Trim(n, base64Alphabet)) != 0 {
		return NewValidationError("not base64 alphabet (%s)", n)
	}
	return nil
}

const (
	// Note that any character with ordinal value less than or equal to '#' (35),
	// which is the allocator KeySpace separator, must not be included in this alphabet.
	base64Alphabet           = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_+/="
	minLabelLen, maxLabelLen = 2, 64
	maxLabelValueLen         = 1024
)
