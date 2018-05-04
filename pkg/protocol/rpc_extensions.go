package protocol

import "net/url"

// Validate returns an error if the ReadRequest is not well-formed.
func (m *ReadRequest) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return ExtendContext(err, "Journal")
	} else if m.Offset < -1 {
		return NewValidationError("invalid Offset (%d; expected -1 <= Offset <= MaxInt64", m.Offset)
	}
	return nil
}

// Validate returns an error if the ReadResponse is not well-formed.
func (m *ReadResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	}

	if m.Content != nil {
		// Require that no other fields are set.
		if m.Status != Status_OK {
			return NewValidationError("unexpected Status with Content (%v)", m.Status)
		} else if m.Offset != 0 {
			return NewValidationError("unexpected Offset with Content (%d)", m.Offset)
		} else if m.WriteHead != 0 {
			return NewValidationError("unexpected WriteHead with Content (%d)", m.WriteHead)
		} else if m.Route != nil {
			return NewValidationError("unexpected Route with Content (%s)", m.Route)
		} else if m.Fragment != nil {
			return NewValidationError("unexpected Fragment with Content (%s)", m.Fragment)
		} else if m.FragmentUrl != "" {
			return NewValidationError("unexpected FragmentUrl with Content (%s)", m.FragmentUrl)
		}
		return nil
	}

	if m.Route != nil {
		if err := m.Route.Validate(); err != nil {
			return ExtendContext(err, "Route")
		}
	}
	if m.WriteHead < 0 {
		return NewValidationError("invalid WriteHead (%d; expected >= 0)", m.WriteHead)
	}

	if m.Fragment != nil {
		if err := m.Fragment.Validate(); err != nil {
			return ExtendContext(err, "Fragment")
		} else if m.Offset < m.Fragment.Begin || m.Offset >= m.Fragment.End {
			return NewValidationError("invalid Offset (%d; expected %d <= offset < %d)",
				m.Offset, m.Fragment.Begin, m.Fragment.End)
		} else if m.WriteHead < m.Fragment.End {
			return NewValidationError("invalid WriteHead (%d; expected >= %d)",
				m.WriteHead, m.Fragment.End)
		}
		if m.FragmentUrl != "" {
			if _, err := url.Parse(m.FragmentUrl); err != nil {
				return ExtendContext(&ValidationError{Err: err}, "FragmentUrl")
			}
		}
	} else {
		if m.Offset != 0 {
			return NewValidationError("unexpected Offset without Fragment (%d)", m.Offset)
		} else if m.FragmentUrl != "" {
			return NewValidationError("unexpected FragmentUrl without Fragment (%s)", m.FragmentUrl)
		}
	}

	return nil
}

// Validate returns an error if the ReplicateRequest is not well-formed.
func (m *ReplicateRequest) Validate() error {
	// If Commit is set, we expect all other fields are zero'd.
	if m.Commit != 0 {
		if m.Commit < 0 {
			return NewValidationError("invalid Commit (%d; expected >= 0)", m.Commit)
		} else if m.Journal != "" {
			return NewValidationError("unexpected Journal with Commit (%v)", m.Journal)
		} else if m.NextOffset != 0 {
			return NewValidationError("unexpected NextOffset with Commit (%d)", m.NextOffset)
		} else if m.Route != nil {
			return NewValidationError("unexpected Route with Commit (%s)", m.Route)
		} else if m.Content != nil {
			return NewValidationError("unexpected Content with Commit (len %d)", len(m.Content))
		}
		return nil
	}

	if m.NextOffset < 0 {
		return NewValidationError("invalid NextOffset (%d; expected >= 0)", m.NextOffset)
	}

	// If Content is set, we expect only NextOffset.
	if m.Content != nil {
		if m.Journal != "" {
			return NewValidationError("unexpected Journal with Content (%v)", m.Journal)
		} else if m.Route != nil {
			return NewValidationError("unexpected Route with Content (%s)", m.Route)
		}
		return nil
	}

	if err := m.Journal.Validate(); err != nil {
		return ExtendContext(err, "Journal")
	} else if m.Route == nil {
		return NewValidationError("expected Route")
	} else if err := m.Route.Validate(); err != nil {
		return ExtendContext(err, "Route")
	}

	return nil
}

// Validate returns an error if the ReplicateResponse is not well-formed.
func (m *ReplicateResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	}
	if m.Route != nil {
		if err := m.Route.Validate(); err != nil {
			return ExtendContext(err, "Route")
		}
	}
	if m.WriteHead < 0 {
		return NewValidationError("invalid WriteHead (%d; expected >= 0)", m.WriteHead)
	}
	return nil
}

// Validate returns an error if the AppendRequest is not well-formed.
func (m *AppendRequest) Validate() error {
	// There are two types of AppendRequest:
	//  * Initial request - Journal is set (only).
	//  * Content chunk - Content is set (only).
	if m.Journal != "" {
		if err := m.Journal.Validate(); err != nil {
			return err
		} else if len(m.Content) != 0 {
			return NewValidationError("unexpected Content")
		}
	} else if len(m.Content) == 0 {
		return NewValidationError("expected Content")
	}
	return nil
}

// Validate returns an error if the AppendResponse is not well-formed.
func (m *AppendResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return ExtendContext(err, "Status")
	}
	if m.Route != nil {
		if err := m.Route.Validate(); err != nil {
			return ExtendContext(err, "Route")
		}
	}
	if m.WriteHead < 0 {
		return NewValidationError("invalid WriteHead (%d; expected >= 0)", m.WriteHead)
	}
	return nil
}

// Validate returns an error if the Status is not well-formed.
func (m Status) Validate() error {
	if _, ok := Status_name[int32(m)]; !ok {
		return NewValidationError("invalid status (%s)", m)
	}
	return nil
}
