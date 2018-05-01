package protocol

// Validate returns an error if the ReadRequest is not well-formed.
func (m *ReadRequest) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return err
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
	if m.Fragment != nil {
		if err := m.Fragment.Validate(); err != nil {
			return ExtendContext(err, "Fragment")
		}
	}
	if m.Route != nil {
		if err := m.Route.Validate(); err != nil {
			return ExtendContext(err, "Route")
		}
	}
	return nil
}

// Validate returns an error if the ReplicateRequest is not well-formed.
func (m *ReplicateRequest) Validate() error {

	// There are three types of ReplicateRequest:
	//  * Request: Journal, NextOffset, and Route are set.
	//  * Content chunk: Content and NextOffset are set.
	//  * Commit: Commit is set (only).
	var isRequest = m.Journal != ""
	var isContent = m.Content != nil
	var isCommit = !isRequest && !isContent

	if isRequest {
		if err := m.Journal.Validate(); err != nil {
			return err
		} else if m.Route == nil {
			return NewValidationError("expected Route")
		} else if err := m.Route.Validate(); err != nil {
			return ExtendContext(err, "Route")
		} else if isContent {
			return NewValidationError("unexpected Content")
		}
	} else {
		if m.Route != nil {
			return NewValidationError("unexpected Route (%s)", m.Route.String())
		}
	}

	if isCommit {
		if m.NextOffset != 0 {
			return NewValidationError("unexpected NextOffset (%d)", m.NextOffset)
		} else if m.Commit < 0 {
			return NewValidationError("invalid Commit (%d; expected >= 0)", m.Commit)
		}
	} else {
		if m.NextOffset < 0 {
			return NewValidationError("invalid NextOffset (%d; expected >= 0)", m.NextOffset)
		} else if m.Commit != 0 {
			return NewValidationError("unexpected Commit (%d)", m.Commit)
		}
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
