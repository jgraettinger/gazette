package protocol

import (
	"fmt"
	"strings"
)

// Validator is a type able to validate itself. Validate inspects the type for
// syntactic or semantic issues, and returns a descriptive error if any
// violations are encountered. It is recommended that Validate return instances
// of ValidationError where possible, which enables tracking nested contexts.
type Validator interface {
	Validate() error
}

// ValidationError is an error implementation which captures its validation context.
type ValidationError struct {
	Context []string
	Err     error
}

// Error implements the error interface.
func (ve *ValidationError) Error() string {
	if len(ve.Context) != 0 {
		return strings.Join(ve.Context, ".") + ": " + ve.Err.Error()
	} else {
		return ve.Err.Error()
	}
}

// ExtendContext type-checks |err| to a *ValidationError, and if matched extends
// it with |context|. In all cases the value of |err| is returned.
func ExtendContext(err error, format string, args ...interface{}) error {
	if ve, ok := err.(*ValidationError); ok {
		ve.Context = append([]string{fmt.Sprintf(format, args...)}, ve.Context...)
	}
	return err
}

// NewValidationError parallels fmt.Errorf to returns a new ValidationError instance.
func NewValidationError(format string, args ...interface{}) error {
	return &ValidationError{Err: fmt.Errorf(format, args...)}
}