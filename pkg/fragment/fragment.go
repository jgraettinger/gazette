package fragment

import (
	"os"

	"github.com/LiveRamp/gazette/pkg/protocol"
)

// Fragment wraps the protocol.Fragment type with a backing,
// local file (if one exists).
type Fragment struct {
	protocol.Fragment

	// Local uncompressed file of the Fragment, or nil iff the Fragment is remote.
	File *os.File
}
