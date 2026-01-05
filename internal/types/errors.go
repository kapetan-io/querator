package types

import "github.com/kapetan-io/querator/transport"

// Namespace errors
var (
	ErrNamespaceNotExist      = transport.NewRequestFailed("namespace does not exist")
	ErrNamespaceAlreadyExists = transport.NewInvalidOption("namespace already exists")
	ErrNamespaceHasQueues     = transport.NewInvalidOption("namespace has queues; delete queues first")
	ErrNamespaceReserved      = transport.NewInvalidOption("namespace name is reserved; names starting with '_' are reserved")
)
