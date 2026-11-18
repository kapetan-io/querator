package types

import (
	"github.com/kapetan-io/tackle/clock"
)

type PartitionState struct {
	Failures           int
	UnLeased           int
	NumLeased          int
	MostRecentDeadline clock.Time
}
