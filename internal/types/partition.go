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

func (s *PartitionState) LeasesExpired(expired int) {
	given := min(expired, max(0, s.NumLeased))
	s.NumLeased -= given
	s.UnLeased += given
}
