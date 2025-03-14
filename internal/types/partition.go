package types

import (
	"github.com/kapetan-io/tackle/clock"
)

type PartitionState struct {
	// Failures is a count of how many times the underlying storage has failed. Resets to
	// zero when storage stops failing. If the value is zero, then the partition is considered
	// active and has communication with the underlying storage. It is updated by the
	// partition LifeCycle
	Failures int
	// UnReserved is the total number of un-reserved items in the partition. It is updated by
	// the partition LifeCycle
	UnReserved int
	// NumReserved is the total number of items reserved during the most recent distribution
	NumReserved int
	// MostRecentDeadline is the most recent deadline of this partition. This could be
	// the ReserveDeadline, or it could be the ExpireDeadline which ever is sooner. It is
	// used to notify LifeCycle of changes to the partition made by this partition
	// as a hint for when an action might be needed on items in the partition.
	MostRecentDeadline clock.Time // TODO: Ensure this gets sent to LifeCycle
}
