package types

import (
	"fmt"

	"github.com/kapetan-io/tiger/assert"
)

const (
	ActionQueueScheduledItem ActionKind = iota
	ActionLeaseExpired
	ActionItemExpired
	ActionItemMaxAttempts
	ActionDeleteItem
)

func ActionToString(a ActionKind) string {
	switch a {
	case ActionQueueScheduledItem:
		return "QueueScheduledItem"
	case ActionLeaseExpired:
		return "LeaseExpired"
	case ActionItemExpired:
		return "ItemExpired"
	case ActionItemMaxAttempts:
		return "ItemMaxAttempts"
	case ActionDeleteItem:
		return "DeleteItem"
	default:
		assert.Unreachable(fmt.Sprintf("undefined action kind '0x%X'", int(a)))
	}
	return ""
}

type ActionKind int

type Action struct {
	Action       ActionKind
	Queue        string
	Item         Item
	PartitionNum int
}
