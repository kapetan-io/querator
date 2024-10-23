package types

const (
	ActionQueueScheduledItem ActionKind = iota
	ActionReserveExpired
	ActionItemExpired
	ActionItemMaxAttempts
	ActionDeleteItem
)

func ActionToString(a ActionKind) string {
	switch a {
	case ActionQueueScheduledItem:
		return "QueueScheduledItem"
	case ActionReserveExpired:
		return "ReserveExpired"
	case ActionItemExpired:
		return "ItemExpired"
	case ActionItemMaxAttempts:
		return "ItemMaxAttempts"
	case ActionDeleteItem:
		return "DeleteItem"
	default:
		return "UnknownAction"
	}
}

type ActionKind int

type Action struct {
	Action       ActionKind
	Queue        string
	Item         Item
	PartitionNum int
}
