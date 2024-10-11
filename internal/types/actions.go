package types

const (
	ActionMoveScheduledItem ActionKind = iota
	ActionReserveExpired
	ActionItemExpired
	ActionItemMaxAttempts
)

func ActionToString(a ActionKind) string {
	switch a {
	case ActionMoveScheduledItem:
		return "MoveScheduledItem"
	case ActionReserveExpired:
		return "ReserveExpired"
	case ActionItemExpired:
		return "ItemExpired"
	case ActionItemMaxAttempts:
		return "ItemMaxAttempts"
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
