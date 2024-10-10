package types

const (
	ActionMoveScheduledItem ActionKind = iota
	ActionResetReserved
	ActionDeadLetter
)

type ActionKind int

type Action struct {
	Action       ActionKind
	Queue        string
	Item         Item
	PartitionNum int
}
