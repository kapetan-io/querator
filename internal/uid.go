package internal

import "github.com/segmentio/ksuid"

// NewUID generates a new unique identifier using KSUID
func NewUID() string {
	return ksuid.New().String()
}
