package internal

import "github.com/segmentio/ksuid"

func NewUID() string {
	return ksuid.New().String()
}
