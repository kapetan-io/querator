package internal

import (
	"context"
	"strings"
	"unicode"

	"github.com/kapetan-io/errors"
	"github.com/kapetan-io/querator/internal/store"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/transport/reply"
)

type DeadLetterRules struct {
	Find     func(ctx context.Context, name string) (types.QueueInfo, error)
	SourceOf func(ctx context.Context, name string) (string, error)
}

func (r DeadLetterRules) Check(ctx context.Context, info types.QueueInfo) error {
	if err := r.checkDeadQueue(ctx, info); err != nil {
		return err
	}
	return r.checkAsDeadQueue(ctx, info)
}

func (r DeadLetterRules) checkDeadQueue(ctx context.Context, info types.QueueInfo) error {
	if info.DeadQueue == "" {
		return nil
	}

	const maxQueueNameLength = 500
	if len(info.DeadQueue) > maxQueueNameLength {
		return reply.NewInvalidOption("dead queue is invalid; cannot be greater than '%d' characters", maxQueueNameLength)
	}

	if strings.ContainsFunc(info.DeadQueue, unicode.IsSpace) {
		return reply.NewInvalidOption("dead queue is invalid; '%s' cannot contain whitespace", info.DeadQueue)
	}

	if strings.Contains(info.DeadQueue, "~") {
		return reply.NewInvalidOption("dead queue is invalid; '%s' cannot contain '~' character", info.DeadQueue)
	}

	if info.DeadQueue == info.Name {
		return reply.NewInvalidOption("dead_queue cannot reference itself")
	}

	dlq, err := r.Find(ctx, info.DeadQueue)
	if err != nil {
		if errors.Is(err, store.ErrQueueNotExist) {
			return reply.NewInvalidOption("dead_queue does not exist; '%s' was not found, create it first", info.DeadQueue)
		}
		return err
	}

	if dlq.DeadQueue != "" {
		return reply.NewInvalidOption("dead_queue is invalid; '%s' already has its own dead_queue configured, "+
			"clear or change dead_queue on '%s' first", info.DeadQueue, info.DeadQueue)
	}

	if dlq.RequestedPartitions > 1 {
		return reply.NewInvalidOption("dead_queue is invalid; '%s' must have a single partition, but has '%d'",
			info.DeadQueue, dlq.RequestedPartitions)
	}

	return nil
}

func (r DeadLetterRules) checkAsDeadQueue(ctx context.Context, info types.QueueInfo) error {
	if info.DeadQueue == "" && info.RequestedPartitions <= 1 {
		return nil
	}

	source, err := r.SourceOf(ctx, info.Name)
	if err != nil {
		return err
	}
	if source == "" {
		return nil
	}

	if info.DeadQueue != "" {
		return reply.NewInvalidOption("dead_queue is invalid; '%s' is the dead_queue of '%s' and cannot have its own dead_queue, "+
			"clear or change dead_queue on '%s' first", info.Name, source, source)
	}

	return reply.NewInvalidOption("partitions is invalid; '%s' is the dead_queue of '%s' and must have a single partition, "+
		"clear or change dead_queue on '%s' first", info.Name, source, source)
}
