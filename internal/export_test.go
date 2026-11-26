package internal

import (
	"context"

	"github.com/kapetan-io/querator/internal/types"
)

func (qm *QueuesManager) AdoptRecordInto(
	ctx context.Context,
	q *Queue,
	stored types.QueueInfo,
) error {
	return qm.adoptRecordInto(ctx, q, stored)
}
