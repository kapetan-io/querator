package transport

import (
	"context"

	pb "github.com/kapetan-io/querator/proto"
)

// QueueOps handles high-frequency queue operations
type QueueOps interface {
	QueueProduce(context.Context, *pb.QueueProduceRequest) error
	QueueLease(context.Context, *pb.QueueLeaseRequest, *pb.QueueLeaseResponse) error
	QueueComplete(context.Context, *pb.QueueCompleteRequest) error
	QueueRetry(context.Context, *pb.QueueRetryRequest) error
	QueueStats(context.Context, *pb.QueueStatsRequest, *pb.QueueStatsResponse) error
	QueueClear(context.Context, *pb.QueueClearRequest) error
}

// QueueAdmin handles queue lifecycle management
type QueueAdmin interface {
	QueuesCreate(context.Context, *pb.QueueInfo) error
	QueuesList(context.Context, *pb.QueuesListRequest, *pb.QueuesListResponse) error
	QueuesUpdate(context.Context, *pb.QueueInfo) error
	QueuesDelete(context.Context, *pb.QueuesDeleteRequest) error
	QueuesInfo(context.Context, *pb.QueuesInfoRequest, *pb.QueueInfo) error
}

// StorageInspector handles direct storage access
type StorageInspector interface {
	StorageItemsList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
	StorageItemsImport(context.Context, *pb.StorageItemsImportRequest, *pb.StorageItemsImportResponse) error
	StorageItemsDelete(context.Context, *pb.StorageItemsDeleteRequest) error
	StorageScheduledList(context.Context, *pb.StorageItemsListRequest, *pb.StorageItemsListResponse) error
}

// Service combines all interfaces for backward compatibility
type Service interface {
	QueueOps
	QueueAdmin
	StorageInspector
}
