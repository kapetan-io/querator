package querator

import (
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/tackle/clock"
)

const (
	maxTimeoutLength  = 15
	defaultAllocation = 512  // 2<<8
	maxAllocation     = 2048 // 2<<10
)

func allocInt32(mem int32) int {
	if mem < 0 {
		return defaultAllocation
	}
	if mem > maxAllocation {
		return defaultAllocation
	}
	return int(mem)
}

func (s *Service) validateQueueProduceProto(in *proto.QueueProduceRequest, out *types.ProduceRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, item := range in.Items {
		// TODO: From Memory Pool
		qi := new(types.Item)
		qi.Encoding = item.Encoding
		qi.Kind = item.Kind
		qi.Reference = item.Reference
		qi.EnqueueAt = item.EnqueueAt.AsTime()
		if item.Bytes != nil {
			qi.Payload = item.Bytes
		} else {
			qi.Payload = []byte(item.Utf8)
		}
		out.Items = append(out.Items, qi)
	}
	return nil
}

func (s *Service) validateQueueLeaseProto(in *proto.QueueLeaseRequest, out *types.LeaseRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	out.ClientID = in.ClientId
	out.NumRequested = int(in.BatchSize)

	return nil
}

func (s *Service) validateQueueCompleteProto(in *proto.QueueCompleteRequest, out *types.CompleteRequest) error {
	var err error

	// TODO: Move this into Queue.Complete()
	// if strings.TrimSpace(in.QueueName) == "" {
	// 	return transport.NewInvalidOption("'queue_name' cannot be empty")
	// }

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, id := range in.Ids {
		out.Ids = append(out.Ids, []byte(id))
	}

	out.Partition = int(in.Partition)

	return nil
}

func (s *Service) validateQueueRetryProto(in *proto.QueueRetryRequest, out *types.RetryRequest) error {
	// Note: RequestTimeout field is not defined in proto, using default timeout
	// TODO: Add RequestTimeout field to QueueRetryRequest proto definition
	out.RequestTimeout = clock.Duration(5 * clock.Minute)

	for _, item := range in.Items {
		retryItem := types.RetryItem{
			ID:   []byte(item.Id),
			Dead: item.Dead,
		}
		
		if item.RetryAt != nil {
			retryItem.RetryAt = clock.Time(item.RetryAt.AsTime())
		}
		
		out.Items = append(out.Items, retryItem)
	}

	out.Partition = int(in.Partition)

	return nil
}

func (s *Service) validateQueueOptionsProto(in *proto.QueueInfo, out *types.QueueInfo) error {
	var err error

	if len(in.LeaseTimeout) > maxTimeoutLength {
		return transport.NewInvalidOption("lease timeout is invalid; cannot be greater than '%d' characters", maxTimeoutLength)
	}

	if len(in.ExpireTimeout) > maxTimeoutLength {
		return transport.NewInvalidOption("expire timeout is invalid; cannot be greater than '%d' characters", maxTimeoutLength)
	}

	if in.ExpireTimeout != "" {
		out.ExpireTimeout, err = clock.ParseDuration(in.ExpireTimeout)
		if err != nil {
			return transport.NewInvalidOption("expire timeout is invalid; %s - expected format: 60m, 2h or 24h", err.Error())
		}
	}

	if in.LeaseTimeout != "" {
		out.LeaseTimeout, err = clock.ParseDuration(in.LeaseTimeout)
		if err != nil {
			return transport.NewInvalidOption("lease timeout is invalid; %s -  expected format: 8m, 15m or 1h", err.Error())
		}
	}

	out.MaxAttempts = int(in.MaxAttempts)
	out.RequestedPartitions = int(in.RequestedPartitions)
	out.DeadQueue = in.DeadQueue
	out.Reference = in.Reference
	out.Name = in.QueueName
	return nil
}
