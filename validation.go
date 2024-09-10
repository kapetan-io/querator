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
		if item.Bytes != nil {
			qi.Payload = item.Bytes
		} else {
			qi.Payload = []byte(item.Utf8)
		}
		out.Items = append(out.Items, qi)
	}
	return nil
}

func (s *Service) validateQueueReserveProto(in *proto.QueueReserveRequest, out *types.ReserveRequest) error {
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
	//if strings.TrimSpace(in.QueueName) == "" {
	//	return transport.NewInvalidOption("'queue_name' cannot be empty")
	//}

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = clock.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, id := range in.Ids {
		out.Ids = append(out.Ids, []byte(id))
	}

	return nil
}

func (s *Service) validateQueueOptionsProto(in *proto.QueueInfo, out *types.QueueInfo) error {
	var err error

	if len(in.ReserveTimeout) > maxTimeoutLength {
		return transport.NewInvalidOption("reserve timeout is invalid; cannot be greater than '%d' characters", maxTimeoutLength)
	}

	if len(in.DeadTimeout) > maxTimeoutLength {
		return transport.NewInvalidOption("dead timeout is invalid; cannot be greater than '%d' characters", maxTimeoutLength)
	}

	if in.DeadTimeout != "" {
		out.DeadTimeout, err = clock.ParseDuration(in.DeadTimeout)
		if err != nil {
			return transport.NewInvalidOption("dead timeout is invalid; %s - expected format: 60m, 2h or 24h", err.Error())
		}
	}

	if in.ReserveTimeout != "" {
		out.ReserveTimeout, err = clock.ParseDuration(in.ReserveTimeout)
		if err != nil {
			return transport.NewInvalidOption("reserve timeout is invalid; %s -  expected format: 8m, 15m or 1h", err.Error())
		}
	}

	out.MaxAttempts = int(in.MaxAttempts)
	out.Partitions = int(in.Partitions)
	out.DeadQueue = in.DeadQueue
	out.Reference = in.Reference
	out.Name = in.QueueName
	return nil
}
