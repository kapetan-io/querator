package querator

import (
	"github.com/kapetan-io/querator/internal/types"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/transport"
	"time"
)

func (s *Service) validateQueueProduceProto(in *proto.QueueProduceRequest, out *types.ProduceRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request_timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
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
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request_timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
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
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidOption("request_timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, id := range in.Ids {
		out.Ids = append(out.Ids, []byte(id))
	}

	return nil
}

func (s *Service) validateQueueOptionsProto(in *proto.QueueInfo, out *types.QueueInfo) error {
	var err error

	if in.DeadTimeout != "" {
		out.DeadTimeout, err = time.ParseDuration(in.DeadTimeout)
		if err != nil {
			return transport.NewInvalidOption("dead_timeout is invalid; %s - expected format: 60m, 2h or 24h", err.Error())
		}
	}

	if in.ReserveTimeout != "" {
		out.ReserveTimeout, err = time.ParseDuration(in.ReserveTimeout)
		if err != nil {
			return transport.NewInvalidOption("res is invalid; %s -  expected format: 8m, 15m or 1h", err.Error())
		}
	}

	out.DeadQueue = in.DeadQueue
	out.Name = in.QueueName
	return nil
}

func (s *Service) validateQueuePauseRequestProto(in *proto.QueuePauseRequest, out *types.PauseRequest) error {
	var err error

	if in.PauseDuration != "" {
		out.PauseDuration, err = time.ParseDuration(in.PauseDuration)
		if err != nil {
			return transport.NewInvalidOption("pause_duration is invalid; %s - expected format: 60m, 2h or 24h", err.Error())
		}
	}
	out.Pause = in.Pause
	return nil
}
