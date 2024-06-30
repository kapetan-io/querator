package querator

import (
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"time"
)

func (s *Service) validateQueueProduceProto(in *proto.QueueProduceRequest, out *internal.ProduceRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidRequest("request_timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	for _, item := range in.Items {
		// TODO: From Memory Pool
		var qi store.Item
		qi.Encoding = item.Encoding
		qi.Kind = item.Kind
		qi.Reference = item.Reference
		qi.Body = item.Body
		out.Items = append(out.Items, &qi)
	}
	return nil
}

func (s *Service) validateQueueReserveProto(in *proto.QueueReserveRequest, out *internal.ReserveRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidRequest("request_timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	out.ClientID = in.ClientId
	out.BatchSize = in.BatchSize

	return nil
}

func (s *Service) validateQueueCompleteProto(in *proto.QueueCompleteRequest, out *internal.CompleteRequest) error {
	var err error

	// TODO: Move this into Queue.Complete()
	//if strings.TrimSpace(in.QueueName) == "" {
	//	return transport.NewInvalidRequest("'queue_name' cannot be empty")
	//}

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidRequest("request_timeout is invalid; %s - expected format: 900ms, 5m or 15m", err.Error())
		}
	}

	out.Ids = in.Ids

	return nil
}

func (s *Service) validateQueueOptionsProto(in *proto.QueueOptions, out *internal.QueueOptions) error {
	var err error

	if in.DeadTimeout != "" {
		out.DeadTimeout, err = time.ParseDuration(in.DeadTimeout)
		if err != nil {
			return transport.NewInvalidRequest("dead_timeout is invalid; %s - expected format: 60m, 2h or 24h", err.Error())
		}
	}

	if in.ReserveTimeout != "" {
		out.ReserveTimeout, err = time.ParseDuration(in.ReserveTimeout)
		if err != nil {
			return transport.NewInvalidRequest("res is invalid; %s -  expected format: 8m, 15m or 1h", err.Error())
		}
	}

	out.DeadQueue = in.DeadQueue
	out.Name = in.Name
	return nil
}
