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
			return transport.NewInvalidRequest("produce request_timeout is invalid; %s", err.Error())
		}
	}

	if len(in.Items) == 0 {
		return transport.NewInvalidRequest("items cannot be empty; at least one item is required")
	}

	if len(in.Items) > s.opts.MaxProduceBatchSize {
		return transport.NewInvalidRequest("too many items in request; max_produce_batch_size is"+
			" %d but received %d", s.opts.MaxProduceBatchSize, len(in.Items))
	}

	for _, item := range in.Items {
		// TODO: From Memory Pool
		var qi store.QueueItem
		qi.FromProtoProduceItem(item)
		out.Items = append(out.Items, &qi)
	}
	return nil
}

func (s *Service) validateQueueReserveProto(in *proto.QueueReserveRequest, out *internal.ReserveRequest) error {
	var err error

	if in.RequestTimeout != "" {
		out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
		if err != nil {
			return transport.NewInvalidRequest("reserve request_timeout is invalid; %s", err.Error())
		}
	}

	if in.BatchSize > s.opts.MaxReserveBatchSize {
		return transport.NewInvalidRequest("requested batch_size is too large; max_reserve_batch_size is"+
			" %d but requested %d", s.opts.MaxProduceBatchSize, in.BatchSize)
	}

	// TODO: ADR about error messages '<unique error; <changing details about the error>'
	// TODO: Validate the rest <-- DO THIS NEXT

	return nil
}

func (s *Service) validateQueueOptionsProto(in *proto.QueueOptions, out *internal.QueueOptions) error {
	var err error

	if in.DeadTimeout != "" {
		out.DeadTimeout, err = time.ParseDuration(in.DeadTimeout)
		if err != nil {
			return transport.NewInvalidRequest("DeadTimeout is invalid; %s", err.Error())
		}
	}

	if in.ReserveTimeout != "" {
		out.ReserveTimeout, err = time.ParseDuration(in.ReserveTimeout)
		if err != nil {
			return transport.NewInvalidRequest("ReserveTimeout is invalid; %s", err.Error())
		}
	}

	out.DeadQueue = in.DeadQueue
	out.Name = in.Name
	return nil
}
