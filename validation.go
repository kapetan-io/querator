package querator

import (
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"time"
)

func validateQueueProduceProto(in *proto.QueueProduceRequest, out *internal.ProduceRequest) error {
	var err error

	out.RequestTimeout, err = time.ParseDuration(in.RequestTimeout)
	if err != nil {
		return transport.NewInvalidRequest("RequestTimeout is invalid; %s", err.Error())
	}

	if len(in.Items) == 0 {
		return transport.NewInvalidRequest("'Items' cannot be empty; at least one item is required")
	}

	for _, item := range in.Items {
		// TODO: From Memory Pool
		var qi store.QueueItem
		qi.FromProtoProduceItem(item)
		out.Items = append(out.Items, &qi)
	}
	return nil
}

func validateQueueOptionsProto(in *proto.QueueOptions, out *internal.QueueOptions) error {
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
