package querator

import (
	"github.com/kapetan-io/querator/internal"
	"github.com/kapetan-io/querator/proto"
	"github.com/kapetan-io/querator/store"
	"github.com/kapetan-io/querator/transport"
	"time"
)

func validateQueueProduceProto(req *proto.QueueProduceRequest, r *internal.ProduceRequest) error {
	var err error

	r.RequestTimeout, err = time.ParseDuration(req.RequestTimeout)
	if err != nil {
		return transport.NewInvalidRequest("RequestTimeout is invalid; %s", err.Error())
	}

	if len(r.Items) == 0 {
		return transport.NewInvalidRequest("'Items' cannot be empty; at least one item is required")
	}

	for _, item := range req.Items {
		// TODO: From Memory Pool
		var in store.QueueItem
		in.FromProtoProduceItem(item)
		r.Items = append(r.Items, &in)
	}
	return nil
}
