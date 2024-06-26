package internal

import (
	"github.com/kapetan-io/querator/proto"
	"time"
)

func validateQueueProduceProto(req *proto.QueueProduceRequest, r *ProduceRequest) error {
	var err error

	r.RequestTimeout, err = time.ParseDuration(req.RequestTimeout)
	if err != nil {
		return NewBadRequest("RequestTimeout is invalid; %s", err.Error())
	}

	if len(r.Items) == 0 {
		return NewBadRequest("'Items' cannot be empty; at least one item is required")
	}

	for i, in := range req.Items {
		// TODO: Memory Pool
		var out ProduceItem

		if in.DeadTimeout != "" {
			out.DeadTimeout, err = time.ParseDuration(in.DeadTimeout)
			if err != nil {
				return NewBadRequest("Items[%d].DeadTimeout is invalid; %s", i, err.Error())
			}
		}
		out.Item.FromProtoProduceItem(in)
		r.Items = append(r.Items, &out)
	}
	return nil
}
