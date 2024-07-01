package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/duh-rpc/duh-go"
	v1 "github.com/duh-rpc/duh-go/proto/v1"
	pb "github.com/kapetan-io/querator/proto"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func toString(out *string, in proto.Message) error {
	b, err := jsonpb.Marshal(in)
	if err != nil {
		return err
	}

	var dst bytes.Buffer
	if err := json.Indent(&dst, b, "", "  "); err != nil {
		return err
	}
	*out = dst.String()
	return nil
}

func onError(err error) {
	if err != nil {
		fmt.Printf("Err: %s\n", err)
	}
}

func Replies() {
	var out string

	fmt.Println("---------------------------")
	fmt.Println("Success")
	fmt.Println("---------------------------")
	onError(toString(&out, &v1.Reply{
		Code:     200,
		CodeText: "OK",
		Message:  "success message",
	}))
	fmt.Println(out)

	fmt.Println("---------------------------")
	fmt.Println("Invalid Request")
	fmt.Println("---------------------------")
	onError(toString(&out, &v1.Reply{
		Code:     duh.CodeBadRequest,
		CodeText: duh.CodeText(duh.CodeBadRequest),
		Message:  "invalid request message",
		Details:  map[string]string{"docs": "https://kapetan.io/docs/querator"},
	}))
	fmt.Println(out)

	fmt.Println("---------------------------")
	fmt.Println("Retry Request")
	fmt.Println("---------------------------")
	onError(toString(&out, &v1.Reply{
		Code:     duh.CodeRetryRequest,
		CodeText: duh.CodeText(duh.CodeRetryRequest),
		Message:  "retry request message",
		Details:  map[string]string{"docs": "https://kapetan.io/docs/querator"},
	}))
	fmt.Println(out)

	fmt.Println("---------------------------")
	fmt.Println("Request Failed")
	fmt.Println("---------------------------")
	onError(toString(&out, &v1.Reply{
		Code:     duh.CodeRequestFailed,
		CodeText: duh.CodeText(duh.CodeRequestFailed),
		Message:  "request failed message",
		Details:  map[string]string{"docs": "https://kapetan.io/docs/querator"},
	}))
	fmt.Println(out)
}

func main() {
	var out string

	fmt.Println("---------------------------")
	fmt.Println("QueueProduceRequest")
	fmt.Println("---------------------------")
	onError(toString(&out, &pb.QueueProduceRequest{
		QueueName:      "queue-name",
		RequestTimeout: "30s",
		Items: []*pb.QueueProduceItem{
			{
				Encoding:  "application/json",
				Kind:      "webhook-v2",
				Reference: "account-1234",
				Utf8:      "{\"key\":\"value\"}",
			},
			{
				Encoding:  "application/json",
				Kind:      "webhook-v2",
				Reference: "account-5323",
				Bytes:     []byte("{\"key\":\"value\"}"),
			},
		},
	}))
	fmt.Println(out)

	j := `{
  "queue_name": "queue-name",
  "request_timeout": "30s",
  "items": [
    {
      "encoding": "application/json",
      "kind": "webhook-v2",
      "reference": "account-1234",
      "bytes": "eyJrZXkiOiJ2YWx1ZSJ9"
    },
    {
      "encoding": "application/json",
      "kind": "webhook-v2",
      "reference": "account-5323",
      "utf8": "Hello World"
    }
  ]
}
`

	var req pb.QueueProduceRequest
	onError(json.Unmarshal([]byte(j), &req))
	fmt.Println(req.Items[0].Bytes)
	fmt.Println(req.Items[1].Utf8)
}
