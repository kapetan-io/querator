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
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
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

func Produce() {
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
}

func Reserve() {
	var out string

	fmt.Println("---------------------------")
	fmt.Println("QueueReserveRequest")
	fmt.Println("---------------------------")
	onError(toString(&out, &pb.QueueReserveRequest{
		QueueName:      "queue-name",
		ClientId:       "client-01",
		BatchSize:      1_000,
		RequestTimeout: "30s",
	}))
	fmt.Println(out)

	fmt.Println("---------------------------")
	fmt.Println("QueueReserveResponse")
	fmt.Println("---------------------------")
	onError(toString(&out, &pb.QueueReserveResponse{
		Items: []*pb.QueueReserveItem{
			{
				Encoding:        "application/json",
				Kind:            "webhook-v2",
				Reference:       "account-1234",
				Id:              "queue-name~1234",
				Attempts:        0,
				ReserveDeadline: timestamppb.New(time.Now().UTC()),
				Bytes:           []byte("{\"key\":\"value\"}"),
			},
		},
	}))
	fmt.Println(out)
}

func Complete() {
	var out string

	fmt.Println("---------------------------")
	fmt.Println("QueueCompleteRequest")
	fmt.Println("---------------------------")
	onError(toString(&out, &pb.QueueCompleteRequest{
		QueueName:      "queue-name",
		RequestTimeout: "30s",
		Ids: []string{
			"id-1234",
			"id-1235",
			"id-1236",
		},
	}))
	fmt.Println(out)

	fmt.Println("---------------------------")
	fmt.Println("QueueCompleteResponse")
	fmt.Println("---------------------------")
	onError(toString(&out, &v1.Reply{Code: duh.CodeOK}))
	fmt.Println(out)
}

func main() {
	Replies()
	//Produce()
	//Reserve()
	Complete()
}
