package main_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kapetan-io/querator"
	main "github.com/kapetan-io/querator/cmd/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateCommandKeepsEarlierUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Shutdown(context.Background())) }()

	const queueName = "update-twice"
	c := d.MustClient()
	require.NoError(t, c.QueuesCreate(ctx, &pb.QueueInfo{
		QueueName:           queueName,
		LeaseTimeout:        "1m",
		ExpireTimeout:       "10m",
		RequestedPartitions: 1,
	}))

	endpoint := fmt.Sprintf("http://%s", d.Listener.Addr().String())
	_, err = captureOutput(t, func() error {
		return main.RunUpdate(main.FlagParams{Endpoint: endpoint, ReferenceUpdate: "first"}, queueName)
	})
	require.NoError(t, err)
	_, err = captureOutput(t, func() error {
		return main.RunUpdate(main.FlagParams{Endpoint: endpoint, MaxAttemptsUpdate: 5}, queueName)
	})
	require.NoError(t, err)

	var list pb.QueuesListResponse
	require.NoError(t, c.QueuesList(ctx, &list, &querator.ListOptions{Pivot: queueName, Limit: 1}))
	require.Len(t, list.Items, 1)
	assert.Equal(t, "first", list.Items[0].Reference)
	assert.Equal(t, int32(5), list.Items[0].MaxAttempts)
}
