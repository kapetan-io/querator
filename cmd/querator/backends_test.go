package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	main "github.com/kapetan-io/querator/cmd/querator"
	"github.com/kapetan-io/querator/daemon"
	pb "github.com/kapetan-io/querator/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestBackendsCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, err := daemon.NewDaemon(ctx, daemon.Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Shutdown(context.Background())) }()
	endpoint := fmt.Sprintf("http://%s", d.Listener.Addr().String())

	t.Run("UpdateThenList", func(t *testing.T) {
		_, err := captureOutput(t, func() error {
			return main.RunBackendsUpdate(main.FlagParams{Endpoint: endpoint, Affinity: 0, AffinitySet: true}, "mem-0")
		})
		require.NoError(t, err)

		out, err := captureOutput(t, func() error {
			return main.RunBackendsList(main.FlagParams{Endpoint: endpoint})
		})
		require.NoError(t, err)

		var list pb.StorageBackendsListResponse
		require.NoError(t, protojson.Unmarshal([]byte(out.Stdout), &list))
		require.Len(t, list.Backends, 1)
		assert.Equal(t, "mem-0", list.Backends[0].Backend)
		assert.Equal(t, 0.0, list.Backends[0].GetAffinity())
		assert.True(t, list.Backends[0].GetSafeToStop())

		var raw struct {
			Backends []struct {
				Partitions []any `json:"partitions"`
				Pending    struct {
					Create []any `json:"create"`
					Drain  []any `json:"drain"`
				} `json:"pending"`
			} `json:"backends"`
		}
		require.NoError(t, json.Unmarshal([]byte(out.Stdout), &raw))
		require.Len(t, raw.Backends, 1)
		assert.NotNil(t, raw.Backends[0].Partitions)
		assert.NotNil(t, raw.Backends[0].Pending.Create)
		assert.NotNil(t, raw.Backends[0].Pending.Drain)
	})

	t.Run("UpdateWithoutAffinityFlag", func(t *testing.T) {
		_, err := captureOutput(t, func() error {
			return main.RunBackendsUpdate(main.FlagParams{Endpoint: endpoint}, "mem-0")
		})
		require.ErrorContains(t, err, "no update flags provided")
	})

	t.Run("UpdateUnknownBackend", func(t *testing.T) {
		_, err := captureOutput(t, func() error {
			return main.RunBackendsUpdate(main.FlagParams{Endpoint: endpoint, Affinity: 1, AffinitySet: true}, "no-such")
		})
		require.ErrorContains(t, err, "'no-such' is not a known storage backend")
	})
}

func TestBackendsUpdateSendsBackendAndAffinity(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	_, err := captureOutput(t, func() error {
		return main.RunBackendsUpdate(main.FlagParams{Endpoint: ms.URL(), Affinity: 2.5, AffinitySet: true}, "pg-2")
	})
	require.NoError(t, err)

	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "/v1/storage/backends.update", reqs[0].Path)
	var req pb.StorageBackendsUpdateRequest
	require.NoError(t, proto.Unmarshal(reqs[0].Body, &req))
	assert.Equal(t, "pg-2", req.Backend)
	assert.Equal(t, 2.5, req.GetAffinity())
}

func TestBackendsListPrintsEachBackend(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	out, err := captureOutput(t, func() error {
		return main.RunBackendsList(main.FlagParams{Endpoint: ms.URL()})
	})
	require.NoError(t, err)

	var list pb.StorageBackendsListResponse
	require.NoError(t, protojson.Unmarshal([]byte(out.Stdout), &list))
	require.Len(t, list.Backends, 1)
	assert.Equal(t, "test-backend", list.Backends[0].Backend)
	assert.Equal(t, 1.0, list.Backends[0].GetAffinity())
}
