package transport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/querator/transport/auth"
)

// failingAuthBackend returns a plain (non-duh) error so the handler logs an internal error;
// the product cannot inject this fault through a real auth backend.
type failingAuthBackend struct {
	auth.NoOpAuthBackend
}

func (f *failingAuthBackend) Authenticate(_ context.Context, _ string) (auth.Principal, error) {
	return auth.Principal{}, errors.New("auth store unavailable")
}

func TestReplyErrorRedactsCredentialHeadersInLog(t *testing.T) {
	var logs bytes.Buffer
	handler := transport.NewHTTPHandler(transport.HTTPHandlerConfig{
		AuthBackend: &failingAuthBackend{},
		Log:         slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	const secret = "qtr-live-supersecretvalue"
	req, err := http.NewRequest(http.MethodPost, server.URL+transport.RPCQueuesList, strings.NewReader(""))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Cookie", "session="+secret)
	req.Header.Set("X-Request-Id", "req-123")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	assert.NotContains(t, logs.String(), secret)
	var entry struct {
		Msg     string              `json:"msg"`
		Headers map[string][]string `json:"http.request.headers"`
	}
	require.NoError(t, json.Unmarshal(logs.Bytes(), &entry))
	assert.Equal(t, "auth store unavailable", entry.Msg)
	assert.Equal(t, []string{"[REDACTED]"}, entry.Headers["Authorization"])
	assert.Equal(t, []string{"[REDACTED]"}, entry.Headers["Cookie"])
	assert.Equal(t, []string{"req-123"}, entry.Headers["X-Request-Id"])
}
