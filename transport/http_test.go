package transport

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/duh-rpc/duh-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPPROFEndpoint(t *testing.T) {
	h := HTTPHandler{
		duration: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "http_handler_duration",
		}, []string{"path"}),
		maxProduceSize: 1,
		metrics:        nil,
		log:            duh.NoOpLogger{},
		service:        nil,
	}
	srv := httptest.NewServer(&h)
	defer srv.Close()

	clt := srv.Client()

	for _, test := range []struct {
		name       string
		uri        string
		expectCode int
	}{{
		name:       "allocs",
		uri:        "/pprof/allocs",
		expectCode: 200,
	}, {
		name:       "goroutine blocks",
		uri:        "/pprof/block",
		expectCode: 200,
	}, {
		name:       "missing profile name",
		uri:        "/pprof/",
		expectCode: 404,
	}, {
		name:       "garbage",
		uri:        "/pprof/test40ways",
		expectCode: 404,
	}, {
		name:       "too long, didn't stop",
		uri:        "/pprof/allocs/must/wonder/why/this/kept/going",
		expectCode: 404,
	}, {
		name:       "SQL injections are rude",
		uri:        "/pprof/%27+UNION+SELECT+user%2C+password+FROM+users--",
		expectCode: 404,
	}} {
		t.Run(test.name, func(t *testing.T) {
			rq, err := http.NewRequest("GET", fmt.Sprintf("%s%s", srv.URL, test.uri), nil)
			require.NoError(t, err)

			rs, err := clt.Do(rq)
			require.NoError(t, err)

			require.Equal(t, test.expectCode, rs.StatusCode, "Expected HTTP %d for GET %s", test.expectCode, test.uri)
		})
	}
}
