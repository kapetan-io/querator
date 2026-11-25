package transport_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/kapetan-io/querator/transport"
	"github.com/kapetan-io/querator/transport/auth"
)

func TestOpenAPIPathsAreServed(t *testing.T) {
	raw, err := os.ReadFile("../openapi.yaml")
	require.NoError(t, err)

	var spec struct {
		Paths map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &spec))
	require.NotEmpty(t, spec.Paths)

	handler := transport.NewHTTPHandler(transport.HTTPHandlerConfig{AuthBackend: &auth.NoOpAuthBackend{}})

	for path := range spec.Paths {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			func() {
				defer func() { _ = recover() }()
				handler.ServeHTTP(rec, req)
			}()
			assert.NotContains(t, rec.Body.String(), "no such method")
		})
	}
}
