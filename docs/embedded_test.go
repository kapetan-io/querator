package main_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockedBuffer lets the test read the example's output while the process is still writing it.
type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestEmbeddedShutsDownDaemonOnInterrupt(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "embedded")
	require.NoError(t, exec.Command("go", "build", "-o", binary, "embedded.go").Run())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var output lockedBuffer
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(os.Environ(), "QUERATOR_ADDRESS=localhost:0")
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())

	require.Eventually(t, func() bool {
		return strings.Contains(output.String(), "Example completed successfully")
	}, 20*time.Second, 50*time.Millisecond)

	require.NoError(t, cmd.Process.Signal(os.Interrupt))
	require.NoError(t, cmd.Wait())
	require.NoError(t, ctx.Err())
	assert.Contains(t, output.String(), "HTTP Server shutdown")
}
