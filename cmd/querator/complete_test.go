package main_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/kapetan-io/querator/cmd/querator"
	pb "github.com/kapetan-io/querator/proto"
)

func TestCompleteCommandWithFileNormalizesLines(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	file := filepath.Join(t.TempDir(), "ids.txt")
	// CRLF endings, blank lines, surrounding whitespace and no trailing newline.
	require.NoError(t, os.WriteFile(file,
		[]byte("item-1\r\n\r\n  item-2  \n\n\titem-3\r\n   \nitem-4"), 0o600))

	out, err := captureOutput(t, func() error {
		return main.RunComplete(main.FlagParams{
			File:            file,
			CompleteTimeout: "30s",
			Endpoint:        ms.URL(),
		}, "test-queue", 0, nil)
	})
	require.NoError(t, err)

	reqs := ms.GetRequests()
	require.Len(t, reqs, 1)
	var req pb.QueueCompleteRequest
	require.NoError(t, proto.Unmarshal(reqs[0].Body, &req))
	assert.Equal(t, []string{"item-1", "item-2", "item-3", "item-4"}, req.Ids)
	assert.Contains(t, out.Stderr, "Successfully completed 4 item")
}

func TestCompleteCommandWithBlankFileFails(t *testing.T) {
	ms := main.NewMockServer()
	defer ms.Close()

	file := filepath.Join(t.TempDir(), "ids.txt")
	require.NoError(t, os.WriteFile(file, []byte("\r\n  \n\n"), 0o600))

	_, err := captureOutput(t, func() error {
		return main.RunComplete(main.FlagParams{
			File:            file,
			CompleteTimeout: "30s",
			Endpoint:        ms.URL(),
		}, "test-queue", 0, nil)
	})
	require.ErrorContains(t, err, "no item IDs to complete")
	assert.Empty(t, ms.GetRequests())
}

func TestCompleteCommandRequiresPartitionFlag(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "querator")
	require.NoError(t, exec.Command("go", "build", "-o", binary, ".").Run())

	ms := main.NewMockServer()
	defer ms.Close()

	output, err := exec.Command(binary, "complete", "test-queue", "item-1",
		"--endpoint", ms.URL()).CombinedOutput()
	require.Error(t, err)
	assert.Contains(t, string(output), `required flag(s) "partition" not set`)
	assert.Empty(t, ms.GetRequests())
}
