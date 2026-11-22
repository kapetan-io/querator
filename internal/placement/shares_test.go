package placement_test

import (
	"fmt"
	"testing"

	"github.com/kapetan-io/querator/internal/placement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every caller checks the count and the backends before it calls Shares, so no client call
// reaches the package's own refusals.
func TestSharesRefuses(t *testing.T) {
	for _, test := range []struct {
		name     string
		count    int
		backends []placement.Backend
		wantErr  string
	}{
		{
			name:     "MoreThanMaxPartitions",
			count:    1_025,
			backends: []placement.Backend{{Name: "mem-00", Affinity: 1}},
			wantErr:  "cannot place 1025 partitions, more than 1024",
		},
		{
			name:     "DuplicateNames",
			count:    6,
			backends: []placement.Backend{{Name: "m-a", Affinity: 1}, {Name: "m-a", Affinity: 1}, {Name: "m-b", Affinity: 1}},
			wantErr:  "duplicate partition storage name 'm-a'",
		},
		{
			name:     "SumOverflows",
			count:    10,
			backends: []placement.Backend{{Name: "m-a", Affinity: 8e307}, {Name: "m-b", Affinity: 8e307}},
			wantErr:  "sum of partition storage affinities is too large",
		},
		{
			name:     "EmptyName",
			count:    10,
			backends: []placement.Backend{{Name: "", Affinity: 1}, {Name: "m-b", Affinity: 1}},
			wantErr:  "partition storage name cannot be empty",
		},
		{
			name:     "NoAffinityAboveZero",
			count:    1,
			backends: []placement.Backend{{Name: "mem-00", Affinity: 0}},
			wantErr:  placement.ErrNoBackend.Error(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := placement.Shares("orders", test.count, test.backends)
			require.ErrorContains(t, err, test.wantErr)
		})
	}

	t.Run("AtMaxPartitions", func(t *testing.T) {
		shares, err := placement.Shares("orders", 1_024, []placement.Backend{{Name: "m-a", Affinity: 1}, {Name: "m-b", Affinity: 1}})
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"m-a": 512, "m-b": 512}, shares)
	})
}

func TestSharesMatchWholeShares(t *testing.T) {
	backends := []placement.Backend{{Name: "badger", Affinity: 3}, {Name: "postgres", Affinity: 7}}
	for i := range 200 {
		shares, err := placement.Shares(fmt.Sprintf("queue-%d", i), 10, backends)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"badger": 3, "postgres": 7}, shares)
	}
}
