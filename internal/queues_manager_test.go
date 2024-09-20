package internal

import (
	"github.com/kapetan-io/querator/internal/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAssignPartitions(t *testing.T) {

	for _, test := range []struct {
		Name     string
		Backends []store.Backend
		Expect   []int
	}{
		{
			Name: "OneBackend",
			Backends: []store.Backend{
				{
					Name:     "store-1",
					Affinity: 1.0,
				},
			},
			Expect: []int{100},
		},
		{
			Name: "EvenDist",
			Backends: []store.Backend{
				{
					Name:     "store-1",
					Affinity: 1.0,
				},
				{
					Name:     "store-2",
					Affinity: 1.0,
				},
				{
					Name:     "store-3",
					Affinity: 1.0,
				},
				{
					Name:     "store-4",
					Affinity: 1.0,
				},
			},
			Expect: []int{25, 25, 25, 25},
		},
		{
			Name: "ZeroAffinity",
			Backends: []store.Backend{
				{
					Name:     "store-1",
					Affinity: 1.0,
				},
				{
					Name:     "store-2",
					Affinity: 1.0,
				},
				{
					Name:     "store-3",
					Affinity: 1.0,
				},
				{
					Name:     "store-4",
					Affinity: 0.0,
				},
			},
			Expect: []int{33, 33, 33, 0},
		},
		{
			Name: "Preferred",
			Backends: []store.Backend{
				{
					Name:     "store-1",
					Affinity: 1.0,
				},
				{
					Name:     "store-2",
					Affinity: 0.5,
				},
				{
					Name:     "store-3",
					Affinity: 0.3,
				},
				{
					Name:     "store-4",
					Affinity: 0.1,
				},
			},
			Expect: []int{52, 26, 15, 5},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			qm := &QueuesManager{
				conf: QueuesManagerConfig{StorageConfig: store.StorageConfig{Backends: test.Backends}},
			}
			a := qm.assignPartitions(100)
			assert.Equal(t, test.Expect, a)
		})
	}
}
