package internal

import (
	"github.com/kapetan-io/querator/internal/store"
	"github.com/stretchr/testify/assert"
	"testing"
)

// This is a happy path unit test for QueuesManager.assignPartitions() DO NOT ADD MORE TESTS
// TODO: Add functional tests to ensure backend affinity is respected when the public
// interface is exercised and remove this test once those are in place.
func TestAssignPartitions(t *testing.T) {

	for _, test := range []struct {
		Name     string
		Backends []store.PartitionStorage
		Expect   []int
	}{
		{
			Name: "OneBackend",
			Backends: []store.PartitionStorage{
				{
					Name:     "store-1",
					Affinity: 1.0,
				},
			},
			Expect: []int{100},
		},
		{
			Name: "EvenDist",
			Backends: []store.PartitionStorage{
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
			Backends: []store.PartitionStorage{
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
			Backends: []store.PartitionStorage{
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
				conf: QueuesManagerConfig{StorageConfig: store.Config{PartitionStorage: test.Backends}},
			}
			a := qm.assignPartitions(100)
			assert.Equal(t, test.Expect, a)
		})
	}
}
