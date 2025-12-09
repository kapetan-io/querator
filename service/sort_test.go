package service_test

import (
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ----------------------------------
// Sort Benchmarks
// ----------------------------------
//
// goos: darwin
// goarch: arm64
// pkg: github.com/kapetan-io/querator
// BenchmarkSort
// BenchmarkSort/MethodCount-10         	 2635221	       429.0 ns/op
// BenchmarkSort/BubbleSort()-10        	 8068135	       148.5 ns/op
// BenchmarkSort/sort.Ints()-10         	17787538	        67.56 ns/op
// BenchmarkSort/slices.Sort()-10       	17902387	        66.97 ns/op
// BenchmarkSort/slices.SortFunc()-10   	12322352	        92.27 ns/op
// BenchmarkSort/BubbleSortPartition()-10  	12902479	        97.53 ns/op
//
// These benchmarks help to decide which design to use when sorting partitions
// for opportunistic distribution. Using the `Partition` struct appears to be
// the most efficient method. Sort performance is important as the current
// implementation calls sort after each request is assigned a partition.

type BenchPartition struct {
	Count          int64
	BenchPartition MethodCount
}

func BubbleSort(n []int) {
	for i := 0; i < len(n); i++ {
		for j := 0; j < len(n); j++ {
			if n[i] < n[j] {
				n[i], n[j] = n[j], n[i]
			}
		}
	}
}

func BubbleSortPartition(n []BenchPartition) {
	for i := 0; i < len(n); i++ {
		for j := 0; j < len(n); j++ {
			if n[i].Count < n[j].Count {
				n[i], n[j] = n[j], n[i]
			}
		}
	}
}

type MethodCount interface {
	MethodCount() int64
	Failures() int64
}

type method struct {
	count    int64
	failures int64
}

func (m *method) MethodCount() int64 {
	return m.count
}

func (m *method) Failures() int64 {
	return m.failures
}

// TestSort ensures sorts items with failures last
func TestSort(t *testing.T) {
	o := []MethodCount{
		&method{count: -1},
		&method{count: 2},
		&method{count: 0},
		&method{count: 1},
		&method{count: 43},
		&method{count: 14},
		&method{count: 85},
		&method{count: 16},
		&method{count: 7, failures: 10},
		&method{count: 58},
		&method{count: 9},
		&method{count: 10},
		&method{count: 21},
		&method{count: 42},
		&method{count: 73},
		&method{count: 4},
		&method{count: 5},
	}

	slices.SortFunc(o, func(a, b MethodCount) int {
		if a.Failures() < b.Failures() {
			return -1
		}

		if a.MethodCount() < b.MethodCount() {
			return -1
		}
		if a.MethodCount() > b.MethodCount() {
			return +1
		}
		return 0
	})

	assert.Equal(t, int64(7), o[len(o)-1].MethodCount())
}

func BenchmarkSort(b *testing.B) {
	b.Run("MethodCount", func(b *testing.B) {

		for i := 0; i < b.N; i++ {
			o := []MethodCount{
				&method{count: -1},
				&method{count: 2},
				&method{count: 0},
				&method{count: 1},
				&method{count: 43},
				&method{count: 14},
				&method{count: 85},
				&method{count: 16},
				&method{count: 7},
				&method{count: 58},
				&method{count: 9},
				&method{count: 10},
				&method{count: 21},
				&method{count: 42},
				&method{count: 73},
				&method{count: 4},
				&method{count: 5},
			}

			slices.SortFunc(o, func(a, b MethodCount) int {
				if a.Failures() < b.Failures() {
					return -1
				}

				if a.MethodCount() < b.MethodCount() {
					return -1
				}
				if a.MethodCount() > b.MethodCount() {
					return +1
				}
				return 0
			})
		}
	})

	b.Run("BubbleSort()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			a := []int{-1, 2, 0, 1, 43, 14, 85, 16, 7, 58, 9, 10, 21, 42, 73, 4, 5}

			BubbleSort(a)
		}
	})

	b.Run("sort.Ints()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			a := []int{-1, 2, 0, 1, 43, 14, 85, 16, 7, 58, 9, 10, 21, 42, 73, 4, 5}

			sort.Ints(a)
		}
	})

	b.Run("slices.Sort()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			a := []int{-1, 2, 0, 1, 43, 14, 85, 16, 7, 58, 9, 10, 21, 42, 73, 4, 5}

			slices.Sort(a)
		}
	})

	b.Run("slices.SortFunc()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			a := []BenchPartition{
				{Count: 0, BenchPartition: &method{count: 0}},
				{Count: 12, BenchPartition: &method{count: 0}},
				{Count: 5, BenchPartition: &method{count: 0}},
				{Count: 44, BenchPartition: &method{count: 0}},
				{Count: 85, BenchPartition: &method{count: 0}},
				{Count: 96, BenchPartition: &method{count: 0}},
				{Count: 27, BenchPartition: &method{count: 0}},
				{Count: 18, BenchPartition: &method{count: 0}},
				{Count: 49, BenchPartition: &method{count: 0}},
				{Count: 1, BenchPartition: &method{count: 0}},
			}

			slices.SortFunc(a, func(a, b BenchPartition) int {
				if a.Count < b.Count {
					return -1
				}
				if a.Count > b.Count {
					return +1
				}
				return 0
			})
		}
	})

	b.Run("BubbleSortPartition()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			a := []BenchPartition{
				{Count: 0, BenchPartition: &method{count: 0}},
				{Count: 12, BenchPartition: &method{count: 0}},
				{Count: 5, BenchPartition: &method{count: 0}},
				{Count: 44, BenchPartition: &method{count: 0}},
				{Count: 85, BenchPartition: &method{count: 0}},
				{Count: 96, BenchPartition: &method{count: 0}},
				{Count: 27, BenchPartition: &method{count: 0}},
				{Count: 18, BenchPartition: &method{count: 0}},
				{Count: 49, BenchPartition: &method{count: 0}},
				{Count: 1, BenchPartition: &method{count: 0}},
			}

			BubbleSortPartition(a)
		}
	})
}
