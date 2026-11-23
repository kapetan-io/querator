package placement_test

import (
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/kapetan-io/querator/internal/placement"
	"github.com/kapetan-io/querator/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type held struct {
	item int
	due  int
}

const fuzzSteps = 150

const delayMax = 20

const leaseMax = 100_000

const _ = uint(leaseMax - fuzzSteps)

const tickMax = 1_000

const _ = uint(tickMax - delayMax)

type world struct {
	backends []placement.Backend
	down     map[string]bool
	queue    types.QueueInfo
	items    map[int][]int
	leased   map[int][]int
	waiting  map[int][]held
	used     map[int]bool
	delayed  map[int]bool
	first    map[int]bool
	retried  map[int]int
	consumed []int
	disorder string
	produced int
	created  int
	jumped   int
	clock    int
	last     int
}

func newWorld(t *testing.T, name string, count int, backends []placement.Backend) *world {
	t.Helper()
	w := &world{
		backends: append([]placement.Backend{}, backends...),
		down:     map[string]bool{},
		items:    map[int][]int{},
		leased:   map[int][]int{},
		waiting:  map[int][]held{},
		used:     map[int]bool{},
		delayed:  map[int]bool{},
		first:    map[int]bool{},
		retried:  map[int]int{},
		last:     -1,
	}
	queue, err := placement.Create(types.QueueInfo{Name: name, RequestedPartitions: count}, w.backends)
	require.NoError(t, err)
	w.adopt(t, queue)
	return w
}

func (w *world) failing(p types.PartitionInfo) bool { return w.down[p.StorageName] }
func (w *world) unleased(p types.PartitionInfo) int { return len(w.items[p.PartitionNum]) }

func (w *world) adopt(t *testing.T, after types.QueueInfo) {
	t.Helper()
	known := map[int]types.PartitionInfo{}
	for _, p := range w.queue.PartitionInfo {
		known[p.PartitionNum] = p
	}
	for _, p := range after.PartitionInfo {
		old, existed := known[p.PartitionNum]
		if !existed {
			require.False(t, w.used[p.PartitionNum], "partition number %d was used before", p.PartitionNum)
			w.used[p.PartitionNum] = true
			w.created++
			continue
		}
		require.Equal(t, old.StorageName, p.StorageName, "partition %d changed backend", p.PartitionNum)
		if old.Draining {
			require.True(t, p.Draining, "draining partition %d took produce again", p.PartitionNum)
		}
	}
	w.queue = after
}

func (w *world) setAffinity(name string, affinity float64) {
	for i := range w.backends {
		if w.backends[i].Name == name {
			w.backends[i].Affinity = affinity
			return
		}
	}
	w.backends = append(w.backends, placement.Backend{Name: name, Affinity: affinity})
}

func (w *world) rebalance(t *testing.T) {
	t.Helper()
	result, err := placement.Rebalance(w.queue, w.backends)
	after := result.Queue
	if err != nil {
		require.ErrorIs(t, err, placement.ErrNoBackend)
		return
	}

	affinity := map[string]float64{}
	for _, b := range w.backends {
		affinity[b.Name] = b.Affinity
	}
	live := 0
	for _, p := range after.PartitionInfo {
		if p.Draining {
			continue
		}
		live++
		require.Greater(t, affinity[p.StorageName], 0.0, "partition %d takes produce on a backend with no share", p.PartitionNum)
	}
	require.Equal(t, after.RequestedPartitions, live)

	second, err := placement.Rebalance(after, w.backends)
	require.NoError(t, err)
	require.False(t, second.Changed, "a second rebalance changed the queue")
	require.Equal(t, after, second.Queue)

	reversed := make([]placement.Backend, len(w.backends))
	for i, b := range w.backends {
		reversed[len(w.backends)-1-i] = b
	}
	other, err := placement.Rebalance(w.queue, reversed)
	require.NoError(t, err)
	require.Equal(t, after, other.Queue, "backend order changed the result")

	w.adopt(t, after)
}

func (w *world) produce(rng *rand.Rand) { w.produceIn(rng, 0) }

func (w *world) produceIn(rng *rand.Rand, delay int) {
	taking := placement.ProduceTo(w.queue, w.failing)
	if len(taking) == 0 {
		return
	}
	p := taking[rng.Intn(len(taking))]
	if delay > 0 {
		w.delayed[w.produced] = true
		w.waiting[p.PartitionNum] = append(w.waiting[p.PartitionNum], held{item: w.produced, due: w.clock + delay})
	} else {
		w.items[p.PartitionNum] = append(w.items[p.PartitionNum], w.produced)
	}
	w.produced++
}

func (w *world) observe(item int) {
	if !w.first[item] {
		w.first[item] = true
		if w.delayed[item] {
			return
		}
		if item < w.last && w.disorder == "" {
			w.disorder = fmt.Sprintf("item %d was first leased after item %d", item, w.last)
		}
		if item > w.last {
			w.last = item
		}
		return
	}
	mark, ok := w.retried[item]
	if !ok {
		return
	}
	delete(w.retried, item)
	for older := 0; older < mark; older++ {
		if !w.delayed[older] && !w.first[older] {
			w.jumped++
			return
		}
	}
}

func (w *world) take() (int, bool) {
	p, ok := placement.LeaseFrom(w.queue, w.unleased, w.failing)
	if !ok || len(w.items[p.PartitionNum]) == 0 {
		return 0, false
	}
	item := w.items[p.PartitionNum][0]
	w.items[p.PartitionNum] = w.items[p.PartitionNum][1:]
	w.leased[p.PartitionNum] = append(w.leased[p.PartitionNum], item)
	w.observe(item)
	return p.PartitionNum, true
}

func (w *world) lease() bool {
	number, ok := w.take()
	if !ok {
		return false
	}
	last := len(w.leased[number]) - 1
	w.consumed = append(w.consumed, w.leased[number][last])
	w.leased[number] = w.leased[number][:last]
	return true
}

func (w *world) finish(rng *rand.Rand, outcome string) bool {
	var holding []int
	for _, p := range w.queue.PartitionInfo {
		if len(w.leased[p.PartitionNum]) > 0 && !w.down[p.StorageName] {
			holding = append(holding, p.PartitionNum)
		}
	}
	if len(holding) == 0 {
		return false
	}
	number := holding[rng.Intn(len(holding))]
	index := rng.Intn(len(w.leased[number]))
	item := w.leased[number][index]
	w.leased[number] = append(w.leased[number][:index], w.leased[number][index+1:]...)
	switch outcome {
	case "complete":
		w.consumed = append(w.consumed, item)
	case "retry":
		w.items[number] = append(w.items[number], item)
		w.retried[item] = w.produced
	case "retryLater":
		w.waiting[number] = append(w.waiting[number], held{item: item, due: w.clock + 1 + rng.Intn(delayMax)})
	}
	return true
}

func (w *world) tick() {
	w.clock++
	for _, p := range w.queue.PartitionInfo {
		if w.down[p.StorageName] {
			continue
		}
		var still []held
		for _, h := range w.waiting[p.PartitionNum] {
			if h.due <= w.clock {
				w.items[p.PartitionNum] = append(w.items[p.PartitionNum], h.item)
				continue
			}
			still = append(still, h)
		}
		w.waiting[p.PartitionNum] = still
	}
}

func (w *world) holds(number int) int {
	return len(w.items[number]) + len(w.leased[number]) + len(w.waiting[number])
}

func (w *world) audit(t *testing.T) {
	t.Helper()
	present := map[int]bool{}
	for _, p := range w.queue.PartitionInfo {
		present[p.PartitionNum] = true
	}
	for _, number := range slices.Sorted(maps.Keys(w.used)) {
		if !present[number] {
			require.Zero(t, w.holds(number), "partition %d was removed while it held items", number)
		}
	}
}

func (w *world) removeEmpty() {
	for _, p := range w.queue.PartitionInfo {
		if p.Draining && !w.down[p.StorageName] && w.holds(p.PartitionNum) == 0 {
			w.queue = placement.RemoveEmpty(w.queue, p.PartitionNum)
		}
	}
}

func (w *world) settle(t *testing.T) {
	t.Helper()
	w.down = map[string]bool{}
	taking := false
	for _, b := range w.backends {
		taking = taking || b.Affinity > 0
	}
	if !taking {
		w.setAffinity(w.backends[0].Name, 1)
	}
	w.rebalance(t)
	for range tickMax {
		for _, number := range slices.Sorted(maps.Keys(w.leased)) {
			w.consumed = append(w.consumed, w.leased[number]...)
			w.leased[number] = nil
		}
		w.leaseAll(t)
		pending := 0
		for _, waiting := range w.waiting {
			pending += len(waiting)
		}
		if pending == 0 {
			w.removeEmpty()
			w.audit(t)
			return
		}
		w.tick()
	}
	require.Fail(t, "items still waiting after the tick limit")
}

func (w *world) leaseAll(t *testing.T) {
	t.Helper()
	for range leaseMax {
		if !w.lease() {
			return
		}
	}
	require.Fail(t, "lease never ran out of items")
}

func (w *world) layout() string {
	live := map[string][]int{}
	var draining []int
	for _, p := range w.queue.PartitionInfo {
		if p.Draining {
			draining = append(draining, p.PartitionNum)
			continue
		}
		live[p.StorageName] = append(live[p.StorageName], p.PartitionNum)
	}
	names := make([]string, 0, len(live))
	for name := range live {
		names = append(names, name)
	}
	sort.Strings(names)
	var parts []string
	for _, name := range names {
		sort.Ints(live[name])
		parts = append(parts, fmt.Sprintf("%s:%v", name, live[name]))
	}
	sort.Ints(draining)
	if len(draining) > 0 {
		parts = append(parts, fmt.Sprintf("draining:%v", draining))
	}
	return strings.Join(parts, " ")
}

func liveOn(queue types.QueueInfo) map[string]int {
	out := map[string]int{}
	for _, p := range queue.PartitionInfo {
		if !p.Draining {
			out[p.StorageName]++
		}
	}
	return out
}

func equal(names ...string) []placement.Backend {
	out := make([]placement.Backend, 0, len(names))
	for _, name := range names {
		out = append(out, placement.Backend{Name: name, Affinity: 1})
	}
	return out
}

func TestPlacement(t *testing.T) {
	t.Run("Walkthrough", func(t *testing.T) {
		w := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		step := func(label string) {
			w.rebalance(t)
			during := w.layout()
			w.settle(t)
			t.Logf("%-34s %-44s then %s", label, during, w.layout())
		}
		t.Logf("%-34s %s", "create with 4", w.layout())

		w.queue.RequestedPartitions = 6
		step("requested_partitions 6")
		w.queue.RequestedPartitions = 4
		step("requested_partitions 4")
		w.setAffinity("pg-2", 0)
		step("pg-2 affinity 0")
		assert.Equal(t, map[string]int{"pg-1": 4}, liveOn(w.queue))
		w.setAffinity("pg-2", 1)
		step("pg-2 affinity 1")
		assert.Equal(t, map[string]int{"pg-1": 2, "pg-2": 2}, liveOn(w.queue))
		w.setAffinity("pg-3", 1)
		step("add pg-3 affinity 1")
		w.setAffinity("pg-3", 0)
		step("pg-3 affinity 0")
		assert.Equal(t, map[string]int{"pg-1": 2, "pg-2": 2}, liveOn(w.queue))
	})

	t.Run("PageExamples", func(t *testing.T) {
		w := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		assert.Equal(t, "pg-1:[0 1] pg-2:[2 3]", w.layout())

		w.setAffinity("pg-2", 0)
		w.rebalance(t)
		assert.Equal(t, "pg-1:[0 1 4 5] draining:[2 3]", w.layout())
		w.settle(t)

		w.setAffinity("pg-2", 1)
		w.rebalance(t)
		assert.Equal(t, "pg-1:[0 1] pg-2:[6 7] draining:[4 5]", w.layout())
		w.settle(t)

		w.setAffinity("pg-3", 0)
		w.rebalance(t)
		assert.Equal(t, "pg-1:[0 1] pg-2:[6 7]", w.layout())
		w.setAffinity("pg-3", 1)
		w.rebalance(t)
		assert.Equal(t, "pg-1:[0] pg-2:[6] pg-3:[8 9] draining:[1 7]", w.layout())

		half := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		half.setAffinity("pg-2", 0)
		half.rebalance(t)
		half.setAffinity("pg-2", 1)
		half.rebalance(t)
		assert.Equal(t, "pg-1:[0 1] pg-2:[6 7] draining:[2 3 4 5]", half.layout())

		season := &world{
			backends: []placement.Backend{{Name: "pg-1", Affinity: 1}, {Name: "bf-1", Affinity: 0}},
			down:     map[string]bool{},
			items:    map[int][]int{},
			used:     map[int]bool{},
			queue: types.QueueInfo{
				Name:                "orders",
				RequestedPartitions: 4,
				NextPartition:       8,
				PartitionInfo: []types.PartitionInfo{
					{PartitionNum: 0, StorageName: "pg-1"}, {PartitionNum: 1, StorageName: "pg-1"},
					{PartitionNum: 2, StorageName: "pg-1"}, {PartitionNum: 3, StorageName: "pg-1"},
					{PartitionNum: 4, StorageName: "bf-1"}, {PartitionNum: 5, StorageName: "bf-1"},
					{PartitionNum: 6, StorageName: "bf-1"}, {PartitionNum: 7, StorageName: "bf-1"},
				},
			},
		}
		season.rebalance(t)
		assert.Equal(t, "pg-1:[0 1 2 3] draining:[4 5 6 7]", season.layout())
		assert.Zero(t, season.created)
	})

	t.Run("SmallQueuesFollowAffinity", func(t *testing.T) {
		backends := []placement.Backend{{Name: "pg-1", Affinity: 1}, {Name: "pg-2", Affinity: 3}}
		for _, count := range []int{1, 2, 3} {
			small := 0
			for i := 0; i < 1000; i++ {
				shares, err := placement.Shares(fmt.Sprintf("queue-%04d", i), count, backends)
				require.NoError(t, err)
				small += shares["pg-1"]
			}
			t.Logf("count %d: pg-1 holds %d of %d partitions, a quarter is %d", count, small, 1000*count, 250*count)
			assert.InDelta(t, 250*count, small, 40)
		}
	})

	t.Run("CountChangeOnlyDrainsOrOnlyCreates", func(t *testing.T) {
		for _, affinities := range [][]float64{{1, 1}, {1, 3}, {1, 3, 3}, {1, 2, 5}, {0.25, 1, 1}} {
			backends := make([]placement.Backend, len(affinities))
			for i, affinity := range affinities {
				backends[i] = placement.Backend{Name: fmt.Sprintf("backend-%d", i), Affinity: affinity}
			}
			for i := 0; i < 300; i++ {
				name := fmt.Sprintf("queue-%04d", i)
				for count := 1; count < 32; count++ {
					small, err := placement.Rebalance(types.QueueInfo{Name: name, RequestedPartitions: count}, backends)
					require.NoError(t, err)
					large, err := placement.Rebalance(types.QueueInfo{Name: name, RequestedPartitions: count + 1}, backends)
					require.NoError(t, err)

					grow := small.Queue
					grow.RequestedPartitions = count + 1
					grown, err := placement.Rebalance(grow, backends)
					require.NoError(t, err)
					require.Len(t, grown.Queue.PartitionInfo, count+1, "%s grew from %d and drained a partition", name, count)

					shrink := large.Queue
					shrink.RequestedPartitions = count
					shrunk, err := placement.Rebalance(shrink, backends)
					require.NoError(t, err)
					require.Len(t, shrunk.Queue.PartitionInfo, count+1, "%s shrank from %d and created a partition", name, count+1)
				}
			}
		}
	})

	t.Run("LeaseTakesDrainingThenFullest", func(t *testing.T) {
		queue := types.QueueInfo{Name: "orders", RequestedPartitions: 3, PartitionInfo: []types.PartitionInfo{
			{PartitionNum: 0, StorageName: "pg-1"},
			{PartitionNum: 1, StorageName: "pg-1"},
			{PartitionNum: 2, StorageName: "pg-2", Draining: true},
			{PartitionNum: 3, StorageName: "pg-2", Draining: true},
			{PartitionNum: 4, StorageName: "pg-1"},
		}}
		held := map[int]int{0: 2, 1: 9, 2: 0, 3: 1, 4: 9}
		unleased := func(p types.PartitionInfo) int { return held[p.PartitionNum] }
		healthy := func(types.PartitionInfo) bool { return false }

		p, ok := placement.LeaseFrom(queue, unleased, healthy)
		require.True(t, ok)
		assert.Equal(t, 3, p.PartitionNum, "the lowest-numbered draining partition that holds unleased items comes first")

		held[3] = 0
		p, ok = placement.LeaseFrom(queue, unleased, healthy)
		require.True(t, ok)
		assert.Equal(t, 1, p.PartitionNum, "then the available partition with the most unleased items, the lower number on a tie")

		p, ok = placement.LeaseFrom(queue, unleased, func(p types.PartitionInfo) bool { return p.PartitionNum == 1 })
		require.True(t, ok)
		assert.Equal(t, 4, p.PartitionNum, "a failing available partition is passed over")

		p, ok = placement.LeaseFrom(queue, func(types.PartitionInfo) int { return 0 }, healthy)
		require.True(t, ok)
		assert.Equal(t, 0, p.PartitionNum, "with every count at 0 the lease still goes to an available partition, since a count can be wrong")

		_, ok = placement.LeaseFrom(queue, unleased, func(types.PartitionInfo) bool { return true })
		assert.False(t, ok)
	})

	t.Run("SavedRequestsWaitForRebalance", func(t *testing.T) {
		w := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		before := w.layout()
		w.queue.RequestedPartitions = 8
		w.setAffinity("pg-2", 0)
		w.setAffinity("pg-3", 1)
		assert.Equal(t, before, w.layout())
		assert.Len(t, placement.ProduceTo(w.queue, w.failing), 4)
	})

	t.Run("TwoRequestsOneRebalance", func(t *testing.T) {
		apart := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		apart.created = 0
		apart.setAffinity("pg-2", 0)
		apart.settle(t)
		apart.setAffinity("pg-3", 1)
		apart.settle(t)

		together := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		together.created = 0
		together.setAffinity("pg-2", 0)
		together.setAffinity("pg-3", 1)
		together.settle(t)

		t.Logf("two rebalances created %d partitions, one rebalance created %d", apart.created, together.created)
		assert.Equal(t, 4, apart.created)
		assert.Equal(t, 2, together.created)
		assert.Equal(t, liveOn(apart.queue), liveOn(together.queue))
	})

	t.Run("OnePartitionQueueKeepsOrder", func(t *testing.T) {
		w := newWorld(t, "audit", 1, equal("pg-1"))
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 3; i++ {
			w.produce(rng)
		}
		w.setAffinity("pg-1", 0)
		w.setAffinity("pg-2", 1)
		w.rebalance(t)
		t.Logf("moving: %s", w.layout())
		for i := 0; i < 3; i++ {
			w.produce(rng)
		}

		w.down["pg-1"] = true
		assert.False(t, w.lease(), "lease passed a failing draining partition")
		w.down["pg-1"] = false
		w.leaseAll(t)
		w.removeEmpty()
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, w.consumed)
		t.Logf("moved:  %s", w.layout())
	})

	t.Run("RetryAndScheduledWhileMoving", func(t *testing.T) {
		rng := rand.New(rand.NewSource(1))
		w := newWorld(t, "audit", 1, equal("pg-1"))
		w.produce(rng)
		w.produceIn(rng, 10)
		w.setAffinity("pg-1", 0)
		w.setAffinity("pg-2", 1)
		w.rebalance(t)
		w.produce(rng)
		require.Equal(t, "pg-2:[1] draining:[0]", w.layout())

		number, ok := w.take()
		require.True(t, ok)
		assert.Equal(t, 0, number)
		require.True(t, w.finish(rng, "retry"))
		number, ok = w.take()
		require.True(t, ok)
		assert.Equal(t, 0, number, "the retried item waits in the old partition, ahead of the newer item")
		assert.Equal(t, 1, w.jumped)
		require.True(t, w.finish(rng, "complete"))

		w.removeEmpty()
		assert.Equal(t, "pg-2:[1] draining:[0]", w.layout(), "the scheduled item keeps the old partition")
		number, ok = w.take()
		require.True(t, ok)
		assert.Equal(t, 1, number)
		for i := 0; i < 10; i++ {
			w.tick()
		}
		number, ok = w.take()
		require.True(t, ok)
		assert.Equal(t, 0, number, "the scheduled item came due in the old partition")
		w.settle(t)
		assert.Equal(t, "pg-2:[1]", w.layout())
		assert.Len(t, w.consumed, 3)
	})

	t.Run("NoBackendTakesPartitions", func(t *testing.T) {
		w := newWorld(t, "orders", 4, equal("pg-1", "pg-2"))
		before := w.queue
		w.setAffinity("pg-1", 0)
		w.setAffinity("pg-2", 0)
		result, err := placement.Rebalance(w.queue, w.backends)
		require.ErrorIs(t, err, placement.ErrNoBackend)
		assert.False(t, result.Changed)
		assert.Equal(t, before, w.queue)
	})

	t.Run("Fuzz", func(t *testing.T) {
		for _, mode := range []string{"OnePartition", "AnyCount"} {
			t.Run(mode, func(t *testing.T) {
				const runs, steps = 2000, fuzzSteps
				stalled, retries, scheduled, jumped := 0, 0, 0, 0
				for seed := int64(0); seed < runs; seed++ {
					rng := rand.New(rand.NewSource(seed))
					count := 1
					if mode == "AnyCount" {
						count = 1 + rng.Intn(5)
					}
					w := newWorld(t, fmt.Sprintf("queue-%d", seed), count, equal("pg-1", "pg-2"))
					for i := 0; i < steps; i++ {
						switch op := rng.Float64(); {
						case op < 0.26:
							w.produce(rng)
						case op < 0.31:
							w.produceIn(rng, 1+rng.Intn(delayMax))
						case op < 0.52:
							if _, ok := w.take(); !ok && w.produced > len(w.consumed) {
								stalled++
							}
						case op < 0.65:
							w.finish(rng, "complete")
						case op < 0.71:
							if w.finish(rng, "retry") {
								retries++
							}
						case op < 0.74:
							if w.finish(rng, "retryLater") {
								retries++
							}
						case op < 0.81:
							b := w.backends[rng.Intn(len(w.backends))]
							w.setAffinity(b.Name, []float64{0, 0.5, 1, 2, 3}[rng.Intn(5)])
						case op < 0.84:
							if len(w.backends) < 5 {
								w.setAffinity(fmt.Sprintf("pg-%d", len(w.backends)+1), 1)
							}
						case op < 0.88:
							if mode == "AnyCount" {
								w.queue.RequestedPartitions = 1 + rng.Intn(6)
							}
						case op < 0.95:
							w.rebalance(t)
						default:
							b := w.backends[rng.Intn(len(w.backends))]
							w.down[b.Name] = !w.down[b.Name]
						}
						w.tick()
						w.removeEmpty()
						w.audit(t)
					}
					w.settle(t)
					jumped += w.jumped
					scheduled += len(w.delayed)

					require.Len(t, w.consumed, w.produced, "seed %d lost or repeated an item", seed)
					seen := map[int]bool{}
					for _, item := range w.consumed {
						require.False(t, seen[item], "seed %d completed item %d twice", seed, item)
						seen[item] = true
					}
					if mode == "OnePartition" {
						require.Empty(t, w.disorder, "seed %d broke order", seed)
					}
					for _, p := range w.queue.PartitionInfo {
						require.False(t, p.Draining, "seed %d left partition %d draining", seed, p.PartitionNum)
					}
					shares, err := placement.Shares(w.queue.Name, w.queue.RequestedPartitions, w.backends)
					require.NoError(t, err)
					for _, name := range slices.Sorted(maps.Keys(shares)) {
						require.Equal(t, shares[name], liveOn(w.queue)[name], "seed %d backend %s", seed, name)
					}
				}
				t.Logf("%d runs of %d steps; %d retries, %d scheduled items; lease waited %d times; a retried item came out ahead of an older one %d times", runs, steps, retries, scheduled, stalled, jumped)
			})
		}
	})

	t.Run("Fleet", func(t *testing.T) {
		const total = 1000
		build := func() []types.QueueInfo {
			queues := make([]types.QueueInfo, total)
			for i := range queues {
				queues[i] = types.QueueInfo{
					Name:                fmt.Sprintf("queue-%04d", i),
					PartitionInfo:       []types.PartitionInfo{{PartitionNum: 0, StorageName: "pg-1"}},
					RequestedPartitions: 1,
					NextPartition:       1,
				}
			}
			return queues
		}
		home := func(queue types.QueueInfo) string {
			for _, p := range queue.PartitionInfo {
				if !p.Draining {
					return p.StorageName
				}
			}
			return ""
		}
		type outcome struct {
			moved  map[int]bool
			spread map[string]int
		}
		pass := func(queues []types.QueueInfo, backends []placement.Backend) outcome {
			moved := map[int]bool{}
			spread := map[string]int{}
			for i := range queues {
				from := home(queues[i])
				result, err := placement.Rebalance(queues[i], backends)
				require.NoError(t, err)
				after := result.Queue
				for _, p := range after.PartitionInfo {
					if p.Draining {
						after = placement.RemoveEmpty(after, p.PartitionNum)
					}
				}
				queues[i] = after
				if result.Changed {
					moved[i] = true
					require.NotEqual(t, from, home(after))
				}
				spread[home(after)]++
			}
			return outcome{moved: moved, spread: spread}
		}
		with := func(affinities ...float64) []placement.Backend {
			out := make([]placement.Backend, len(affinities))
			for i, affinity := range affinities {
				out[i] = placement.Backend{Name: fmt.Sprintf("pg-%d", i+1), Affinity: affinity}
			}
			return out
		}

		queues := build()
		first := pass(queues, with(1, 1))
		t.Logf("first rebalance after upgrade   moved %4d  %v", len(first.moved), first.spread)
		assert.InDelta(t, total/2, len(first.moved), total/10)
		again := pass(queues, with(1, 1))
		assert.Empty(t, again.moved)

		staged := map[int]bool{}
		for _, affinity := range []float64{0.25, 0.5, 1} {
			step := pass(queues, with(1, 1, affinity))
			t.Logf("pg-3 affinity %-4v              moved %4d  %v", affinity, len(step.moved), step.spread)
			for _, i := range slices.Sorted(maps.Keys(step.moved)) {
				require.False(t, staged[i], "queue %d moved twice while pg-3 came in", i)
				require.Equal(t, "pg-3", home(queues[i]))
				staged[i] = true
			}
		}
		direct := build()
		pass(direct, with(1, 1))
		atOnce := pass(direct, with(1, 1, 1))
		t.Logf("pg-3 affinity 1 in one step     moved %4d", len(atOnce.moved))
		assert.Equal(t, len(atOnce.moved), len(staged))
		assert.InDelta(t, total/3, len(staged), total/10)

		before := make([]string, total)
		for i := range queues {
			before[i] = home(queues[i])
		}
		out := pass(queues, with(1, 0, 1))
		t.Logf("pg-2 affinity 0                 moved %4d  %v", len(out.moved), out.spread)
		back := pass(queues, with(1, 1, 1))
		t.Logf("pg-2 affinity 1                 moved %4d  %v", len(back.moved), back.spread)
		assert.Equal(t, len(out.moved), len(back.moved))
		for i := range queues {
			require.Equal(t, before[i], home(queues[i]))
		}

		heavy := pass(queues, with(3, 1, 1))
		t.Logf("pg-1 affinity 3                 moved %4d  %v", len(heavy.moved), heavy.spread)
		assert.InDelta(t, total*3/5, heavy.spread["pg-1"], total/10)

		emptied := pass(queues, with(0, 1, 1))
		t.Logf("pg-1 affinity 0                 moved %4d  %v", len(emptied.moved), emptied.spread)
		assert.Zero(t, emptied.spread["pg-1"])
	})
}
