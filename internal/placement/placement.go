// Package placement decides which storage backend holds each of a queue's partitions, and which
// partition a produce or a lease goes to. It reads queue records and backend affinities and holds
// no storage or network code, so the same saved facts always give the same answer.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/kapetan-io/querator/internal/types"
)

const MaxPartitions = 1_024

var ErrNoBackend = errors.New("no storage backend has an affinity above 0")

type Rebalanced struct {
	Queue   types.QueueInfo
	Changed bool
}

type Backend struct {
	Name     string
	Affinity float64
}

func CanPlace(backends []Backend) bool {
	for _, backend := range backends {
		if backend.Affinity > 0 {
			return true
		}
	}
	return false
}

func CheckBackends(backends []Backend) error {
	var total float64
	seen := make(map[string]struct{}, len(backends))
	for _, backend := range backends {
		if backend.Name == "" {
			return errors.New("partition storage name cannot be empty")
		}
		if _, ok := seen[backend.Name]; ok {
			return fmt.Errorf("duplicate partition storage name '%s'; every partition storage must have a unique name",
				backend.Name)
		}
		seen[backend.Name] = struct{}{}
		if math.IsNaN(backend.Affinity) || math.IsInf(backend.Affinity, 0) {
			return fmt.Errorf("partition storage '%s' affinity must be a finite number, got %v",
				backend.Name, backend.Affinity)
		}
		if backend.Affinity > 0 {
			total += backend.Affinity
		}
	}
	if math.IsInf(total*MaxPartitions, 0) {
		return errors.New("the sum of partition storage affinities is too large; lower the affinities")
	}
	return nil
}

func hashUnit(queue string, backend Backend) float64 {
	sum := sha256.Sum256([]byte(queue + "\x00" + backend.Name))
	value := binary.BigEndian.Uint64(sum[:8]) >> 11
	return (float64(value) + 0.5) / float64(uint64(1)<<53)
}

func Shares(queue string, count int, backends []Backend) (map[string]int, error) {
	if count > MaxPartitions {
		return nil, fmt.Errorf("cannot place %d partitions, more than %d", count, MaxPartitions)
	}
	if err := CheckBackends(backends); err != nil {
		return nil, err
	}
	var taking []Backend
	for _, backend := range backends {
		if backend.Affinity > 0 {
			taking = append(taking, backend)
		}
	}
	if len(taking) == 0 {
		return nil, ErrNoBackend
	}
	sort.Slice(taking, func(i, j int) bool { return taking[i].Name < taking[j].Name })

	var total float64
	units := make(map[string]float64, len(taking))
	for _, backend := range taking {
		total += backend.Affinity
		units[backend.Name] = hashUnit(queue, backend)
	}

	shares := make(map[string]int, len(taking))
	for placed := 1; placed <= count; placed++ {
		var next string
		var best float64
		for _, backend := range taking {
			below := float64(placed)*backend.Affinity/total - float64(shares[backend.Name])
			if below <= 0 {
				continue
			}
			if rank := -below / math.Log(units[backend.Name]); next == "" || rank > best {
				next, best = backend.Name, rank
			}
		}
		shares[next]++
	}
	return shares, nil
}

func Rebalance(queue types.QueueInfo, backends []Backend) (Rebalanced, error) {
	shares, err := Shares(queue.Name, queue.RequestedPartitions, backends)
	if err != nil {
		return Rebalanced{Queue: queue}, err
	}

	partitions := make([]types.PartitionInfo, len(queue.PartitionInfo))
	copy(partitions, queue.PartitionInfo)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].PartitionNum < partitions[j].PartitionNum })

	changed := false
	kept := make(map[string]int, len(shares))
	for i := range partitions {
		if partitions[i].Draining {
			continue
		}
		if kept[partitions[i].StorageName] < shares[partitions[i].StorageName] {
			kept[partitions[i].StorageName]++
			continue
		}
		partitions[i].Draining = true
		changed = true
	}

	names := make([]string, 0, len(shares))
	for name := range shares {
		names = append(names, name)
	}
	sort.Strings(names)

	next := queue.NextPartition
	for _, name := range names {
		for range min(shares[name]-kept[name], MaxPartitions) {
			partitions = append(partitions, types.PartitionInfo{PartitionNum: next, StorageName: name})
			changed = true
			next++
		}
	}

	queue.NextPartition = next
	queue.PartitionInfo = partitions
	return Rebalanced{Queue: queue, Changed: changed}, nil
}

func Create(queue types.QueueInfo, backends []Backend) (types.QueueInfo, error) {
	queue.NextPartition = 0
	queue.PartitionInfo = nil
	placed, err := Rebalance(queue, backends)
	return placed.Queue, err
}

func RemoveEmpty(queue types.QueueInfo, number int) types.QueueInfo {
	partitions := make([]types.PartitionInfo, 0, len(queue.PartitionInfo))
	for _, partition := range queue.PartitionInfo {
		if partition.PartitionNum == number && partition.Draining {
			continue
		}
		partitions = append(partitions, partition)
	}
	queue.PartitionInfo = partitions
	return queue
}

func ProduceTo(queue types.QueueInfo, failing func(types.PartitionInfo) bool) []types.PartitionInfo {
	var taking []types.PartitionInfo
	for _, partition := range queue.PartitionInfo {
		if partition.Draining || failing(partition) {
			continue
		}
		taking = append(taking, partition)
	}
	return taking
}

func ProduceInto(queue types.QueueInfo, unleased func(types.PartitionInfo) int,
	failing func(types.PartitionInfo) bool) (types.PartitionInfo, bool) {
	chosen := -1
	var fewest int
	for i := range queue.PartitionInfo {
		partition := &queue.PartitionInfo[i]
		if partition.Draining || failing(*partition) {
			continue
		}
		count := unleased(*partition)
		if chosen >= 0 && count > fewest {
			continue
		}
		if chosen >= 0 && count == fewest && partition.PartitionNum > queue.PartitionInfo[chosen].PartitionNum {
			continue
		}
		chosen, fewest = i, count
	}
	if chosen < 0 {
		return types.PartitionInfo{}, false
	}
	return queue.PartitionInfo[chosen], true
}

func LeaseFrom(queue types.QueueInfo, unleased func(types.PartitionInfo) int,
	failing func(types.PartitionInfo) bool) (types.PartitionInfo, bool) {
	draining, blocked := firstDraining(queue, unleased, failing)
	if blocked {
		return types.PartitionInfo{}, false
	}
	if draining >= 0 {
		return queue.PartitionInfo[draining], true
	}

	chosen := -1
	var most int
	for i := range queue.PartitionInfo {
		partition := &queue.PartitionInfo[i]
		if partition.Draining || failing(*partition) {
			continue
		}
		count := unleased(*partition)
		if chosen >= 0 && count < most {
			continue
		}
		if chosen >= 0 && count == most && partition.PartitionNum > queue.PartitionInfo[chosen].PartitionNum {
			continue
		}
		chosen, most = i, count
	}
	if chosen < 0 {
		return types.PartitionInfo{}, false
	}
	return queue.PartitionInfo[chosen], true
}

func firstDraining(queue types.QueueInfo, unleased func(types.PartitionInfo) int,
	failing func(types.PartitionInfo) bool) (int, bool) {
	ordered := queue.RequestedPartitions == 1
	chosen := -1
	var blocked bool
	for i := range queue.PartitionInfo {
		partition := &queue.PartitionInfo[i]
		if !partition.Draining {
			continue
		}
		if chosen >= 0 && partition.PartitionNum > queue.PartitionInfo[chosen].PartitionNum {
			continue
		}
		down := failing(*partition)
		if down && ordered {
			chosen, blocked = i, true
			continue
		}
		if !down && unleased(*partition) > 0 {
			chosen, blocked = i, false
		}
	}
	return chosen, blocked
}
