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

func hashUnit(queue string, backend Backend) float64 {
	sum := sha256.Sum256([]byte(queue + "\x00" + backend.Name))
	value := binary.BigEndian.Uint64(sum[:8]) >> 11
	return (float64(value) + 0.5) / float64(uint64(1)<<53)
}

func Shares(queue string, count int, backends []Backend) (map[string]int, error) {
	if count > MaxPartitions {
		return nil, fmt.Errorf("cannot place %d partitions, more than %d", count, MaxPartitions)
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
	var emptiest types.PartitionInfo
	var found bool
	var fewest int
	for _, partition := range ProduceTo(queue, failing) {
		count := unleased(partition)
		if found && count > fewest {
			continue
		}
		if found && count == fewest && partition.PartitionNum > emptiest.PartitionNum {
			continue
		}
		emptiest, fewest, found = partition, count, true
	}
	return emptiest, found
}

func LeaseFrom(queue types.QueueInfo, unleased func(types.PartitionInfo) int,
	failing func(types.PartitionInfo) bool) (types.PartitionInfo, bool) {
	partitions := make([]types.PartitionInfo, len(queue.PartitionInfo))
	copy(partitions, queue.PartitionInfo)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].PartitionNum < partitions[j].PartitionNum })

	ordered := queue.RequestedPartitions == 1
	for _, partition := range partitions {
		if !partition.Draining {
			continue
		}
		if failing(partition) {
			if ordered {
				return types.PartitionInfo{}, false
			}
			continue
		}
		if unleased(partition) > 0 {
			return partition, true
		}
	}

	var fullest types.PartitionInfo
	var found bool
	var most int
	for _, partition := range partitions {
		if partition.Draining || failing(partition) {
			continue
		}
		if count := unleased(partition); count > most || !found {
			fullest, most, found = partition, count, true
		}
	}
	return fullest, found
}
