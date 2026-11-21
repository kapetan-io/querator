package service_test

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/anishathalye/porcupine"
)

// --- Operation types for Porcupine ---

const (
	opProduce     = "produce"
	opLease       = "lease"
	opComplete    = "complete"
	opRetry       = "retry"
	opLeaseExpiry = "lease-expiry"
)

type linInput struct {
	op  string
	ids []string // produce: IDs of items produced; complete: IDs to complete; lease: unused
}

type linOutput struct {
	ids []string // lease: IDs returned (in order); produce/complete: nil
	ok  bool     // false only when lease returns empty
}

// --- Sequential state ---

type linState struct {
	queue  []string
	leased map[string]bool
}

func (s linState) clone() linState {
	q := make([]string, len(s.queue))
	copy(q, s.queue)
	l := make(map[string]bool, len(s.leased))
	for k, v := range s.leased {
		l[k] = v
	}
	return linState{queue: q, leased: l}
}

// --- Porcupine model ---

// linearizabilityModel is deterministic, but porcupine.Model's Step returns (bool, interface{}), a
// result shape Tiger rejects (TS-E06). It is written as a NondeterministicModel whose Step returns
// its one next state, or none for an illegal step.
var linearizabilityModel = (&porcupine.NondeterministicModel{
	Init: func() []interface{} {
		return []interface{}{linState{
			queue:  []string{},
			leased: make(map[string]bool),
		}}
	},

	Step: func(state, input, output interface{}) []interface{} {
		s := state.(linState)
		inp := input.(linInput)
		out := output.(linOutput)

		switch inp.op {
		case opProduce:
			next := s.clone()
			next.queue = append(next.queue, inp.ids...)
			return []interface{}{next}

		case opLease:
			if !out.ok {
				if len(s.queue) == 0 {
					return []interface{}{s}
				}
				return nil
			}
			if len(out.ids) > len(s.queue) {
				return nil
			}
			for i, id := range out.ids {
				if s.queue[i] != id {
					return nil
				}
			}
			next := s.clone()
			next.queue = next.queue[len(out.ids):]
			for _, id := range out.ids {
				next.leased[id] = true
			}
			return []interface{}{next}

		case opComplete:
			for _, id := range inp.ids {
				if !s.leased[id] {
					return nil
				}
			}
			next := s.clone()
			for _, id := range inp.ids {
				delete(next.leased, id)
			}
			return []interface{}{next}

		case opRetry, opLeaseExpiry:
			for _, id := range inp.ids {
				if !s.leased[id] {
					return nil
				}
			}
			next := s.clone()
			for _, id := range inp.ids {
				delete(next.leased, id)
			}
			// Per ADR 0022: retried and lease-expired items go to the tail of the queue.
			next.queue = append(next.queue, inp.ids...)
			return []interface{}{next}
		}
		return nil
	},

	Equal: func(s1, s2 interface{}) bool {
		a := s1.(linState)
		b := s2.(linState)
		if !slices.Equal(a.queue, b.queue) {
			return false
		}
		if len(a.leased) != len(b.leased) {
			return false
		}
		for k := range a.leased {
			if !b.leased[k] {
				return false
			}
		}
		return true
	},

	DescribeOperation: func(input, output interface{}) string {
		inp := input.(linInput)
		out := output.(linOutput)
		switch inp.op {
		case opProduce:
			return fmt.Sprintf("produce(%s)", strings.Join(inp.ids, ","))
		case opLease:
			if !out.ok {
				return "lease() -> empty"
			}
			return fmt.Sprintf("lease() -> [%s]", strings.Join(out.ids, ","))
		case opComplete:
			return fmt.Sprintf("complete(%s)", strings.Join(inp.ids, ","))
		case opRetry:
			return fmt.Sprintf("retry(%s)", strings.Join(inp.ids, ","))
		case opLeaseExpiry:
			return fmt.Sprintf("expiry(%s)", strings.Join(inp.ids, ","))
		}
		return "<unknown>"
	},

	DescribeState: func(state interface{}) string {
		s := state.(linState)
		leased := slices.Sorted(maps.Keys(s.leased))
		return fmt.Sprintf("queue=[%s] leased=[%s]",
			strings.Join(s.queue, ","),
			strings.Join(leased, ","))
	},
}).ToModel()
