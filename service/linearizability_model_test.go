package service_test

import (
	"fmt"
	"slices"
	"strings"

	"github.com/anishathalye/porcupine"
)

// --- Operation types for Porcupine ---

const (
	opProduce  = "produce"
	opLease    = "lease"
	opComplete = "complete"
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

var linearizabilityModel = porcupine.Model{
	Init: func() interface{} {
		return linState{
			queue:  []string{},
			leased: make(map[string]bool),
		}
	},

	Step: func(state, input, output interface{}) (bool, interface{}) {
		s := state.(linState)
		inp := input.(linInput)
		out := output.(linOutput)

		switch inp.op {
		case opProduce:
			next := s.clone()
			next.queue = append(next.queue, inp.ids...)
			return true, next

		case opLease:
			if !out.ok {
				return len(s.queue) == 0, s
			}
			if len(out.ids) > len(s.queue) {
				return false, s
			}
			for i, id := range out.ids {
				if s.queue[i] != id {
					return false, s
				}
			}
			next := s.clone()
			next.queue = next.queue[len(out.ids):]
			for _, id := range out.ids {
				next.leased[id] = true
			}
			return true, next

		case opComplete:
			for _, id := range inp.ids {
				if !s.leased[id] {
					return false, s
				}
			}
			next := s.clone()
			for _, id := range inp.ids {
				delete(next.leased, id)
			}
			return true, next
		}
		return false, s
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
		}
		return "<unknown>"
	},

	DescribeState: func(state interface{}) string {
		s := state.(linState)
		leased := make([]string, 0, len(s.leased))
		for k := range s.leased {
			leased = append(leased, k)
		}
		return fmt.Sprintf("queue=[%s] leased=[%s]",
			strings.Join(s.queue, ","),
			strings.Join(leased, ","))
	},
}
