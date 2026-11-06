package dag

import (
	"fmt"
	"sort"

	"Tuna_v2/crypto"
)

type Linearizer struct {
	blocks   map[BlockRef]bool
	payloads map[crypto.Digest]bool
}

func NewLinearizer() *Linearizer {
	return &Linearizer{
		blocks:   make(map[BlockRef]bool),
		payloads: make(map[crypto.Digest]bool),
	}
}

// Plan does not mutate the committed set until its journal transaction succeeds.
func (l *Linearizer) Plan(reader Reader, decision Decision, index uint64) (CommittedSubDag, error) {
	commit := CommittedSubDag{
		Index:  index,
		Slot:   decision.Slot,
		Leader: decision.Block,
	}
	stack := []BlockRef{decision.Block}
	seen := make(map[BlockRef]bool)
	for len(stack) > 0 {
		ref := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[ref] || l.blocks[ref] || ref.Round == 0 {
			continue
		}

		seen[ref] = true
		block, exists := reader.Get(ref)
		if !exists {
			return commit, fmt.Errorf("linearization missing block %v", ref)
		}

		commit.Blocks = append(commit.Blocks, ref)
		stack = append(stack, block.Parents...)
	}

	sort.Slice(commit.Blocks, func(i, j int) bool {
		return LessRef(commit.Blocks[i], commit.Blocks[j])
	})
	payloads := make(map[crypto.Digest]bool)
	for _, ref := range commit.Blocks {
		block, _ := reader.Get(ref)
		for _, digest := range block.Payloads {
			if !payloads[digest] && !l.payloads[digest] {
				payloads[digest] = true
				commit.Payloads = append(commit.Payloads, digest)
			}
		}
	}

	return commit, nil
}

func (l *Linearizer) Apply(commit CommittedSubDag) {
	for _, ref := range commit.Blocks {
		l.blocks[ref] = true
	}

	for _, digest := range commit.Payloads {
		l.payloads[digest] = true
	}
}
