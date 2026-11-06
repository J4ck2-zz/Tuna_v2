// Direct support and certificate rules follow the reference DAG architecture
// in /root/mysticeti (Mysten Labs, Apache-2.0). Tuna_v2 has no indirect decider.
package tunav2

import (
	"fmt"

	"Tuna_v2/core"
	"Tuna_v2/dag"
)

type BaseCommitter struct {
	quorum int
}

// SupportedBlock follows signed parent order, selecting only the first target-slot block.
func SupportedBlock(reader dag.Reader, from *dag.Block, author core.NodeID, round dag.Round) (dag.BlockRef, bool, error) {
	stack := make([]dag.BlockRef, 0, len(from.Parents))
	for i := len(from.Parents) - 1; i >= 0; i-- {
		stack = append(stack, from.Parents[i])
	}

	visited := make(map[dag.BlockRef]bool)
	for len(stack) > 0 {
		ref := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if ref.Round < round || visited[ref] {
			continue
		}

		visited[ref] = true
		if ref.Round == round && ref.Author == author {
			return ref, true, nil
		}

		if ref.Round == round {
			continue
		}

		block, exists := reader.Get(ref)
		if !exists {
			return dag.BlockRef{}, false, fmt.Errorf("support walk missing ancestor %v", ref)
		}

		for i := len(block.Parents) - 1; i >= 0; i-- {
			stack = append(stack, block.Parents[i])
		}
	}

	return dag.BlockRef{}, false, nil
}

func (b BaseCommitter) CarriesCertificate(reader dag.Reader, certificate *dag.Block, target *dag.Block) (bool, error) {
	voters := make(map[core.NodeID]bool)
	for _, ref := range certificate.Parents {
		if ref.Round != VotingRound(target.Round) {
			continue
		}

		vote, exists := reader.Get(ref)
		if !exists {
			return false, fmt.Errorf("missing certificate vote")
		}

		support, exists, err := SupportedBlock(reader, vote, target.Author, target.Round)
		if err != nil {
			return false, err
		}

		if exists && support == target.Ref() {
			voters[vote.Author] = true
			if len(voters) >= b.quorum {
				return true, nil
			}
		}
	}

	return false, nil
}

func (b BaseCommitter) Decide(reader dag.Reader, slot dag.Slot) (dag.Decision, error) {
	result := dag.Decision{
		Slot: slot,
		Kind: dag.Undecided,
	}
	blamers := make(map[core.NodeID]bool)
	for _, vote := range reader.AtRound(VotingRound(slot.Round)) {
		_, exists, err := SupportedBlock(reader, vote, slot.Author, slot.Round)
		if err != nil {
			return result, err
		}

		if !exists {
			blamers[vote.Author] = true
		}
	}

	if len(blamers) >= b.quorum {
		result.Kind = dag.DirectSkip
		return result, nil
	}

	for _, target := range reader.AtSlot(slot.Author, slot.Round) {
		certifiers := make(map[core.NodeID]bool)
		for _, certificate := range reader.AtRound(DecisionRound(slot.Round)) {
			carries, err := b.CarriesCertificate(reader, certificate, target)
			if err != nil {
				return result, err
			}

			if carries {
				certifiers[certificate.Author] = true
			}
		}

		if len(certifiers) >= b.quorum {
			if result.Kind == dag.DirectCommit {
				return result, fmt.Errorf("conflicting direct commits at slot %+v", slot)
			}

			result.Kind = dag.DirectCommit
			result.Block = target.Ref()
		}
	}

	return result, nil
}
