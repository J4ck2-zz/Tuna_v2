package tunav2

import (
	"fmt"

	"Tuna_v2/core"
	"Tuna_v2/dag"
)

type Committer struct {
	elector LeaderElector
	base    BaseCommitter
}

func New(committee core.Committee, leaderCount int) (*Committer, error) {
	if committee.Size() == 0 || leaderCount < 1 || leaderCount > committee.Size() {
		return nil, fmt.Errorf("invalid leader count")
	}

	return &Committer{
		elector: NewLeaderElector(committee, leaderCount),
		base: BaseCommitter{
			quorum: 2*committee.Size()/3 + 1,
		},
	}, nil
}

func (c *Committer) Quorum() int {
	return c.base.quorum
}

func (c *Committer) Leaders(round dag.Round) []core.NodeID {
	return c.elector.Leaders(round)
}

func (c *Committer) Evaluate(reader dag.Reader, cursor *dag.Slot) ([]dag.Decision, error) {
	start := dag.Round(1)
	if cursor != nil {
		start = cursor.Round
	}

	var decisions []dag.Decision

	for round := start; round <= reader.HighestRound(); round++ {
		for offset, author := range c.Leaders(round) {
			slot := dag.Slot{
				Round:  round,
				Offset: offset,
				Author: author,
			}
			if cursor != nil && !dag.LessSlot(*cursor, slot) {
				continue
			}

			decision, err := c.base.Decide(reader, slot)
			if err != nil {
				return nil, err
			}

			decisions = append(decisions, decision)
		}
	}

	return decisions, nil
}
