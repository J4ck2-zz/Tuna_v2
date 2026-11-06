package dag

import "Tuna_v2/core"

// 按区块数量阈值推进的逻辑时钟，用于确定本节点当前可以在哪一轮提议区块
type ThresholdClock struct {
	round   Round
	quorum  int
	authors map[Round]map[core.NodeID]bool
}

func NewThresholdClock(q int) *ThresholdClock {
	return &ThresholdClock{
		quorum:  q,
		authors: make(map[Round]map[core.NodeID]bool),
	}
}

func (c *ThresholdClock) Add(ref BlockRef) {
	if ref.Round < c.round {
		return
	}

	if c.authors[ref.Round] == nil {
		c.authors[ref.Round] = make(map[core.NodeID]bool)
	}

	c.authors[ref.Round][ref.Author] = true
	if len(c.authors[ref.Round]) >= c.quorum {
		c.round = ref.Round + 1
		for round := range c.authors {
			if round < c.round {
				delete(c.authors, round)
			}
		}
	}
}

func (c *ThresholdClock) Round() Round {
	return c.round
}
