// Package tunav2 implements Tuna_v2's direct DAG decision rules.
package tunav2

import (
	"Tuna_v2/core"
	"Tuna_v2/dag"
)

type LeaderElector struct {
	members []core.NodeID
	count   int
}

func NewLeaderElector(committee core.Committee, count int) LeaderElector {
	return LeaderElector{
		members: dag.Members(committee),
		count:   count,
	}
}

// 目前按照轮转规则决定每一轮的leader
func (e LeaderElector) Leaders(round dag.Round) []core.NodeID {
	leaders := make([]core.NodeID, e.count)
	for offset := range leaders {
		leaders[offset] = e.members[(uint64(round)%uint64(len(e.members))+uint64(offset))%uint64(len(e.members))]
	}

	return leaders
}
