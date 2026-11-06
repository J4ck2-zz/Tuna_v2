// Package dag owns Tuna_v2's block graph and single-owner runtime.
package dag

import (
	"bytes"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

type Round uint64

type BlockRef struct {
	Round  Round
	Author core.NodeID
	Digest crypto.Digest
}

func LessRef(a, b BlockRef) bool {
	if a.Round != b.Round {
		return a.Round < b.Round
	}

	if a.Author != b.Author {
		return a.Author < b.Author
	}

	return bytes.Compare(a.Digest[:], b.Digest[:]) < 0
}

type Slot struct {
	Round  Round
	Offset int
	Author core.NodeID
}

func LessSlot(a, b Slot) bool {
	return a.Round < b.Round || a.Round == b.Round && a.Offset < b.Offset
}

type DecisionKind uint8

const (
	Undecided DecisionKind = iota
	DirectCommit
	DirectSkip
)

func (k DecisionKind) String() string {
	switch k {
	case DirectCommit:
		return "direct_commit"
	case DirectSkip:
		return "direct_skip"
	default:
		return "undecided"
	}
}

type Decision struct {
	Slot  Slot
	Kind  DecisionKind
	Block BlockRef
}

// 查询 DAG 区块需要提供哪些方法 State 实现了这个接口
type Reader interface {
	Get(BlockRef) (*Block, bool)
	AtRound(Round) []*Block
	AtSlot(core.NodeID, Round) []*Block
	HighestRound() Round
}

type Consensus interface {
	Quorum() int
	Leaders(Round) []core.NodeID
	// Evaluate includes later decisions even when an earlier slot is undecided.
	Evaluate(Reader, *Slot) ([]Decision, error)
}

// UnresolvedResolver is an extension point for Tuna_v2's future agreement method.
// No implementation or automatic invocation is installed in this version.
type UnresolvedResolver interface {
	Resolve(Reader, Slot, []*Block) (Decision, error)
}

type CommittedSubDag struct {
	Index    uint64
	Slot     Slot
	Leader   BlockRef
	Blocks   []BlockRef
	Payloads []crypto.Digest
}

// PayloadSource bridges an external mempool without a package dependency on it.
type PayloadSource interface {
	Take(int) []crypto.Digest
	Restore([]crypto.Digest)
	Check(core.NodeID, []crypto.Digest, crypto.Digest) (bool, error)
	Remove([]crypto.Digest)
}
