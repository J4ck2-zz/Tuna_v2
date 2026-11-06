package tunav2

import (
	"testing"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/dag"
)

type fixture struct {
	committee core.Committee
	signers   []*crypto.SigService
	domain    crypto.Digest
	state     *dag.State
}

func newFixture(n, k int) *fixture {
	f := &fixture{
		committee: core.Committee{
			Authorities: make(map[core.NodeID]core.Authority),
		},
	}
	for i := 0; i < n; i++ {
		private, public := crypto.GenED25519Keys()
		f.committee.Authorities[core.NodeID(i)] = core.Authority{
			Name: crypto.PublickKey{
				Pubkey: public,
			},
		}
		f.signers = append(f.signers, crypto.NewSigService(crypto.PrivateKey{
			Prikey: private,
		}, crypto.SecretShareKey{}))
	}

	f.domain = dag.Domain(f.committee, k)
	f.state = dag.NewState(f.domain, f.committee)
	return f
}

func (f *fixture) block(
	t *testing.T,
	author int,
	round dag.Round,
	parents []dag.BlockRef,
	nonce int64,
) *dag.Block {
	t.Helper()
	block, err := dag.NewBlock(f.domain, core.NodeID(author), round, parents, nil, nonce, f.signers[author])
	if err != nil {
		t.Fatal(err)
	}

	f.state.Add(block)
	return block
}

func (f *fixture) layer(t *testing.T, round dag.Round) {
	t.Helper()

	var refs []dag.BlockRef

	for _, block := range f.state.AtRound(round - 1) {
		refs = append(refs, block.Ref())
	}

	for i := range f.signers {
		parents := append([]dag.BlockRef(nil), refs...)
		for j, ref := range parents {
			if ref.Author == core.NodeID(i) {
				parents[0], parents[j] = parents[j], parents[0]
				break
			}
		}

		f.block(t, i, round, parents, int64(round))
	}
}

func TestDirectCommitRequiresTwoCertificateLevels(t *testing.T) {
	f := newFixture(4, 2)
	committer, _ := New(f.committee, 2)
	f.layer(t, 1)
	f.layer(t, 2)
	decisions, err := committer.Evaluate(f.state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if decisions[0].Kind != dag.Undecided {
		t.Fatal("votes alone must not commit")
	}

	f.layer(t, 3)
	decisions, err = committer.Evaluate(f.state, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, decision := range decisions[:2] {
		if decision.Kind != dag.DirectCommit {
			t.Fatalf("expected direct commit: %+v", decision)
		}
	}

	cursor := decisions[0].Slot
	rest, err := committer.Evaluate(f.state, &cursor)
	if err != nil {
		t.Fatal(err)
	}

	if rest[0] != decisions[1] {
		t.Fatal("mid-round cursor resumed in the wrong offset order")
	}

	again, _ := committer.Evaluate(f.state, &cursor)
	if len(again) != len(rest) {
		t.Fatal("decision evaluation is not idempotent")
	}
}

func TestSkipRequiresNoSupportForTheWholeSlot(t *testing.T) {
	f := newFixture(4, 1)
	f.layer(t, 1)
	target := f.state.AtSlot(1, 1)[0]
	// Three voters omit the target. The equivocating author can link its older own block.
	for i := 0; i < 4; i++ {
		parents := []dag.BlockRef{dag.Genesis(f.domain, core.NodeID(i)).Ref()}
		for _, block := range f.state.AtRound(1) {
			if block.Author != 1 {
				parents = append(parents, block.Ref())
			}
		}

		f.block(t, i, 2, parents, 2)
	}

	base := BaseCommitter{
		quorum: 3,
	}
	decision, err := base.Decide(f.state, dag.Slot{
		Round:  1,
		Author: target.Author,
	})
	if err != nil {
		t.Fatal(err)
	}

	if decision.Kind != dag.DirectSkip {
		t.Fatalf("expected skip, got %v", decision.Kind)
	}
}

func TestSignedParentOrderSelectsOnlyOneEquivocation(t *testing.T) {
	f := newFixture(4, 1)
	f.layer(t, 1)
	first := f.state.AtSlot(1, 1)[0]
	twin := first.Clone()
	twin.Timestamp++
	twin.Signature, _ = f.signers[1].RequestSignature(twin.Hash())
	f.state.Add(twin)
	own := f.state.AtSlot(0, 1)[0].Ref()
	vote := f.block(t, 0, 2, []dag.BlockRef{
		own,
		twin.Ref(),
		first.Ref(),
		f.state.AtSlot(2, 1)[0].Ref(),
	}, 2)
	ref, ok, err := SupportedBlock(f.state, vote, 1, 1)
	if err != nil || !ok || ref != twin.Ref() {
		t.Fatalf("parent order did not pick the first twin: %v", err)
	}

	vote.Parents[1], vote.Parents[2] = vote.Parents[2], vote.Parents[1]
	if vote.Signature.Verify(f.committee.Name(0), vote.Hash()) {
		t.Fatal("changing parent order must invalidate the signature")
	}

	if len(f.state.AtSlot(1, 1)) != 2 {
		t.Fatal("equivocation overwrote a candidate")
	}
}

func TestEquivocatingCertifiersCountOnce(t *testing.T) {
	f := newFixture(4, 1)
	f.layer(t, 1)
	f.layer(t, 2)

	var votes []dag.BlockRef

	for _, block := range f.state.AtRound(2) {
		votes = append(votes, block.Ref())
	}

	for nonce := int64(0); nonce < 4; nonce++ {
		f.block(t, 0, 3, votes, nonce)
	}

	f.block(t, 1, 3, votes, 10)
	decision, err := (BaseCommitter{
		quorum: 3,
	}).Decide(f.state, dag.Slot{
		Round:  1,
		Author: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if decision.Kind != dag.Undecided {
		t.Fatal("multiple certificates from one author were counted more than once")
	}
}

func TestLaterDirectCommitsDoNotResolveAnUndecidedSlot(t *testing.T) {
	f := newFixture(4, 1)
	f.layer(t, 1)
	for i := 0; i < 4; i++ {
		own := f.state.AtSlot(core.NodeID(i), 1)[0].Ref()
		parents := []dag.BlockRef{own}
		for _, block := range f.state.AtRound(1) {
			if block.Author == core.NodeID(i) {
				continue
			}

			if i >= 2 && block.Author == 1 {
				continue
			}

			parents = append(parents, block.Ref())
		}

		f.block(t, i, 2, parents, 2)
	}

	for round := dag.Round(3); round <= 7; round++ {
		f.layer(t, round)
	}

	committer, _ := New(f.committee, 1)
	decisions, err := committer.Evaluate(f.state, nil)
	if err != nil {
		t.Fatal(err)
	}

	if decisions[0].Kind != dag.Undecided {
		t.Fatal("later anchors must not resolve the first slot")
	}

	if decisions[1].Kind != dag.DirectCommit {
		t.Fatal("later slots must still be directly evaluated")
	}
}

func TestRoundRobinWithSparseIDsAndWraparound(t *testing.T) {
	committee := core.Committee{
		Authorities: map[core.NodeID]core.Authority{
			9:  {},
			2:  {},
			17: {},
			5:  {},
		},
	}
	elector := NewLeaderElector(committee, 4)
	leaders := elector.Leaders(3)
	expected := []core.NodeID{
		17,
		2,
		5,
		9,
	}
	for i, leader := range leaders {
		if leader != expected[i] {
			t.Fatalf("bad leader order: %v", leaders)
		}
	}

	committer, _ := New(committee, 2)
	if committer.Quorum() != 3 {
		t.Fatal("wrong quorum")
	}

	five := newFixture(5, 1)
	c, _ := New(five.committee, 1)
	if c.Quorum() != 4 {
		t.Fatal("quorum must be floor(2N/3)+1")
	}
}
