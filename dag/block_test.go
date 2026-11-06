package dag

import (
	"reflect"
	"testing"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

func signedFixture(t *testing.T) (core.Committee, []*crypto.SigService, Parameters, crypto.Digest) {
	t.Helper()
	committee := core.Committee{
		Authorities: make(map[core.NodeID]core.Authority),
	}

	var signers []*crypto.SigService

	for i := 0; i < 4; i++ {
		private, public := crypto.GenED25519Keys()
		committee.Authorities[core.NodeID(i)] = core.Authority{
			Name: crypto.PublickKey{
				Pubkey: public,
			},
		}
		signers = append(signers, crypto.NewSigService(crypto.PrivateKey{
			Prikey: private,
		}, crypto.SecretShareKey{}))
	}

	parameters := DefaultParameters
	return committee, signers, parameters, Domain(committee, parameters.LeaderCount)
}

func TestBlockValidationAndCanonicalHash(t *testing.T) {
	committee, signers, parameters, domain := signedFixture(t)

	var parents []BlockRef

	for _, id := range Members(committee) {
		parents = append(parents, Genesis(domain, id).Ref())
	}

	block, err := NewBlock(domain, 0, 1, parents, nil, 123, signers[0])
	if err != nil {
		t.Fatal(err)
	}

	if err := block.Verify(domain, committee, 3, parameters); err != nil {
		t.Fatal(err)
	}

	encoded, _ := block.Encode()
	decoded, err := DecodeBlock(encoded)
	if err != nil || decoded.Ref() != block.Ref() {
		t.Fatal("block round trip changed its reference")
	}

	cases := map[string]func(*Block){
		"wrong domain": func(b *Block) {
			b.Domain[0]++
		},
		"unknown author": func(b *Block) {
			b.Author = 99
		},
		"duplicate parent": func(b *Block) {
			b.Parents[1] = b.Parents[0]
		},
		"future parent": func(b *Block) {
			b.Parents[1].Round = 1
		},
		"wrong own parent": func(b *Block) {
			b.Parents[0], b.Parents[1] = b.Parents[1], b.Parents[0]
		},
		"insufficient authors": func(b *Block) {
			b.Parents = b.Parents[:2]
		},
		"invalid signature": func(b *Block) {
			b.Signature.Sig = nil
		},
		"duplicate payload": func(b *Block) {
			b.Payloads = []crypto.Digest{
				{1},
				{1},
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			copy := block.Clone()
			mutate(copy)
			if err := copy.Verify(domain, committee, 3, parameters); err == nil {
				t.Fatal("invalid block accepted")
			}
		})
	}

	copy := block.Clone()
	copy.Parents[0].Author = 3
	if block.Parents[0].Author != 0 {
		t.Fatal("clone aliases signed parent array")
	}
}

func TestClockDeduplicatesAuthors(t *testing.T) {
	clock := NewThresholdClock(3)
	for i := byte(0); i < 10; i++ {
		clock.Add(BlockRef{
			Round:  1,
			Author: 0,
			Digest: crypto.Digest{i},
		})
	}

	if clock.Round() != 0 {
		t.Fatal("equivocations advanced clock")
	}

	clock.Add(BlockRef{
		Round:  1,
		Author: 1,
	})
	clock.Add(BlockRef{
		Round:  1,
		Author: 2,
	})
	if clock.Round() != 2 {
		t.Fatal("quorum did not advance clock")
	}

	clock.Add(BlockRef{
		Round:  0,
		Author: 3,
	})
	if clock.Round() != 2 {
		t.Fatal("clock moved backwards")
	}
}

func TestBlockManagerWaitsForFullParentClosure(t *testing.T) {
	committee, signers, parameters, domain := signedFixture(t)
	state := NewState(domain, committee)

	var genesis []BlockRef

	for _, id := range Members(committee) {
		genesis = append(genesis, Genesis(domain, id).Ref())
	}

	var layer []*Block

	for i := 0; i < 4; i++ {
		parents := append([]BlockRef(nil), genesis...)
		parents[0], parents[i] = parents[i], parents[0]
		b, _ := NewBlock(domain, core.NodeID(i), 1, parents, nil, 1, signers[i])
		layer = append(layer, b)
	}

	parents := []BlockRef{layer[0].Ref(), layer[1].Ref(), layer[2].Ref()}
	child, _ := NewBlock(domain, 0, 2, parents, nil, 2, signers[0])
	manager := NewBlockManager(parameters.MaxPending)
	manager.Add(state, child)
	if len(manager.Ready(state)) != 0 || len(manager.Missing(state)) != 3 {
		t.Fatal("child accepted before parents")
	}

	for _, block := range layer[:3] {
		state.Add(block)
	}

	if len(manager.Ready(state)) != 1 {
		t.Fatal("complete child not released")
	}
}

func TestLinearizationHasStableOrderAndDeduplicatesPayloads(t *testing.T) {
	committee, signers, _, domain := signedFixture(t)
	first := NewState(domain, committee)
	second := NewState(domain, committee)

	var genesis []BlockRef

	for _, id := range Members(committee) {
		genesis = append(genesis, Genesis(domain, id).Ref())
	}

	var layer []*Block
	var refs []BlockRef

	for i := 0; i < 4; i++ {
		parents := append([]BlockRef(nil), genesis...)
		parents[0], parents[i] = parents[i], parents[0]
		block, _ := NewBlock(domain, core.NodeID(i), 1, parents, []crypto.Digest{
			{1},
		}, 1, signers[i])
		layer = append(layer, block)
		refs = append(refs, block.Ref())
		first.Add(block)
	}

	for i := len(layer) - 1; i >= 0; i-- {
		second.Add(layer[i])
	}

	root, _ := NewBlock(domain, 0, 2, refs, []crypto.Digest{
		{1},
		{2},
	}, 2, signers[0])
	first.Add(root)
	second.Add(root)
	decision := Decision{
		Slot: Slot{
			Round:  2,
			Author: 0,
		},
		Kind:  DirectCommit,
		Block: root.Ref(),
	}
	left, right := NewLinearizer(), NewLinearizer()
	a, err := left.Plan(first, decision, 1)
	if err != nil {
		t.Fatal(err)
	}

	b, err := right.Plan(second, decision, 1)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(a, b) || len(a.Payloads) != 2 || len(a.Blocks) != 5 {
		t.Fatal("linearization depends on arrival order or repeats payloads")
	}

	left.Apply(a)
	again, err := left.Plan(first, decision, 2)
	if err != nil || len(again.Blocks) != 0 || len(again.Payloads) != 0 {
		t.Fatal("committed data was emitted twice")
	}
}
