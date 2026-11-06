package dag_test

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	tunav2 "Tuna_v2/consensus/tuna_v2"
	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/dag"
	"Tuna_v2/store"
)

type emptyPayloads struct {
	queue     []crypto.Digest
	restored  int
	available bool
}

func (p *emptyPayloads) Take(limit int) []crypto.Digest {
	end := min(limit, len(p.queue))
	out := append([]crypto.Digest(nil), p.queue[:end]...)
	p.queue = p.queue[end:]
	return out
}

func (p *emptyPayloads) Restore(refs []crypto.Digest) {
	p.restored += len(refs)
	p.queue = append(p.queue, refs...)
}

func (p *emptyPayloads) Check(_ core.NodeID, refs []crypto.Digest, _ crypto.Digest) (bool, error) {
	return len(refs) == 0 || p.available, nil
}

func (p *emptyPayloads) Remove([]crypto.Digest) {}

type simulation struct {
	committee  core.Committee
	signers    []*crypto.SigService
	parameters dag.Parameters
	engines    []*dag.Core
	stores     []*store.Store
	outputs    [][]dag.CommittedSubDag
	step       int
	rng        *rand.Rand
	events     []delivery
}

type delivery struct {
	at, recipient int
	block         *dag.Block
}

func newSimulation(t *testing.T, n, k int) *simulation {
	t.Helper()
	s := &simulation{
		committee: core.Committee{
			Authorities: make(map[core.NodeID]core.Authority),
		},
		parameters: dag.DefaultParameters,
		rng:        rand.New(rand.NewSource(7)),
	}
	s.parameters.LeaderCount = k
	s.parameters.MinBlockDelay = 1
	s.parameters.LeaderTimeout = 25
	for i := 0; i < n; i++ {
		private, public := crypto.GenED25519Keys()
		s.committee.Authorities[core.NodeID(i)] = core.Authority{
			Name: crypto.PublickKey{
				Pubkey: public,
			},
		}
		s.signers = append(s.signers, crypto.NewSigService(crypto.PrivateKey{
			Prikey: private,
		}, crypto.SecretShareKey{}))
		s.stores = append(s.stores, store.NewStore(store.NewMemoryDB()))
		s.outputs = append(s.outputs, nil)
		s.engines = append(s.engines, nil)
	}

	for i := 0; i < n; i++ {
		s.restart(t, i)
	}

	return s
}

func (s *simulation) restart(t *testing.T, id int) {
	t.Helper()
	committer, _ := tunav2.New(s.committee, s.parameters.LeaderCount)
	repository, err := dag.OpenRepository(s.stores[id], dag.Domain(s.committee, s.parameters.LeaderCount), core.NodeID(id))
	if err != nil {
		t.Fatal(err)
	}

	s.engines[id], err = dag.NewCore(
		core.NodeID(id),
		s.committee,
		s.parameters,
		s.signers[id],
		&emptyPayloads{},
		repository,
		committer,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (s *simulation) run(t *testing.T, steps int, active []bool) {
	t.Helper()
	for iteration := 0; iteration < steps; iteration++ {
		s.step++
		now := time.Unix(1000, 0).Add(time.Duration(s.step) * 5 * time.Millisecond)

		var later []delivery

		for _, event := range s.events {
			if event.at > s.step {
				later = append(later, event)
				continue
			}

			if active[event.recipient] {
				if err := s.engines[event.recipient].Add(event.block); err != nil {
					t.Fatal(err)
				}
			}
		}

		s.events = later
		// A periodic head exchange and explicit missing-parent fetch model the wire syncer.
		for dst, engine := range s.engines {
			if !active[dst] {
				continue
			}

			if s.step%10 == 0 {
				for src, source := range s.engines {
					if src == dst || !active[src] {
						continue
					}

					for _, head := range source.State.Heads() {
						if _, exists := engine.State.Get(head); !exists {
							block, _ := source.State.Get(head)
							if err := engine.Add(block); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
			}

			for _, ref := range engine.Manager.Missing(engine.State) {
				for src, source := range s.engines {
					if src == dst || !active[src] {
						continue
					}

					if block, exists := source.State.Get(ref); exists {
						if err := engine.Add(block); err != nil {
							t.Fatal(err)
						}

						break
					}
				}
			}
		}

		for id, engine := range s.engines {
			if !active[id] {
				continue
			}

			effects, err := engine.Step(now)
			if err != nil {
				t.Fatalf("node %d step %d: %v", id, s.step, err)
			}

			s.outputs[id] = append(s.outputs[id], effects.Commits...)
			if effects.Proposed != nil {
				for dst := range s.engines {
					if dst != id && active[dst] {
						s.events = append(s.events, delivery{
							at:        s.step + s.rng.Intn(4) + 1,
							recipient: dst,
							block:     effects.Proposed,
						})
					}
				}
			}
		}
	}
}

func (s *simulation) assertPrefix(t *testing.T, active []bool) {
	t.Helper()

	var expected []dag.CommittedSubDag

	for id, sequence := range s.outputs {
		if !active[id] {
			continue
		}

		if len(sequence) < 4 {
			t.Fatalf("node %d produced too few commits: %d", id, len(sequence))
		}

		for i, commit := range sequence {
			if commit.Index != uint64(i+1) {
				t.Fatalf("node %d repeated or lost commit index", id)
			}
		}

		if expected == nil {
			expected = sequence
			continue
		}

		for i := 0; i < min(len(sequence), len(expected)); i++ {
			if sequence[i].Leader != expected[i].Leader {
				t.Fatalf("commit disagreement at index %d", i+1)
			}
		}
	}
}

func TestCoresAgreeWithReorderingAndDuplicates(t *testing.T) {
	for _, n := range []int{4, 7} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			s := newSimulation(t, n, 2)
			active := make([]bool, n)
			for i := range active {
				active[i] = true
			}

			s.run(t, 120, active)
			s.assertPrefix(t, active)
		})
	}
}

func TestOfflineMemberCanCatchUpWithoutBlockingQuorum(t *testing.T) {
	s := newSimulation(t, 4, 2)
	active := []bool{
		true,
		true,
		true,
		false,
	}
	s.run(t, 160, active)
	s.assertPrefix(t, active)
	active[3] = true
	s.run(t, 200, active)
	s.assertPrefix(t, active)
	if s.engines[3].Stats().Highest < 10 {
		t.Fatal("late node did not catch up")
	}
}

func TestRestartRestoresOwnChainAndCommitCursor(t *testing.T) {
	s := newSimulation(t, 4, 2)
	active := []bool{
		true,
		true,
		true,
		true,
	}
	s.run(t, 100, active)
	before := s.engines[0].Stats()
	s.restart(t, 0)
	after := s.engines[0].Stats()
	if before.Proposed != after.Proposed ||
		before.Commits != after.Commits ||
		before.Cursor == nil ||
		after.Cursor == nil ||
		*before.Cursor != *after.Cursor {
		t.Fatal("durable state not restored")
	}

	s.run(t, 100, active)
	s.assertPrefix(t, active)
	journal, err := s.engines[0].Repository.ReadCommits(0)
	if err != nil || len(journal) != len(s.outputs[0]) {
		t.Fatalf("journal recovery failed: %v", err)
	}
}

type controlledDecider struct {
	dag.Consensus
	decisions []dag.Decision
}

func (d *controlledDecider) Evaluate(_ dag.Reader, cursor *dag.Slot) ([]dag.Decision, error) {
	var result []dag.Decision

	for _, decision := range d.decisions {
		if cursor == nil || dag.LessSlot(*cursor, decision.Slot) {
			result = append(result, decision)
		}
	}

	return result, nil
}

func TestCoreStopsOutputAtFirstUndecidedAndResumesInOffsetOrder(t *testing.T) {
	s := newSimulation(t, 4, 2)
	base, _ := tunav2.New(s.committee, 2)
	domain := dag.Domain(s.committee, 2)

	var parents []dag.BlockRef

	for _, id := range dag.Members(s.committee) {
		parents = append(parents, dag.Genesis(domain, id).Ref())
	}

	parents[0], parents[2] = parents[2], parents[0]
	target, _ := dag.NewBlock(domain, 2, 1, parents, nil, 1, s.signers[2])
	controlled := &controlledDecider{
		Consensus: base,
		decisions: []dag.Decision{
			{
				Slot: dag.Slot{
					Round:  1,
					Offset: 0,
					Author: 1,
				},
				Kind: dag.Undecided,
			},
			{
				Slot: dag.Slot{
					Round:  1,
					Offset: 1,
					Author: 2,
				},
				Kind:  dag.DirectCommit,
				Block: target.Ref(),
			},
		},
	}
	repository, _ := dag.OpenRepository(s.stores[0], domain, 0)
	engine, err := dag.NewCore(0, s.committee, s.parameters, s.signers[0], &emptyPayloads{}, repository, controlled)
	if err != nil {
		t.Fatal(err)
	}

	if err := engine.Add(target); err != nil {
		t.Fatal(err)
	}

	effects, err := engine.Step(time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}

	if len(effects.Commits) != 0 || engine.Stats().Cursor != nil {
		t.Fatal("output crossed an undecided slot")
	}

	controlled.decisions[0].Kind = dag.DirectSkip
	effects, err = engine.Step(time.Unix(1000, 1))
	if err != nil {
		t.Fatal(err)
	}

	if len(effects.Commits) != 1 || effects.Commits[0].Leader != target.Ref() {
		t.Fatal("output did not resume after the resolved prefix")
	}

	effects, err = engine.Step(time.Unix(1000, 2))
	if err != nil || len(effects.Commits) != 0 {
		t.Fatal("commit was emitted twice")
	}
}

func TestMissingPayloadDoesNotEnterAcceptedDAG(t *testing.T) {
	s := newSimulation(t, 4, 2)
	base, _ := tunav2.New(s.committee, 2)
	domain := dag.Domain(s.committee, 2)
	source := &emptyPayloads{}
	repository, _ := dag.OpenRepository(s.stores[0], domain, 0)
	engine, err := dag.NewCore(0, s.committee, s.parameters, s.signers[0], source, repository, base)
	if err != nil {
		t.Fatal(err)
	}

	parents := []dag.BlockRef{dag.Genesis(domain, 1).Ref(), dag.Genesis(domain, 0).Ref(), dag.Genesis(domain, 2).Ref()}
	target, _ := dag.NewBlock(domain, 1, 1, parents, []crypto.Digest{
		{1},
	}, 1, s.signers[1])
	engine.Add(target)
	if err := engine.Drain(); err != nil {
		t.Fatal(err)
	}

	if _, exists := engine.State.Get(target.Ref()); exists {
		t.Fatal("unavailable payload block entered DAG")
	}

	source.available = true
	if err := engine.Drain(); err != nil {
		t.Fatal(err)
	}

	if _, exists := engine.State.Get(target.Ref()); !exists {
		t.Fatal("block not admitted after payload availability")
	}
}

type faultyBatch struct {
	*store.MemoryDB
	fail bool
}

func (db *faultyBatch) WriteBatch(entries []store.Entry) error {
	if db.fail {
		return errors.New("injected journal failure")
	}

	return db.MemoryDB.WriteBatch(entries)
}

func TestFailedOwnProposalRestoresPayloadsWithoutPublishingState(t *testing.T) {
	s := newSimulation(t, 4, 2)
	backend := &faultyBatch{
		MemoryDB: store.NewMemoryDB(),
	}
	storage := store.NewStore(backend)
	domain := dag.Domain(s.committee, 2)
	repository, _ := dag.OpenRepository(storage, domain, 0)
	base, _ := tunav2.New(s.committee, 2)
	source := &emptyPayloads{
		queue: []crypto.Digest{
			{1},
		},
		available: true,
	}
	engine, err := dag.NewCore(0, s.committee, s.parameters, s.signers[0], source, repository, base)
	if err != nil {
		t.Fatal(err)
	}

	backend.fail = true
	if _, err := engine.Step(time.Unix(1000, 0)); err == nil {
		t.Fatal("journal failure ignored")
	}

	if engine.Stats().Proposed != 0 || source.restored != 1 {
		t.Fatal("failed proposal changed own state or lost reserved payloads")
	}

	backend.fail = false
	effects, err := engine.Step(time.Unix(1000, 1))
	if err != nil || effects.Proposed == nil || len(effects.Proposed.Payloads) != 1 {
		t.Fatalf("retry lost payload: %v", err)
	}
}

func TestEvictedBodiesReloadFromDurableEvidence(t *testing.T) {
	s := newSimulation(t, 4, 2)
	s.parameters.CacheRounds = 2
	for i := range s.engines {
		s.restart(t, i)
	}

	active := []bool{
		true,
		true,
		true,
		true,
	}
	s.run(t, 100, active)
	s.assertPrefix(t, active)
	engine := s.engines[0]
	old := s.outputs[0][0].Leader
	before := engine.State.CachedBlocks()
	block, exists := engine.State.Get(old)
	if !exists || block.Ref() != old || engine.State.Error() != nil {
		t.Fatal("evicted evidence could not be reloaded")
	}

	if engine.State.CachedBlocks() <= before {
		t.Fatal("old block body was never evicted")
	}
}

func TestDurableCommitsRecoverWithoutReemittingLostNotification(t *testing.T) {
	s := newSimulation(t, 4, 2)
	domain := dag.Domain(s.committee, 2)
	base, _ := tunav2.New(s.committee, 2)
	parents := []dag.BlockRef{dag.Genesis(domain, 2).Ref(), dag.Genesis(domain, 0).Ref(), dag.Genesis(domain, 1).Ref()}
	target, _ := dag.NewBlock(domain, 2, 1, parents, nil, 1, s.signers[2])
	controlled := &controlledDecider{
		Consensus: base,
		decisions: []dag.Decision{
			{
				Slot: dag.Slot{
					Round:  1,
					Offset: 0,
					Author: 1,
				},
				Kind: dag.DirectSkip,
			},
			{
				Slot: dag.Slot{
					Round:  1,
					Offset: 1,
					Author: 2,
				},
				Kind:  dag.DirectCommit,
				Block: target.Ref(),
			},
		},
	}
	repository, _ := dag.OpenRepository(s.stores[0], domain, 0)
	source := &emptyPayloads{}
	engine, err := dag.NewCore(0, s.committee, s.parameters, s.signers[0], source, repository, controlled)
	if err != nil {
		t.Fatal(err)
	}

	engine.Add(target)
	effects, err := engine.Step(time.Unix(1000, 0))
	if err != nil || len(effects.Commits) != 1 {
		t.Fatalf("commit setup failed: %v", err)
	}
	// Drop the notification and recreate the core using only its durable state.
	restored, err := dag.NewCore(0, s.committee, s.parameters, s.signers[0], source, repository, controlled)
	if err != nil {
		t.Fatal(err)
	}

	history, err := repository.ReadCommits(0)
	if err != nil || len(history) != 1 || history[0].Leader != target.Ref() {
		t.Fatal("durable notification recovery failed")
	}

	next, err := restored.Step(time.Unix(1001, 0))
	if err != nil || len(next.Commits) != 0 {
		t.Fatal("history was emitted twice after recovery")
	}
}
