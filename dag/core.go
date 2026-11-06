package dag

import (
	"fmt"
	"time"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

type Effects struct {
	Proposed  *Block
	Decisions []Decision
	Commits   []CommittedSubDag
}

type Stats struct {
	Cached         int
	Round          Round
	Highest        Round
	Proposed       Round
	Pending        int
	Undecided      int
	FirstUndecided *Slot
	Cursor         *Slot
	Commits        uint64
}

// Core has one owner; callers send network and timer events through the runtime.
type Core struct {
	ID           core.NodeID
	Domain       crypto.Digest
	State        *State
	Manager      *BlockManager
	Clock        *ThresholdClock
	Repository   *Repository
	consensus    Consensus
	parameters   Parameters
	committee    core.Committee
	signer       *crypto.SigService
	payloads     PayloadSource
	linearizer   *Linearizer
	lastOwn      *Block
	cursor       *Slot
	commitIndex  uint64
	known        map[Slot]Decision
	waitingRound Round
	waitingSince time.Time
	lastProposal time.Time
}

func NewCore(
	id core.NodeID,
	committee core.Committee,
	parameters Parameters,
	signer *crypto.SigService,
	payloads PayloadSource,
	repository *Repository,
	consensus Consensus,
) (*Core, error) {
	if err := parameters.Validate(committee); err != nil {
		return nil, err
	}

	if _, ok := committee.Authorities[id]; !ok {
		return nil, fmt.Errorf("unknown local authority")
	}

	//根据委员会成员和每轮 leader 数量，计算一个共识配置标识，并保存到变量 domain 中。
	//避免不同共识配置的区块混用
	domain := Domain(committee, parameters.LeaderCount)
	c := &Core{
		ID:         id,
		Domain:     domain,
		State:      NewState(domain, committee),
		Manager:    NewBlockManager(parameters.MaxPending),
		Clock:      NewThresholdClock(consensus.Quorum()),
		Repository: repository,
		consensus:  consensus,
		parameters: parameters,
		committee:  committee,
		signer:     signer,
		payloads:   payloads,
		linearizer: NewLinearizer(),
		known:      make(map[Slot]Decision),
	}
	for _, member := range Members(committee) {
		c.Clock.Add(Genesis(domain, member).Ref())
	}

	recovery, err := repository.Recover()
	if err != nil {
		return nil, err
	}

	expectedOwn := Genesis(domain, id).Ref()
	for _, block := range recovery.Blocks {
		if err := block.Verify(domain, committee, consensus.Quorum(), parameters); err != nil {
			return nil, fmt.Errorf("recovered block: %w", err)
		}

		for _, ref := range block.Parents {
			if _, ok := c.State.Get(ref); !ok {
				return nil, fmt.Errorf("recovered block has a missing parent")
			}
		}

		ready, err := payloads.Check(block.Author, block.Payloads, block.Hash())
		if err != nil {
			return nil, err
		}

		if !ready {
			return nil, fmt.Errorf("recovered block has missing payloads")
		}

		if block.Author == id {
			if block.Parents[0] != expectedOwn {
				return nil, fmt.Errorf("recovered own chain is inconsistent")
			}

			expectedOwn = block.Ref()
		}

		c.State.Add(block)
		c.Clock.Add(block.Ref())
	}

	c.lastOwn = Genesis(domain, id)
	if recovery.Own != nil {
		own, ok := c.State.Get(*recovery.Own)
		if !ok || own.Author != id {
			return nil, fmt.Errorf("invalid recovered own proposal")
		}

		c.lastOwn = own
	}

	for _, block := range recovery.Blocks {
		if block.Author == id {
			if block.Round > c.lastOwn.Round {
				return nil, fmt.Errorf("own block is ahead of durable proposal pointer")
			}

			c.payloads.Remove(block.Payloads)
		}
	}

	if recovery.Cursor != nil {
		slot := *recovery.Cursor
		leaders := consensus.Leaders(slot.Round)
		if slot.Round > c.State.HighestRound() ||
			slot.Round == 0 ||
			slot.Offset < 0 ||
			slot.Offset >= len(leaders) ||
			leaders[slot.Offset] != slot.Author {
			return nil, fmt.Errorf("invalid recovered cursor")
		}

		c.cursor = &slot
	}

	for _, commit := range recovery.Commits {
		if c.cursor == nil || LessSlot(*c.cursor, commit.Slot) {
			return nil, fmt.Errorf("commit journal is ahead of cursor")
		}

		for _, ref := range commit.Blocks {
			if _, ok := c.State.Get(ref); !ok {
				return nil, fmt.Errorf("commit references missing block")
			}
		}

		c.linearizer.Apply(commit)
		c.payloads.Remove(commit.Payloads)
		c.commitIndex = commit.Index
	}

	c.State.SetLoader(func(ref BlockRef) (*Block, error) {
		block, err := repository.ReadBlock(ref)
		if err != nil {
			return nil, err
		}

		if err := block.Verify(domain, committee, consensus.Quorum(), parameters); err != nil {
			return nil, err
		}

		return block, nil
	})
	return c, nil
}

func (c *Core) Add(block *Block) error {
	if err := block.Verify(c.Domain, c.committee, c.consensus.Quorum(), c.parameters); err != nil {
		return err
	}

	if _, exists := c.State.Get(block.Ref()); exists {
		return nil
	}

	if block.Author == c.ID {
		return fmt.Errorf("received unknown own proposal")
	}

	return c.Manager.Add(c.State, block)
}

func (c *Core) Drain() error {
	for {
		progress := false
		for _, block := range c.Manager.Ready(c.State) {
			ready, err := c.payloads.Check(block.Author, block.Payloads, block.Hash())
			if err != nil {
				return err
			}

			if !ready {
				continue
			}

			if err := c.Repository.SaveBlock(block, false); err != nil {
				return err
			}

			c.State.Add(block)
			c.Clock.Add(block.Ref())
			c.Manager.Remove(block.Ref())
			progress = true
		}

		if !progress {
			return nil
		}
	}
}

func (c *Core) propose(now time.Time) (*Block, error) {
	round := c.Clock.Round()
	if round <= c.lastOwn.Round {
		return nil, nil
	}

	if c.waitingRound != round {
		c.waitingRound = round
		c.waitingSince = now
	}

	if !c.lastProposal.IsZero() &&
		now.Sub(c.lastProposal) < time.Duration(c.parameters.MinBlockDelay)*time.Millisecond {
		return nil, nil
	}

	leadersReady := true
	for _, leader := range c.consensus.Leaders(round - 1) {
		if len(c.State.AtSlot(leader, round-1)) == 0 {
			leadersReady = false
			break
		}
	}

	if !leadersReady &&
		now.Sub(c.waitingSince) < time.Duration(c.parameters.LeaderTimeout)*time.Millisecond {
		return nil, nil
	}

	parents := []BlockRef{c.lastOwn.Ref()}
	seen := map[BlockRef]bool{
		c.lastOwn.Ref(): true,
	}
	authors := make(map[core.NodeID]bool)
	// One strong link per author preserves space for a quorum even with equivocation.
	for _, block := range c.State.AtRound(round - 1) {
		if authors[block.Author] {
			continue
		}

		authors[block.Author] = true
		ref := block.Ref()
		if !seen[ref] {
			parents = append(parents, ref)
			seen[ref] = true
		}
	}

	if len(authors) < c.consensus.Quorum() {
		return nil, nil
	}

	for _, ref := range c.State.Heads() {
		if ref.Round < round && !seen[ref] && len(parents) < c.parameters.MaxParents {
			parents = append(parents, ref)
			seen[ref] = true
		}
	}

	payloads := c.payloads.Take(c.parameters.MaxPayloads)
	block, err := NewBlock(c.Domain, c.ID, round, parents, payloads, now.UnixNano(), c.signer)
	if err == nil {
		err = block.Verify(c.Domain, c.committee, c.consensus.Quorum(), c.parameters)
	}

	if err == nil {
		ready, checkErr := c.payloads.Check(c.ID, payloads, block.Hash())
		if checkErr != nil {
			err = checkErr
		} else if !ready {
			c.payloads.Restore(payloads)
			return nil, nil
		}
	}

	if err == nil {
		err = c.Repository.SaveBlock(block, true)
	}

	if err != nil {
		c.payloads.Restore(payloads)
		return nil, err
	}

	c.lastOwn = block
	c.State.Add(block)
	c.Clock.Add(block.Ref())
	c.lastProposal = now
	return block, nil
}

func (c *Core) Step(now time.Time) (Effects, error) {
	var effects Effects

	if err := c.State.Error(); err != nil {
		return effects, err
	}

	if err := c.Drain(); err != nil {
		return effects, err
	}

	if err := c.State.Error(); err != nil {
		return effects, err
	}

	block, err := c.propose(now)
	if err != nil {
		return effects, err
	}

	effects.Proposed = block
	decisions, err := c.consensus.Evaluate(c.State, c.cursor)
	if err != nil {
		return effects, err
	}

	if err := c.State.Error(); err != nil {
		return effects, err
	}

	for _, decision := range decisions {
		previous, exists := c.known[decision.Slot]
		if exists && previous.Kind != Undecided && previous != decision {
			return effects, fmt.Errorf("non-monotonic slot decision")
		}

		if !exists || previous != decision {
			effects.Decisions = append(effects.Decisions, decision)
		}

		c.known[decision.Slot] = decision
	}

	for _, decision := range decisions {
		if decision.Kind == Undecided {
			break
		}

		var commit *CommittedSubDag

		if decision.Kind == DirectCommit {
			planned, err := c.linearizer.Plan(c.State, decision, c.commitIndex+1)
			if err != nil {
				return effects, err
			}

			commit = &planned
		}

		if err := c.Repository.SaveDecision(decision.Slot, commit); err != nil {
			return effects, err
		}

		slot := decision.Slot
		c.cursor = &slot
		delete(c.known, slot)
		if commit != nil {
			c.commitIndex = commit.Index
			c.linearizer.Apply(*commit)
			c.payloads.Remove(commit.Payloads)
			effects.Commits = append(effects.Commits, *commit)
		}
	}

	if c.parameters.CacheRounds > 0 && c.State.HighestRound() > Round(c.parameters.CacheRounds) {
		c.State.UnloadBelow(c.State.HighestRound() - Round(c.parameters.CacheRounds))
	}

	return effects, nil
}

func (c *Core) Stats() Stats {
	stats := Stats{
		Cached:   c.State.CachedBlocks(),
		Round:    c.Clock.Round(),
		Highest:  c.State.HighestRound(),
		Proposed: c.lastOwn.Round,
		Pending:  c.Manager.Len(),
		Commits:  c.commitIndex,
	}
	if c.cursor != nil {
		cursor := *c.cursor
		stats.Cursor = &cursor
	}

	for slot, decision := range c.known {
		if decision.Kind == Undecided {
			stats.Undecided++
			if stats.FirstUndecided == nil || LessSlot(slot, *stats.FirstUndecided) {
				copy := slot
				stats.FirstUndecided = &copy
			}
		}
	}

	return stats
}
