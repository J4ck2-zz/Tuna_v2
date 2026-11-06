package dag

import (
	"sort"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

// State is owned by Core. Accepted blocks and their ordered parents are immutable.
// 管理当前节点已经接受的区块
type State struct {
	loader  func(BlockRef) (*Block, error) //从储存中重新加载区块
	loadErr error                          //加载区块时的错误
	loaded  map[BlockRef]bool              //记录哪些区块的内容当前缓存在内存中，作为集合使用。
	blocks  map[BlockRef]*Block
	rounds  map[Round]map[BlockRef]*Block //按轮次组织区块：轮次 → 该轮的区块集合。
	highest Round                         //已接受区块的最大轮次。
}

func NewState(domain crypto.Digest, committee core.Committee) *State {
	state := &State{
		blocks: make(map[BlockRef]*Block),
		loaded: make(map[BlockRef]bool),
		rounds: make(map[Round]map[BlockRef]*Block),
	}
	//这段代码在为委员会中的每个节点创建一个创世块，并加入 State，作为 DAG 的起点。
	for _, id := range Members(committee) {
		state.Add(Genesis(domain, id))
	}

	return state
}

func (s *State) Add(block *Block) bool {
	ref := block.Ref()
	if _, exists := s.blocks[ref]; exists {
		return false
	}

	copy := block.Clone()
	s.blocks[ref] = copy
	s.loaded[ref] = true
	if s.rounds[ref.Round] == nil {
		s.rounds[ref.Round] = make(map[BlockRef]*Block)
	}

	s.rounds[ref.Round][ref] = copy
	if ref.Round > s.highest {
		s.highest = ref.Round
	}

	return true
}

func (s *State) Get(ref BlockRef) (*Block, bool) {
	block, exists := s.blocks[ref]
	if !exists {
		return nil, false
	}

	if block == nil && s.loader != nil {
		var err error

		block, err = s.loader(ref)
		if err != nil {
			s.loadErr = err
			return nil, false
		}

		s.blocks[ref] = block
		s.rounds[ref.Round][ref] = block
		s.loaded[ref] = true
	}

	return block, block != nil
}

func (s *State) AtRound(round Round) []*Block {
	var refs []BlockRef

	for ref := range s.rounds[round] {
		refs = append(refs, ref)
	}

	sort.Slice(refs, func(i, j int) bool {
		return LessRef(refs[i], refs[j])
	})

	var blocks []*Block

	for _, ref := range refs {
		if block, ok := s.Get(ref); ok {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

func (s *State) SetLoader(loader func(BlockRef) (*Block, error)) {
	s.loader = loader
}

func (s *State) Error() error {
	return s.loadErr
}

func (s *State) CachedBlocks() int {
	return len(s.loaded)
}

// UnloadBelow drops only cached bodies. Graph indexes and durable evidence remain.
func (s *State) UnloadBelow(round Round) {
	if s.loader == nil {
		return
	}

	for ref := range s.loaded {
		if ref.Round > 0 && ref.Round < round {
			s.blocks[ref] = nil
			s.rounds[ref.Round][ref] = nil
			delete(s.loaded, ref)
		}
	}
}

func (s *State) AtSlot(author core.NodeID, round Round) []*Block {
	var blocks []*Block

	for _, block := range s.AtRound(round) {
		if block.Author == author {
			blocks = append(blocks, block)
		}
	}

	return blocks
}

func (s *State) HighestRound() Round {
	return s.highest
}

func (s *State) Heads() []BlockRef {
	latest := make(map[core.NodeID]BlockRef)
	for ref := range s.blocks {
		old, ok := latest[ref.Author]
		if !ok || ref.Round > old.Round || ref.Round == old.Round && LessRef(ref, old) {
			latest[ref.Author] = ref
		}
	}

	var heads []BlockRef

	for _, ref := range latest {
		if ref.Round > 0 {
			heads = append(heads, ref)
		}
	}

	sort.Slice(heads, func(i, j int) bool {
		return LessRef(heads[i], heads[j])
	})
	return heads
}
