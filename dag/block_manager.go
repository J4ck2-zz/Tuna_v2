package dag

import (
	"fmt"
	"sort"
)

// 待接收区块管理器
type BlockManager struct {
	pending map[BlockRef]*Block
	limit   int
}

func NewBlockManager(limit int) *BlockManager {
	return &BlockManager{
		pending: make(map[BlockRef]*Block),
		limit:   limit,
	}
}

func (m *BlockManager) Add(reader Reader, block *Block) error {
	ref := block.Ref()
	if _, ok := reader.Get(ref); ok {
		return nil
	}

	if _, ok := m.pending[ref]; ok {
		return nil
	}

	if len(m.pending) >= m.limit {
		return fmt.Errorf("pending block limit reached")
	}

	m.pending[ref] = block.Clone()
	return nil
}

func (m *BlockManager) Ready(reader Reader) []*Block {
	var ready []*Block

	for _, block := range m.pending {
		complete := true
		for _, ref := range block.Parents {
			if _, ok := reader.Get(ref); !ok {
				complete = false
				break
			}
		}

		if complete {
			ready = append(ready, block)
		}
	}

	sort.Slice(ready, func(i, j int) bool {
		return LessRef(ready[i].Ref(), ready[j].Ref())
	})
	return ready
}

func (m *BlockManager) Remove(ref BlockRef) {
	delete(m.pending, ref)
}

func (m *BlockManager) Missing(reader Reader) []BlockRef {
	missing := make(map[BlockRef]bool)
	for _, block := range m.pending {
		for _, ref := range block.Parents {
			if _, ok := reader.Get(ref); !ok {
				if _, waiting := m.pending[ref]; !waiting {
					missing[ref] = true
				}
			}
		}
	}

	var refs []BlockRef

	for ref := range missing {
		refs = append(refs, ref)
	}

	sort.Slice(refs, func(i, j int) bool {
		return LessRef(refs[i], refs[j])
	})
	return refs
}

func (m *BlockManager) Len() int {
	return len(m.pending)
}
