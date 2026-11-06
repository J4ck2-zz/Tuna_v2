package dag

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/store"
)

const repositoryPrefix = "tuna_v2/v1/"

type Repository struct {
	store  *store.Store
	domain crypto.Digest
	owner  core.NodeID
}

type repositoryIdentity struct {
	Domain crypto.Digest
	Owner  core.NodeID
}

type Recovery struct {
	Blocks  []*Block
	Own     *BlockRef
	Cursor  *Slot
	Commits []CommittedSubDag
}

func OpenRepository(storage *store.Store, domain crypto.Digest, owner core.NodeID) (*Repository, error) {
	identity := repositoryIdentity{
		Domain: domain,
		Owner:  owner,
	}
	key := []byte(repositoryPrefix + "identity")
	data, err := storage.Read(key)
	if errors.Is(err, store.ErrNotFoundKey) {
		data, _ = json.Marshal(identity)
		err = storage.Write(key, data)
	} else if err == nil {
		var previous repositoryIdentity

		if err = json.Unmarshal(data, &previous); err == nil && previous != identity {
			err = fmt.Errorf("database belongs to a different node, committee, or protocol configuration")
		}
	}

	if err != nil {
		return nil, err
	}

	return &Repository{
		store:  storage,
		domain: domain,
		owner:  owner,
	}, nil
}

func blockKey(ref BlockRef) []byte {
	return []byte(repositoryPrefix + "block/" + hex.EncodeToString(ref.Digest[:]))
}

func (r *Repository) SaveBlock(block *Block, own bool) error {
	data, err := block.Encode()
	if err != nil {
		return err
	}

	entries := []store.Entry{
		{
			Key:   blockKey(block.Ref()),
			Value: data,
		},
	}
	if own {
		ref, _ := json.Marshal(block.Ref())
		entries = append(entries, store.Entry{
			Key:   []byte(repositoryPrefix + "own"),
			Value: ref,
		})
	}

	return r.store.WriteBatch(entries)
}

func (r *Repository) SaveDecision(slot Slot, commit *CommittedSubDag) error {
	cursor, _ := json.Marshal(slot)
	entries := []store.Entry{
		{
			Key:   []byte(repositoryPrefix + "cursor"),
			Value: cursor,
		},
	}
	if commit != nil {
		data, err := json.Marshal(commit)
		if err != nil {
			return err
		}

		entries = append(
			entries,
			store.Entry{
				Key:   []byte(fmt.Sprintf("%scommit/%020d", repositoryPrefix, commit.Index)),
				Value: data,
			},
		)
	}

	return r.store.WriteBatch(entries)
}

func (r *Repository) ReadCommits(after uint64) ([]CommittedSubDag, error) {
	entries, err := r.store.Scan([]byte(repositoryPrefix + "commit/"))
	if err != nil {
		return nil, err
	}

	var commits []CommittedSubDag

	for _, entry := range entries {
		var commit CommittedSubDag

		if err := json.Unmarshal(entry.Value, &commit); err != nil {
			return nil, err
		}

		if commit.Index > after {
			commits = append(commits, commit)
		}
	}

	sort.Slice(commits, func(i, j int) bool {
		return commits[i].Index < commits[j].Index
	})
	for i, commit := range commits {
		if commit.Index != after+uint64(i)+1 {
			return nil, fmt.Errorf("non-contiguous commit journal")
		}
	}

	return commits, nil
}

func (r *Repository) Recover() (Recovery, error) {
	var recovery Recovery

	entries, err := r.store.Scan([]byte(repositoryPrefix + "block/"))
	if err != nil {
		return recovery, err
	}

	for _, entry := range entries {
		block, err := DecodeBlock(entry.Value)
		if err != nil {
			return recovery, err
		}

		if !bytes.Equal(entry.Key, blockKey(block.Ref())) {
			return recovery, fmt.Errorf("recovered block index mismatch")
		}

		if block.Domain != r.domain {
			return recovery, fmt.Errorf("recovered block domain mismatch")
		}

		recovery.Blocks = append(recovery.Blocks, block)
	}

	sort.Slice(
		recovery.Blocks,
		func(i, j int) bool {
			return LessRef(recovery.Blocks[i].Ref(), recovery.Blocks[j].Ref())
		},
	)
	if data, err := r.store.Read([]byte(repositoryPrefix + "own")); err == nil {
		var ref BlockRef

		if err = json.Unmarshal(data, &ref); err != nil {
			return recovery, err
		}

		recovery.Own = &ref
	} else if !errors.Is(err, store.ErrNotFoundKey) {
		return recovery, err
	}

	if data, err := r.store.Read([]byte(repositoryPrefix + "cursor")); err == nil {
		var slot Slot

		if err = json.Unmarshal(data, &slot); err != nil {
			return recovery, err
		}

		recovery.Cursor = &slot
	} else if !errors.Is(err, store.ErrNotFoundKey) {
		return recovery, err
	}

	recovery.Commits, err = r.ReadCommits(0)
	return recovery, err
}

func (r *Repository) ReadBlock(ref BlockRef) (*Block, error) {
	data, err := r.store.Read(blockKey(ref))
	if err != nil {
		return nil, err
	}

	block, err := DecodeBlock(data)
	if err != nil {
		return nil, err
	}

	if block.Ref() != ref || block.Domain != r.domain {
		return nil, fmt.Errorf("stored block reference mismatch")
	}

	return block, nil
}
