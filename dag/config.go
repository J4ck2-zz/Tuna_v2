package dag

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"sort"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

type Parameters struct {
	CacheRounds   int `json:"cache_rounds"`
	LeaderCount   int `json:"leader_count"`
	LeaderTimeout int `json:"leader_timeout_ms"`
	MinBlockDelay int `json:"min_block_delay_ms"`
	SyncInterval  int `json:"sync_interval_ms"`
	MaxPayloads   int `json:"max_payloads_per_block"`
	MaxParents    int `json:"max_parents"`
	MaxBlockBytes int `json:"max_block_bytes"`
	MaxPending    int `json:"max_pending_blocks"`
}

var DefaultParameters = Parameters{
	CacheRounds:   100,
	LeaderCount:   2,
	LeaderTimeout: 300,
	MinBlockDelay: 20,
	SyncInterval:  100,
	MaxPayloads:   32,
	MaxParents:    1024,
	MaxBlockBytes: 1 << 20,
	MaxPending:    10000,
}

func Members(committee core.Committee) []core.NodeID {
	ids := make([]core.NodeID, 0, committee.Size())
	for id := range committee.Authorities {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func (p Parameters) Validate(committee core.Committee) error {
	n := committee.Size()
	if n == 0 {
		return fmt.Errorf("empty committee")
	}

	if p.LeaderCount < 1 || p.LeaderCount > n {
		return fmt.Errorf("leader_count must be in [1,%d]", n)
	}

	if p.LeaderTimeout <= 0 || p.SyncInterval <= 0 || p.MinBlockDelay < 0 {
		return fmt.Errorf("invalid consensus timers")
	}

	if p.CacheRounds < 0 ||
		p.MaxPayloads < 1 ||
		p.MaxPending < 1 ||
		p.MaxParents < n+1 ||
		p.MaxBlockBytes < 256 ||
		p.MaxBlockBytes > 1<<20 {
		return fmt.Errorf("invalid consensus limits")
	}

	if 192+p.MaxParents*48+p.MaxPayloads*32 > p.MaxBlockBytes {
		return fmt.Errorf("block byte limit is smaller than configured parent/payload limits")
	}

	seen := make(map[string]bool)
	for id, a := range committee.Authorities {
		if id < 0 || len(a.Name.Pubkey) != ed25519.PublicKeySize {
			return fmt.Errorf("invalid committee authority %d", id)
		}

		key := string(a.Name.Pubkey)
		if seen[key] {
			return fmt.Errorf("duplicate committee public key")
		}

		seen[key] = true
	}

	return nil
}

func Domain(committee core.Committee, leaderCount int) crypto.Digest {
	h := crypto.NewHasher()
	h.Add([]byte("Tuna_v2/consensus/v1"))

	var word [8]byte

	binary.BigEndian.PutUint64(word[:], uint64(leaderCount))
	h.Add(word[:])
	for _, id := range Members(committee) {
		binary.BigEndian.PutUint64(word[:], uint64(id))
		h.Add(word[:])
		h.Add(committee.Name(id).Pubkey)
	}

	return h.Sum256(nil)
}
