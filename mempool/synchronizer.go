package mempool

import (
	"context"
	"time"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/store"
)

type payloadSyncRequest struct {
	ID      crypto.Digest
	Missing []crypto.Digest
	Author  core.NodeID
}

type cancelPayloadSync struct {
	ID crypto.Digest
}

type Synchronizer struct {
	Name       core.NodeID
	Store      *store.Store
	Transimtor *Transmit
	Ready      chan crypto.Digest
	Parameters Parameters
	interChan  chan any
}

func NewSynchronizer(name core.NodeID, transmitter *Transmit, parameters Parameters, storage *store.Store) *Synchronizer {
	return &Synchronizer{
		Name:       name,
		Store:      storage,
		Transimtor: transmitter,
		Parameters: parameters,
		Ready:      make(chan crypto.Digest, 1000),
		interChan:  make(chan any, 1000),
	}
}

func (s *Synchronizer) Cancel(id crypto.Digest) {
	select {
	case s.interChan <- cancelPayloadSync{
		ID: id,
	}:
	default:
	}
}

func (s *Synchronizer) missing(digests []crypto.Digest) []crypto.Digest {
	var result []crypto.Digest

	for _, digest := range digests {
		if _, err := s.Store.Read(payloadKey(digest)); err != nil {
			result = append(result, digest)
		}
	}

	return result
}

func (s *Synchronizer) Verify(author core.NodeID, digests []crypto.Digest, id crypto.Digest) VerifyStatus {
	missing := s.missing(digests)
	if len(missing) == 0 {
		return OK
	}

	select {
	case s.interChan <- payloadSyncRequest{
		ID:      id,
		Missing: missing,
		Author:  author,
	}:
	default:
	}

	return Wait
}

func (s *Synchronizer) Run() {
	s.RunContext(context.Background())
}

func (s *Synchronizer) RunContext(ctx context.Context) {
	type retryState struct {
		next    time.Time
		attempt int
		author  core.NodeID
	}

	pending := make(map[crypto.Digest]payloadSyncRequest)
	retries := make(map[crypto.Digest]*retryState)
	number := 0
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-s.interChan:
			switch req := message.(type) {
			case payloadSyncRequest:
				if _, exists := pending[req.ID]; !exists {
					pending[req.ID] = req
				}
			case cancelPayloadSync:
				delete(pending, req.ID)
			}
		case now := <-ticker.C:
			// Many DAG blocks may reference the same payload. Schedule one retry per digest,
			// not one per waiting block, and contact its source before falling back to peers.
			needed := make(map[crypto.Digest]bool)
			requests := make(map[core.NodeID][]crypto.Digest)
			for id, entry := range pending {
				missing := s.missing(entry.Missing)
				if len(missing) == 0 {
					delete(pending, id)
					select {
					case s.Ready <- id:
					case <-ctx.Done():
						return
					}

					continue
				}

				for _, digest := range missing {
					if needed[digest] {
						continue
					}

					needed[digest] = true
					retry := retries[digest]
					if retry == nil {
						retry = &retryState{
							next:   now.Add(time.Duration(s.Parameters.RequestPloadDelay) * time.Millisecond),
							author: entry.Author,
						}
						retries[digest] = retry
					}

					if now.Before(retry.next) {
						continue
					}

					recipient := retry.author
					if retry.attempt > 0 {
						recipient = core.NONE
					}

					requests[recipient] = append(requests[recipient], digest)
					retry.attempt++
					retry.next = now.Add(time.Duration(s.Parameters.RetryDelay) * time.Millisecond)
				}
			}
			for digest := range retries {
				if !needed[digest] {
					delete(retries, digest)
				}
			}
			for recipient, digests := range requests {
				for len(digests) > 0 {
					end := min(64, len(digests))
					request := &RequestPayloadMsg{
						Author:  s.Name,
						Digests: digests[:end],
						ReqId:   number,
					}
					s.Transimtor.MempoolSend(s.Name, recipient, request)
					number++
					digests = digests[end:]
				}
			}
		}
	}
}

func waitForPayloads(missing []crypto.Digest, storage *store.Store, cancel <-chan struct{}) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		for _, digest := range missing {
			if _, err := storage.Read(payloadKey(digest)); err != nil {
				ready = false
				break
			}
		}

		if ready {
			return true
		}

		select {
		case <-cancel:
			return false
		case <-ticker.C:
		}
	}
}
