package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/logger"
)

type Runtime struct {
	Core         *Core
	Transport    *core.Transmitor
	PayloadReady <-chan crypto.Digest
	OnCommit     func(context.Context, CommittedSubDag) error
	blockedAt    time.Time
	blockedSlot  *Slot
}

func (r *Runtime) request(to core.NodeID, refs []BlockRef) {
	for len(refs) > 0 {
		end := min(64, len(refs))
		r.Transport.Send(r.Core.ID, to, &BlockRequest{
			Author: r.Core.ID,
			Refs:   append([]BlockRef(nil), refs[:end]...),
		})
		refs = refs[end:]
	}
}

func (r *Runtime) receive(message core.Message) {
	switch message := message.(type) {
	case *BlockMessage:
		if err := r.Core.Add(message.Block); err != nil {
			logger.Debug.Printf("Rejected DAG block: %v", err)
		}
	case *BlockResponse:
		if len(message.Blocks) > 4 {
			return
		}
		for _, block := range message.Blocks {
			if err := r.Core.Add(block); err != nil {
				logger.Debug.Printf("Rejected fetched block: %v", err)
			}
		}
	case *BlockRequest:
		if len(message.Refs) > 64 {
			return
		}
		if _, ok := r.Core.committee.Authorities[message.Author]; !ok || message.Author == r.Core.ID {
			return
		}
		var blocks []*Block
		flush := func() {
			if len(blocks) > 0 {
				r.Transport.Send(r.Core.ID, message.Author, &BlockResponse{
					Blocks: blocks,
				})
				blocks = nil
			}
		}
		for _, ref := range message.Refs {
			if ref.Round == 0 {
				continue
			}

			if block, ok := r.Core.State.Get(ref); ok {
				blocks = append(blocks, block)
				if len(blocks) == 4 {
					flush()
				}
			}
		}
		flush()
	case *StatusMessage:
		if message.Domain != r.Core.Domain || len(message.Heads) > r.Core.committee.Size() {
			return
		}
		if _, ok := r.Core.committee.Authorities[message.Author]; !ok || message.Author == r.Core.ID {
			return
		}
		var missing []BlockRef
		for _, ref := range message.Heads {
			if _, known := r.Core.committee.Authorities[ref.Author]; !known {
				continue
			}

			if _, exists := r.Core.State.Get(ref); !exists {
				if _, waiting := r.Core.Manager.pending[ref]; !waiting {
					missing = append(missing, ref)
				}
			}
		}
		r.request(message.Author, missing)
	}
}

func (r *Runtime) step(ctx context.Context, now time.Time) error {
	effects, err := r.Core.Step(now)
	if err != nil {
		return err
	}

	var sendErr error

	if effects.Proposed != nil {
		block := effects.Proposed
		logger.Info.Printf(
			"Tuna_v2 proposal round=%d author=%d hash=%x payloads=%d",
			block.Round,
			block.Author,
			block.Hash(),
			len(block.Payloads),
		)
		sendErr = r.Transport.Send(r.Core.ID, core.NONE, &BlockMessage{
			Block: block,
		})
	}

	for _, decision := range effects.Decisions {
		logger.Debug.Printf(
			"Tuna_v2 decision round=%d offset=%d author=%d state=%s",
			decision.Slot.Round,
			decision.Slot.Offset,
			decision.Slot.Author,
			decision.Kind,
		)
	}

	for _, commit := range effects.Commits {
		if r.OnCommit != nil {
			if err := r.OnCommit(ctx, commit); err != nil {
				return err
			}
		}
	}

	return sendErr
}

func (r *Runtime) report(now time.Time) {
	stats := r.Core.Stats()
	if stats.FirstUndecided != nil {
		if r.blockedSlot == nil || *stats.FirstUndecided != *r.blockedSlot {
			copy := *stats.FirstUndecided
			r.blockedSlot = &copy
			r.blockedAt = now
		}
	} else {
		r.blockedSlot = nil
		r.blockedAt = time.Time{}
	}

	var blockedMS int64

	if !r.blockedAt.IsZero() {
		blockedMS = now.Sub(r.blockedAt).Milliseconds()
	}

	data, _ := json.Marshal(struct {
		Stats
		BlockedMS int64 `json:"blocked_ms"`
	}{stats, blockedMS})
	logger.Info.Printf("Tuna_v2 stats %s", data)
}

func (r *Runtime) Run(ctx context.Context) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	syncTick := time.NewTicker(time.Duration(r.Core.parameters.SyncInterval) * time.Millisecond)
	defer syncTick.Stop()
	reportTick := time.NewTicker(time.Second)
	defer reportTick.Stop()
	if err := r.step(ctx, time.Now()); err != nil {
		return err
	}

	r.report(time.Now())
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-r.Transport.RecvChannel():
			if !ok {
				if ctx.Err() != nil {
					return nil
				}

				return fmt.Errorf("DAG receiver closed")
			}
			r.receive(message)
		case <-r.PayloadReady:
		case <-tick.C:
		case now := <-reportTick.C:
			r.report(now)
		case <-syncTick.C:
			r.Transport.Send(
				r.Core.ID,
				core.NONE,
				&StatusMessage{
					Author: r.Core.ID,
					Domain: r.Core.Domain,
					Heads:  r.Core.State.Heads(),
				},
			)
			r.request(core.NONE, r.Core.Manager.Missing(r.Core.State))
		}

		if err := r.step(ctx, time.Now()); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}
	}
}
