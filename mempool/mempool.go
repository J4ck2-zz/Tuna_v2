package mempool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/logger"
	"Tuna_v2/network"
	"Tuna_v2/pool"
	"Tuna_v2/store"
)

type Mempool struct {
	Name        core.NodeID
	Committee   core.Committee
	Parameters  Parameters
	SigService  *crypto.SigService
	Store       *store.Store
	TxPool      *pool.Pool
	Transimtor  *Transmit
	muQueue     *sync.RWMutex
	SelfQueue   map[crypto.Digest]struct{}
	Queue       map[crypto.Digest]struct{}
	Sync        *Synchronizer
	availableAt map[crypto.Digest]time.Time
	retired     map[crypto.Digest]bool
	done        chan struct{}
}

func NewMempool(
	name core.NodeID,
	committee core.Committee,
	parameters Parameters,
	signer *crypto.SigService,
	storage *store.Store,
	txPool *pool.Pool,
) *Mempool {
	m := &Mempool{
		Name:        name,
		Committee:   committee,
		Parameters:  parameters,
		SigService:  signer,
		Store:       storage,
		TxPool:      txPool,
		muQueue:     &sync.RWMutex{},
		SelfQueue:   make(map[crypto.Digest]struct{}),
		Queue:       make(map[crypto.Digest]struct{}),
		availableAt: make(map[crypto.Digest]time.Time),
		retired:     make(map[crypto.Digest]bool),
		done:        make(chan struct{}),
	}
	_, port, err := net.SplitHostPort(committee.MempoolAddress(name))
	if err != nil {
		panic(err)
	}

	codec := network.NewCodec(DefaultMessageTypeMap)
	m.Transimtor = NewTransmit(network.NewSender(codec), network.NewReceiver(net.JoinHostPort("", port), codec), committee)
	m.Sync = NewSynchronizer(name, m.Transimtor, parameters, storage)
	return m
}

func payloadKey(digest crypto.Digest) []byte {
	return append([]byte("tuna_v2/payload/"), digest[:]...)
}

func (m *Mempool) StorePayload(payload *Payload) error {
	if payload == nil || !payload.Verify(m.Committee) || payload.Batch.ID < 0 {
		return fmt.Errorf("invalid payload")
	}

	if len(payload.Batch.Txs) > 65536 {
		return fmt.Errorf("too many transactions in payload")
	}

	size := 0
	for _, tx := range payload.Batch.Txs {
		size += len(tx)
		if size > 1<<20 {
			return fmt.Errorf("payload exceeds 1 MiB")
		}
	}

	digest := payload.Hash()
	if _, err := m.Store.Read(payloadKey(digest)); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFoundKey) {
		return err
	}

	data, err := payload.Encode()
	if err != nil {
		return err
	}

	if len(data) > 2<<20 {
		return fmt.Errorf("encoded payload too large")
	}

	return m.Store.Write(payloadKey(payload.Hash()), data)
}

func (m *Mempool) GetPayload(digest crypto.Digest) (*Payload, error) {
	data, err := m.Store.Read(payloadKey(digest))
	if errors.Is(err, store.ErrNotFoundKey) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	payload := &Payload{}
	if err := payload.Decode(data); err != nil {
		return nil, err
	}

	if payload.Hash() != digest || !payload.Verify(m.Committee) {
		return nil, fmt.Errorf("corrupt payload %x", digest)
	}

	return payload, nil
}

func ErrFullMemory(author core.NodeID) error {
	return fmt.Errorf("author %d payload queue full", author)
}

func (m *Mempool) enqueue(payload *Payload, restore bool) error {
	digest := payload.Hash()
	m.muQueue.Lock()
	defer m.muQueue.Unlock()
	if m.retired == nil {
		m.retired = make(map[crypto.Digest]bool)
	}

	if m.availableAt == nil {
		m.availableAt = make(map[crypto.Digest]time.Time)
	}

	if restore {
		delete(m.retired, digest)
	} else if m.retired[digest] {
		return nil
	}

	if _, ok := m.SelfQueue[digest]; ok {
		return nil
	}

	if _, ok := m.Queue[digest]; ok {
		return nil
	}

	if uint64(len(m.SelfQueue)+len(m.Queue)) >= m.Parameters.MaxMempoolQueenSize {
		return ErrFullMemory(m.Name)
	}

	if payload.Proposer == m.Name {
		m.SelfQueue[digest] = struct{}{}
		m.availableAt[digest] = time.Now().Add(time.Duration(m.Parameters.PayloadDelay) * time.Millisecond)
	} else {
		m.Queue[digest] = struct{}{}
	}

	return nil
}

func (m *Mempool) HandleOwnPayload(message *OwnPayloadMsg) error {
	if message == nil || message.Payload == nil || message.Payload.Proposer != m.Name {
		return fmt.Errorf("invalid own payload")
	}

	if err := m.StorePayload(message.Payload); err != nil {
		return err
	}

	if err := m.enqueue(message.Payload, false); err != nil {
		return err
	}

	return m.Transimtor.MempoolSend(m.Name, core.NONE, &OtherPayloadMsg{
		Payload: message.Payload,
	})
}

func (m *Mempool) HandleOthorPayload(message *OtherPayloadMsg) error {
	if message == nil {
		return fmt.Errorf("nil payload message")
	}

	if err := m.StorePayload(message.Payload); err != nil {
		return err
	}

	if err := m.enqueue(message.Payload, false); err != nil {
		return err
	}

	logger.Info.Printf("receive batch %d from %d", message.Payload.Batch.ID, message.Payload.Proposer)
	return nil
}

func (m *Mempool) HandleRequestPayload(message *RequestPayloadMsg) error {
	if message == nil || len(message.Digests) > 64 {
		return fmt.Errorf("invalid payload request")
	}

	if _, exists := m.Committee.Authorities[message.Author]; !exists {
		return fmt.Errorf("unknown payload requester")
	}

	var payloads []*Payload

	for _, digest := range message.Digests {
		payload, err := m.GetPayload(digest)
		if err != nil {
			return err
		}

		if payload != nil {
			payloads = append(payloads, payload)
		}
	}

	for len(payloads) > 0 {
		end := min(4, len(payloads))
		if err := m.Transimtor.MempoolSend(m.Name, message.Author, &ReplyPayloadRequestMsg{
			Payload: payloads[:end],
			Author:  m.Name,
		}); err != nil {
			return err
		}

		payloads = payloads[end:]
	}

	return nil
}

func (m *Mempool) handleReplyPayloadMsg(message *ReplyPayloadRequestMsg) error {
	if message == nil || len(message.Payload) > 4 {
		return fmt.Errorf("invalid payload reply")
	}

	for _, payload := range message.Payload {
		if err := m.StorePayload(payload); err != nil {
			return err
		}

		if err := m.enqueue(payload, false); err != nil {
			logger.Debug.Println(err)
		}

		logger.Info.Printf("receive batch %d author %d from %d", payload.Batch.ID, payload.Proposer, message.Author)
	}

	return nil
}

func (m *Mempool) TakePayloads(limit int) []crypto.Digest {
	if limit <= 0 {
		return nil
	}

	m.muQueue.Lock()
	defer m.muQueue.Unlock()
	if m.retired == nil {
		m.retired = make(map[crypto.Digest]bool)
	}

	var result []crypto.Digest

	for _, queue := range []map[crypto.Digest]struct{}{m.SelfQueue, m.Queue} {
		for digest := range queue {
			if until, exists := m.availableAt[digest]; exists && time.Now().Before(until) {
				continue
			}

			delete(m.SelfQueue, digest)
			delete(m.Queue, digest)
			delete(m.availableAt, digest)
			m.retired[digest] = true
			result = append(result, digest)
			if len(result) == limit {
				return result
			}
		}
	}

	return result
}

func (m *Mempool) RestorePayloads(digests []crypto.Digest) {
	for _, digest := range digests {
		payload, err := m.GetPayload(digest)
		if err == nil && payload != nil {
			if err := m.enqueue(payload, true); err != nil {
				logger.Warn.Println(err)
			}
		}
	}
}

func (m *Mempool) RemovePayloads(digests []crypto.Digest) {
	m.muQueue.Lock()
	defer m.muQueue.Unlock()
	if m.retired == nil {
		m.retired = make(map[crypto.Digest]bool)
	}

	for _, digest := range digests {
		delete(m.Queue, digest)
		delete(m.SelfQueue, digest)
		delete(m.availableAt, digest)
		m.retired[digest] = true
	}
}

// Recover reconstructs unconsumed queues; the DAG then removes durable proposal/commit refs.
func (m *Mempool) Recover() error {
	entries, err := m.Store.Scan([]byte("tuna_v2/payload/"))
	if err != nil {
		return err
	}

	highest := -1
	for _, entry := range entries {
		payload := &Payload{}
		if err := payload.Decode(entry.Value); err != nil {
			return err
		}

		digest := payload.Hash()
		if !bytes.Equal(entry.Key, payloadKey(digest)) || !payload.Verify(m.Committee) {
			return fmt.Errorf("invalid recovered payload")
		}

		if payload.Proposer == m.Name && payload.Batch.ID > highest {
			highest = payload.Batch.ID
		}
		// Accepted DAG data may exceed the live queue cap; it remains available on disk.
		_ = m.enqueue(payload, false)
	}

	m.TxPool.ResumeAfterBatch(highest)
	return nil
}

func (m *Mempool) Start(ctx context.Context) error {
	if err := m.Transimtor.receiver.Start(ctx); err != nil {
		return err
	}

	var workers sync.WaitGroup

	launch := func(work func()) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			work()
		}()
	}
	launch(func() {
		m.Transimtor.sender.RunContext(ctx)
	})
	launch(func() {
		m.Transimtor.Run(ctx)
	})
	launch(func() {
		m.Sync.RunContext(ctx)
	})
	launch(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case batch := <-m.TxPool.BatchChannel():
				payload, err := NewPload(m.Name, batch, m.SigService)
				if err != nil {
					logger.Error.Println(err)
					continue
				}
				if err := m.HandleOwnPayload(payloadMessage(payload)); err != nil {
					logger.Warn.Println(err)
				}
			}
		}
	})
	launch(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-m.Transimtor.MempoolChannel():
				var err error
				switch message := message.(type) {
				case *OwnPayloadMsg:
					err = m.HandleOwnPayload(message)
				case *OtherPayloadMsg:
					err = m.HandleOthorPayload(message)
				case *RequestPayloadMsg:
					err = m.HandleRequestPayload(message)
				case *ReplyPayloadRequestMsg:
					err = m.handleReplyPayloadMsg(message)
				}
				if err != nil {
					logger.Warn.Printf("Payload: %v", err)
				}
			}
		}
	})
	go func() {
		workers.Wait()
		close(m.done)
	}()
	return nil
}

func payloadMessage(payload *Payload) *OwnPayloadMsg {
	return &OwnPayloadMsg{
		Payload: payload,
	}
}

func (m *Mempool) Wait() {
	<-m.done
}

func (m *Mempool) Run() {
	if err := m.Start(context.Background()); err != nil {
		panic(err)
	}

	m.Wait()
}
