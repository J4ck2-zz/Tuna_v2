package mempool

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/network"
	"Tuna_v2/pool"
	"Tuna_v2/store"
)

type memoryDB map[string][]byte

func (db memoryDB) Put(key, value []byte) error {
	db[string(key)] = value
	return nil
}

func (db memoryDB) Get(key []byte) ([]byte, error) {
	if value, exists := db[string(key)]; exists {
		return value, nil
	}

	return nil, store.ErrNotFoundKey
}

func TestPayloadTransferAndRetrieval(t *testing.T) {
	private, public := crypto.GenED25519Keys()
	committee := core.Committee{
		Authorities: map[core.NodeID]core.Authority{
			1: {
				Name: crypto.PublickKey{
					Pubkey: public,
				},
				MempoolAddr: "127.0.0.1:7001",
			},
		},
	}
	service := crypto.NewSigService(crypto.PrivateKey{
		Prikey: private,
	}, crypto.SecretShareKey{})
	original, err := NewPload(1, pool.Batch{
		ID:  7,
		Txs: []pool.Transaction{[]byte("transaction")},
	}, service)
	if err != nil {
		t.Fatal(err)
	}

	var wire bytes.Buffer

	codec := network.NewCodec(DefaultMessageTypeMap).Bind(&wire)
	if err := codec.Write(&OtherPayloadMsg{
		Payload: original,
	}); err != nil {
		t.Fatal(err)
	}

	decoded, err := codec.Read()
	if err != nil {
		t.Fatal(err)
	}

	transmitter := &Transmit{
		committee: committee,
		msgCh:     make(chan *network.NetMessage, 10),
	}
	m := &Mempool{
		Parameters: DefaultParameters,
		Committee:  committee,
		Store:      store.NewStore(memoryDB{}),
		Transimtor: transmitter,
		muQueue:    &sync.RWMutex{},
		Queue:      make(map[crypto.Digest]struct{}),
		SelfQueue:  make(map[crypto.Digest]struct{}),
	}
	if err := m.HandleOthorPayload(decoded.(*OtherPayloadMsg)); err != nil {
		t.Fatal(err)
	}

	stored, err := m.GetPayload(original.Hash())
	if err != nil || stored == nil || !stored.Verify(committee) {
		t.Fatalf("invalid stored payload: %v", err)
	}

	if !reflect.DeepEqual(stored.Batch, original.Batch) {
		t.Fatal("transactions changed in transit")
	}

	if err := m.HandleRequestPayload(&RequestPayloadMsg{
		Author: 1,
		Digests: []crypto.Digest{
			original.Hash(),
			{99},
		},
	}); err != nil {
		t.Fatal(err)
	}

	reply := (<-transmitter.msgCh).Msg.(*ReplyPayloadRequestMsg)
	if len(reply.Payload) != 1 || reply.Payload[0].Hash() != original.Hash() {
		t.Fatal("payload retrieval returned incorrect data")
	}

	selected := m.TakePayloads(1)
	if len(selected) != 1 || selected[0] != original.Hash() || len(m.TakePayloads(1)) != 0 {
		t.Fatal("payload consumption did not remove the queued reference")
	}

	if _, err := m.GetPayload(original.Hash()); err != nil {
		t.Fatal("consumption removed stored payload")
	}
}

func TestSynchronizerRequestsPayloadsAndReportsReadiness(t *testing.T) {
	storage := store.NewStore(memoryDB{})
	transmitter := &Transmit{
		committee: core.Committee{
			Authorities: map[core.NodeID]core.Authority{
				1: {
					MempoolAddr: "127.0.0.1:7001",
				},
			},
		},
		msgCh: make(chan *network.NetMessage, 10),
	}
	parameters := DefaultParameters
	parameters.RetryDelay = 5
	parameters.RequestPloadDelay = 0
	synchronizer := NewSynchronizer(0, transmitter, parameters, storage)
	payloadID, requestID := crypto.Digest{1}, crypto.Digest{2}
	if status := synchronizer.Verify(1, []crypto.Digest{payloadID}, requestID); status != Wait {
		t.Fatal("missing payload was accepted")
	}

	go synchronizer.Run()
	select {
	case message := <-transmitter.msgCh:
		request := message.Msg.(*RequestPayloadMsg)
		if len(request.Digests) != 1 || request.Digests[0] != payloadID {
			t.Fatal("incorrect payload request")
		}
	case <-time.After(time.Second):
		t.Fatal("missing payload was not requested")
	}

	if err := storage.Write(payloadKey(payloadID), []byte("payload")); err != nil {
		t.Fatal(err)
	}

	select {
	case ready := <-synchronizer.Ready:
		if ready != requestID {
			t.Fatal("readiness notification changed the opaque request ID")
		}
	case <-time.After(time.Second):
		t.Fatal("payload arrival was not reported")
	}

	if status := synchronizer.Verify(1, []crypto.Digest{payloadID}, requestID); status != OK {
		t.Fatal("stored payload was not accepted")
	}
}

func TestPayloadWaitCanBeCancelled(t *testing.T) {
	storage := store.NewStore(memoryDB{})
	cancel := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		result <- waitForPayloads([]crypto.Digest{
			{1},
		}, storage, cancel)
	}()
	close(cancel)
	select {
	case ready := <-result:
		if ready {
			t.Fatal("cancelled request reported readiness")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled payload wait did not return")
	}
}

func TestPayloadHashHasUnambiguousFields(t *testing.T) {
	// The old variable-width binary string concatenation collided for (1,2)/(3,0).
	first := &Payload{
		Proposer: 1,
		Batch: pool.Batch{
			ID:  2,
			Txs: []pool.Transaction{[]byte("same")},
		},
	}
	second := &Payload{
		Proposer: 3,
		Batch: pool.Batch{
			ID:  0,
			Txs: []pool.Transaction{[]byte("same")},
		},
	}
	if first.Hash() == second.Hash() {
		t.Fatal("payload identity fields are ambiguous")
	}

	second.Proposer = first.Proposer
	second.Batch.ID = first.Batch.ID
	second.Batch.Txs = []pool.Transaction{[]byte("sa"), []byte("me")}
	if first.Hash() == second.Hash() {
		t.Fatal("transaction boundaries are ambiguous")
	}
}

func TestSelectedPayloadCanBeRestoredAndRetiredPayloadStaysRemoved(t *testing.T) {
	private, public := crypto.GenED25519Keys()
	committee := core.Committee{
		Authorities: map[core.NodeID]core.Authority{
			1: {
				Name: crypto.PublickKey{
					Pubkey: public,
				},
			},
		},
	}
	signer := crypto.NewSigService(crypto.PrivateKey{
		Prikey: private,
	}, crypto.SecretShareKey{})
	payload, _ := NewPload(1, pool.Batch{
		ID:  1,
		Txs: []pool.Transaction{[]byte("tx")},
	}, signer)
	m := &Mempool{
		Name:       0,
		Committee:  committee,
		Parameters: DefaultParameters,
		Store:      store.NewStore(store.NewMemoryDB()),
		muQueue:    &sync.RWMutex{},
		SelfQueue:  make(map[crypto.Digest]struct{}),
		Queue:      make(map[crypto.Digest]struct{}),
	}
	if err := m.HandleOthorPayload(&OtherPayloadMsg{
		Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}

	selected := m.TakePayloads(1)
	if len(selected) != 1 {
		t.Fatal("payload not selected")
	}

	m.RestorePayloads(selected)
	if len(m.TakePayloads(1)) != 1 {
		t.Fatal("failed proposal lost its payload")
	}

	m.RemovePayloads(selected)
	m.HandleOthorPayload(&OtherPayloadMsg{
		Payload: payload,
	})
	if len(m.TakePayloads(1)) != 0 {
		t.Fatal("late duplicate reinserted a retired payload")
	}
}

type countingDB struct {
	*store.MemoryDB
	writes int
}

func (db *countingDB) Put(key, value []byte) error {
	db.writes++
	return db.MemoryDB.Put(key, value)
}

func TestRepeatedPayloadRepliesDoNotRewriteDurableData(t *testing.T) {
	private, public := crypto.GenED25519Keys()
	committee := core.Committee{
		Authorities: map[core.NodeID]core.Authority{
			1: {
				Name: crypto.PublickKey{
					Pubkey: public,
				},
			},
		},
	}
	signer := crypto.NewSigService(crypto.PrivateKey{
		Prikey: private,
	}, crypto.SecretShareKey{})
	payload, _ := NewPload(1, pool.Batch{
		ID:  1,
		Txs: []pool.Transaction{[]byte("tx")},
	}, signer)
	backend := &countingDB{
		MemoryDB: store.NewMemoryDB(),
	}
	m := &Mempool{
		Committee: committee,
		Store:     store.NewStore(backend),
	}
	for i := 0; i < 10; i++ {
		if err := m.StorePayload(payload); err != nil {
			t.Fatal(err)
		}
	}

	if backend.writes != 1 {
		t.Fatalf("duplicate replies wrote %d transactions", backend.writes)
	}
}

func TestOverlappingBlockRequestsShareOnePayloadRetry(t *testing.T) {
	storage := store.NewStore(store.NewMemoryDB())
	transmitter := &Transmit{
		committee: core.Committee{
			Authorities: map[core.NodeID]core.Authority{
				1: {
					MempoolAddr: "127.0.0.1:7001",
				},
			},
		},
		msgCh: make(chan *network.NetMessage, 10),
	}
	parameters := DefaultParameters
	parameters.RequestPloadDelay = 0
	parameters.RetryDelay = 1000
	synchronizer := NewSynchronizer(0, transmitter, parameters, storage)
	digest := crypto.Digest{1}
	first, second := crypto.Digest{2}, crypto.Digest{3}
	synchronizer.Verify(1, []crypto.Digest{digest}, first)
	synchronizer.Verify(1, []crypto.Digest{digest}, second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	defer func() {
		cancel()
		<-done
	}()
	go func() {
		synchronizer.RunContext(ctx)
		close(done)
	}()
	select {
	case message := <-transmitter.msgCh:
		if len(message.Msg.(*RequestPayloadMsg).Digests) != 1 {
			t.Fatal("duplicate payload digests in request")
		}
	case <-time.After(time.Second):
		t.Fatal("no payload request")
	}

	select {
	case <-transmitter.msgCh:
		t.Fatal("overlapping blocks generated duplicate initial requests")
	case <-time.After(40 * time.Millisecond):
	}

	storage.Write(payloadKey(digest), []byte("arrived"))
	ready := make(map[crypto.Digest]bool)
	for len(ready) < 2 {
		select {
		case id := <-synchronizer.Ready:
			ready[id] = true
		case <-time.After(time.Second):
			t.Fatal("not all waiting blocks were notified")
		}
	}

	if !ready[first] || !ready[second] {
		t.Fatal("wrong readiness notifications")
	}
}
