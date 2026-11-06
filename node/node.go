package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"Tuna_v2/config"
	tunav2 "Tuna_v2/consensus/tuna_v2"
	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/dag"
	"Tuna_v2/logger"
	"Tuna_v2/mempool"
	"Tuna_v2/network"
	"Tuna_v2/pool"
	"Tuna_v2/store"
)

type Node struct {
	ID         core.NodeID
	Committee  core.Committee
	TxPool     *pool.Pool
	Mempool    *mempool.Mempool
	Store      *store.Store
	SigService *crypto.SigService
	DAG        *dag.Core
	// Set before RunContext. A slow consumer applies backpressure after durable commit.
	CommitConsumer chan<- dag.CommittedSubDag
	outputs        []io.Closer
}

func NewNode(
	keysFile, tssKeyFile, committeeFile, parametersFile, storePath, logPath string,
	logLevel, nodeID int,
) (*Node, error) {
	_, privateKey, err := config.GenKeysFromFile(keysFile)
	if err != nil {
		return nil, err
	}

	var shareKey crypto.SecretShareKey

	if tssKeyFile != "" {
		shareKey, err = config.GenTsKeyFromFile(tssKeyFile)
		if err != nil {
			return nil, err
		}
	}

	committee, err := config.GenCommitteeFromFile(committeeFile)
	if err != nil {
		return nil, err
	}

	id := core.NodeID(nodeID)
	authority, exists := committee.Authorities[id]
	if !exists {
		return nil, fmt.Errorf("node %d is not in committee", nodeID)
	}

	if !authority.Name.Pubkey.Equal(privateKey.Prikey.Public()) {
		return nil, fmt.Errorf("node key does not match committee identity")
	}

	parameters, err := config.LoadParameters(parametersFile)
	if err != nil {
		return nil, err
	}

	if err := parameters.Consensus.Validate(committee); err != nil {
		return nil, err
	}

	if err := parameters.Pool.Validate(); err != nil {
		return nil, err
	}

	if err := parameters.Mempool.Validate(); err != nil {
		return nil, err
	}

	if logPath == "" {
		logPath = "."
	}

	if err := os.MkdirAll(logPath, 0o700); err != nil {
		return nil, err
	}

	node := &Node{
		ID:        id,
		Committee: committee,
	}
	for _, output := range []struct {
		level logger.Level
		name  string
	}{
		{logger.InfoLevel, "info"},
		{logger.DebugLevel, "debug"},
		{logger.WarnLevel, "warn"},
		{logger.ErrorLevel, "error"},
	} {
		writer, err := os.OpenFile(
			filepath.Join(logPath, fmt.Sprintf("node-%s-%d.log", output.name, nodeID)),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND,
			0o600,
		)
		if err != nil {
			node.closeOutputs()
			return nil, err
		}

		node.outputs = append(node.outputs, writer)
		logger.SetOutput(output.level, writer)
	}

	logger.SetLevel(logger.Level(logLevel))
	backend, err := store.OpenNutsDB(storePath)
	if err != nil {
		node.closeOutputs()
		return nil, err
	}

	node.Store = store.NewStore(backend)
	fail := func(err error) (*Node, error) {
		node.Store.Close()
		node.closeOutputs()
		return nil, err
	}
	index := 0
	for i, member := range dag.Members(committee) {
		if member == id {
			index = i
		}
	}

	node.TxPool = pool.NewPool(parameters.Pool, committee.Size(), index)
	node.SigService = crypto.NewSigService(privateKey, shareKey)
	node.Mempool = mempool.NewMempool(id, committee, parameters.Mempool, node.SigService, node.Store, node.TxPool)
	if err := node.Mempool.Recover(); err != nil {
		return fail(err)
	}

	committer, err := tunav2.New(committee, parameters.Consensus.LeaderCount)
	if err != nil {
		return fail(err)
	}

	repository, err := dag.OpenRepository(node.Store, dag.Domain(committee, parameters.Consensus.LeaderCount), id)
	if err != nil {
		return fail(err)
	}

	node.DAG, err = dag.NewCore(
		id,
		committee,
		parameters.Consensus,
		node.SigService,
		payloadBridge{node.Mempool},
		repository,
		committer,
	)
	if err != nil {
		return fail(err)
	}

	logger.Info.Printf(
		"Tuna_v2 ready node=%d leaders=%d quorum=%d",
		nodeID,
		parameters.Consensus.LeaderCount,
		committer.Quorum(),
	)
	return node, nil
}

type payloadBridge struct {
	pool *mempool.Mempool
}

func (p payloadBridge) Take(limit int) []crypto.Digest {
	return p.pool.TakePayloads(limit)
}

func (p payloadBridge) Restore(refs []crypto.Digest) {
	p.pool.RestorePayloads(refs)
}

func (p payloadBridge) Remove(refs []crypto.Digest) {
	p.pool.RemovePayloads(refs)
}

func (p payloadBridge) Check(author core.NodeID, refs []crypto.Digest, id crypto.Digest) (bool, error) {
	missing := false
	for _, ref := range refs {
		payload, err := p.pool.GetPayload(ref)
		if err != nil {
			return false, err
		}

		if payload == nil {
			missing = true
		}
	}

	if missing {
		p.pool.Sync.Verify(author, refs, id)
		return false, nil
	}

	return true, nil
}

func (n *Node) deliver(ctx context.Context, commit dag.CommittedSubDag) error {
	record := struct {
		Index        uint64      `json:"index"`
		Round        dag.Round   `json:"round"`
		Offset       int         `json:"offset"`
		Author       core.NodeID `json:"author"`
		Leader       string      `json:"leader"`
		Blocks       int         `json:"blocks"`
		Payloads     []string    `json:"payloads"`
		Batches      []int       `json:"batches"`
		Transactions int         `json:"transactions"`
		CommittedAt  int64       `json:"committed_at_ns"`
	}{
		Index:       commit.Index,
		Round:       commit.Slot.Round,
		Offset:      commit.Slot.Offset,
		Author:      commit.Slot.Author,
		Leader:      fmt.Sprintf("%x", commit.Leader.Digest),
		Blocks:      len(commit.Blocks),
		CommittedAt: time.Now().UnixNano(),
	}
	for _, ref := range commit.Payloads {
		payload, err := n.Mempool.GetPayload(ref)
		if err != nil {
			return err
		}

		if payload == nil {
			return fmt.Errorf("committed payload missing")
		}

		record.Payloads = append(record.Payloads, fmt.Sprintf("%x", ref))
		record.Batches = append(record.Batches, payload.Batch.ID)
		record.Transactions += len(payload.Batch.Txs)
	}

	data, _ := json.Marshal(record)
	logger.Info.Printf("Tuna_v2 commit %s", data)
	if n.CommitConsumer != nil {
		select {
		case n.CommitConsumer <- commit:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// ReadCommits returns durable history, including commits not re-emitted after restart.
func (n *Node) ReadCommits(after uint64) ([]dag.CommittedSubDag, error) {
	return n.DAG.Repository.ReadCommits(after)
}

func (n *Node) closeOutputs() {
	for _, output := range n.outputs {
		output.Close()
	}

	n.outputs = nil
}

func (n *Node) RunContext(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer func() {
		n.Store.Close()
		n.closeOutputs()
	}()
	if err := n.Mempool.Start(ctx); err != nil {
		cancel()
		return err
	}

	n.TxPool.RunContext(ctx)
	codec := network.NewCodec(dag.MessageTypes)
	sender := network.NewSender(codec)
	_, port, _ := net.SplitHostPort(n.Committee.Address(n.ID))
	receiver := network.NewReceiver(net.JoinHostPort("", port), codec)

	var workers sync.WaitGroup

	defer func() {
		cancel()
		workers.Wait()
		n.TxPool.Wait()
		n.Mempool.Wait()
	}()
	if err := receiver.Start(ctx); err != nil {
		return err
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		sender.RunContext(ctx)
	}()
	transport := core.NewTransmitorContext(ctx, sender, receiver, n.Committee)
	runtime := &dag.Runtime{
		Core:         n.DAG,
		Transport:    transport,
		PayloadReady: n.Mempool.Sync.Ready,
		OnCommit:     n.deliver,
	}
	return runtime.Run(ctx)
}

func (n *Node) Run() {
	if err := n.RunContext(context.Background()); err != nil {
		logger.Error.Println(err)
	}
}
