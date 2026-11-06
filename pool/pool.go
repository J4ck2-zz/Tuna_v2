package pool

import (
	"context"
	"sync"
	"time"

	"Tuna_v2/logger"
)

type txQueue struct {
	queue        []Transaction
	batchChannel chan Batch
	wind         int // write index
	rind         int // read index
	nums         int
	maxQueueSize int
	batchSize    int
	N            int
	Id           int
	bcnt         int
}

func newTxQueue(
	maxQueueSize, batchSize int,
	batchChannel chan Batch,
	N, Id int,
) *txQueue {
	r := &txQueue{
		queue:        make([]Transaction, maxQueueSize),
		batchChannel: batchChannel,
		wind:         0,
		rind:         -1,
		nums:         0,
		maxQueueSize: maxQueueSize,
		batchSize:    batchSize,
		N:            N,
		Id:           Id,
		bcnt:         0,
	}
	return r
}

func (q *txQueue) run(txChannel <-chan Transaction) {
	q.runContext(context.Background(), txChannel)
}

func (q *txQueue) runContext(ctx context.Context, txChannel <-chan Transaction) {
	for {
		var tx Transaction

		select {
		case <-ctx.Done():
			return
		case tx = <-txChannel:
		}

		if q.wind == q.rind {
			logger.Warn.Println("Transaction pool is full")
			continue
		}

		q.queue[q.wind] = tx
		q.wind = (q.wind + 1) % q.maxQueueSize
		q.nums++
		if q.nums >= q.batchSize {
			q.makeContext(ctx)
		}
	}
}

func (q *txQueue) make() {
	q.makeContext(context.Background())
}

func (q *txQueue) makeContext(ctx context.Context) {
	batch := Batch{
		ID: q.Id + q.N*q.bcnt,
	}
	defer func() {
		q.bcnt++
		// BenchMark print batch create time
		logger.Info.Printf("Received Batch %d\n", batch.ID)
	}()

	for i := 0; i < q.batchSize; i++ {
		q.rind = (q.rind + 1) % q.maxQueueSize
		batch.Txs = append(batch.Txs, q.queue[q.rind])
		q.nums--
	}
	// batch.Txs = nil
	select {
	case q.batchChannel <- batch:
	case <-ctx.Done():
	}
}

func (q *txQueue) get() Batch {
	if len(q.batchChannel) > 0 {
		return <-q.batchChannel
	} else {
		return Batch{
			ID:  -1,
			Txs: nil,
		}
	}
}

// func (q *TxQueue)

const PRECISION = 20 // Sample precision.

const BURST_DURATION = 1000 / PRECISION

type txMaker struct {
	txSize int
	rate   int
}

func newTxMaker(txSize, rate int) *txMaker {
	return &txMaker{
		txSize: txSize,
		rate:   rate,
	}
}

func (maker *txMaker) run(txChannel chan<- Transaction) {
	maker.runContext(context.Background(), txChannel)
}

func (maker *txMaker) runContext(ctx context.Context, txChannel chan<- Transaction) {
	ticker := time.NewTicker(time.Millisecond * BURST_DURATION)
	defer ticker.Stop()
	remainder := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			remainder += maker.rate
			count := remainder / PRECISION
			remainder %= PRECISION
			for i := 0; i < count; i++ {
				select {
				case txChannel <- make(Transaction, maker.txSize):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

type Pool struct {
	start        sync.Once
	done         chan struct{}
	Parameters   Parameters
	queue        *txQueue
	maker        *txMaker
	txChannel    chan Transaction
	batchChannel chan Batch
}

func NewPool(parameters Parameters, N, Id int) *Pool {
	logger.Info.Printf(
		"Transaction pool queue capacity set to %d \n",
		parameters.MaxQueueSize,
	)
	logger.Info.Printf(
		"Transaction pool tx size set to %d \n",
		parameters.TxSize,
	)
	logger.Info.Printf(
		"Transaction pool batch size set to %d \n",
		parameters.BatchSize,
	)
	logger.Info.Printf(
		"Transaction pool tx rate set to %d \n",
		parameters.Rate,
	)

	batchChannel, txChannel := make(chan Batch, 1_000), make(chan Transaction, 10_000)
	p := &Pool{
		done:         make(chan struct{}),
		Parameters:   parameters,
		txChannel:    txChannel,
		batchChannel: batchChannel,
	}

	p.queue = newTxQueue(
		parameters.MaxQueueSize,
		parameters.BatchSize,
		batchChannel,
		N,
		Id,
	)

	p.maker = newTxMaker(
		parameters.TxSize,
		parameters.Rate,
	)

	return p
}

func (p *Pool) Run() {
	p.RunContext(context.Background())
}

func (p *Pool) RunContext(ctx context.Context) {
	p.start.Do(func() {
		var workers sync.WaitGroup

		workers.Add(2)
		go func() {
			defer workers.Done()
			p.queue.runContext(ctx, p.txChannel)
		}()
		go func() {
			defer workers.Done()
			p.maker.runContext(ctx, p.txChannel)
		}()
		go func() {
			workers.Wait()
			close(p.done)
		}()
	})
}

func (p *Pool) Wait() {
	<-p.done
}

// ResumeAfterBatch is called before starting the generator.
func (p *Pool) ResumeAfterBatch(id int) {
	if id >= p.queue.Id {
		p.queue.bcnt = (id-p.queue.Id)/p.queue.N + 1
	}
}

func (p *Pool) GetBatch() Batch {
	return p.queue.get()
}

func (p *Pool) BatchChannel() chan Batch {
	return p.batchChannel
}
