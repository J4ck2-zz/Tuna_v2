package pool

import (
	"context"
	"testing"
	"time"
)

func TestPoolGeneratesLowRatesAndResumesBatchIDs(t *testing.T) {
	parameters := DefaultParameters
	parameters.Rate = 10
	parameters.BatchSize = 1
	pool := NewPool(parameters, 4, 2)
	pool.ResumeAfterBatch(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.RunContext(ctx)
	select {
	case batch := <-pool.BatchChannel():
		if batch.ID != 14 || len(batch.Txs) != 1 {
			t.Fatalf("incorrect resumed batch: %+v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("rate below sample precision generated no transactions")
	}

	cancel()
	done := make(chan struct{})
	go func() {
		pool.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pool did not stop")
	}
}
