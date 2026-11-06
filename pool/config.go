package pool

import "fmt"

type Parameters struct {
	Rate         int `json:"rate"`           // tx send rate
	TxSize       int `json:"tx_size"`        // byte
	BatchSize    int `json:"batch_size"`     // the max number of tx that a batch can hold
	MaxQueueSize int `json:"max_queue_size"` // max queue size
}

var DefaultParameters = Parameters{
	Rate:         1_000,
	TxSize:       16,
	BatchSize:    200,
	MaxQueueSize: 10_000,
}

func (p Parameters) Validate() error {
	if p.Rate < 0 ||
		p.TxSize <= 0 ||
		p.BatchSize <= 0 ||
		p.MaxQueueSize < p.BatchSize ||
		int64(p.TxSize)*int64(p.BatchSize) > 1<<20 {
		return fmt.Errorf("invalid transaction pool parameters or batch exceeds 1 MiB")
	}

	return nil
}
