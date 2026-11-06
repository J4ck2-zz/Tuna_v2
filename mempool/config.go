package mempool

import "fmt"

// Parameters controls payload dissemination and retrieval. Durations are milliseconds.
type Parameters struct {
	SyncTimeout         int    `json:"sync_timeout"`
	RetryDelay          int    `json:"retry_delay"`
	PayloadDelay        int    `json:"payload_delay_send"`
	RequestPloadDelay   int    `json:"Requst_Pload_delay"`
	MaxMempoolQueenSize uint64 `json:"maxmempoolqueensize"`
}

var DefaultParameters = Parameters{
	SyncTimeout:         500,
	RetryDelay:          500,
	PayloadDelay:        50,
	RequestPloadDelay:   100,
	MaxMempoolQueenSize: 10_000,
}

func (p Parameters) Validate() error {
	if p.SyncTimeout < 0 ||
		p.RetryDelay <= 0 ||
		p.PayloadDelay < 0 ||
		p.RequestPloadDelay < 0 ||
		p.MaxMempoolQueenSize == 0 {
		return fmt.Errorf("invalid mempool parameters")
	}

	return nil
}
