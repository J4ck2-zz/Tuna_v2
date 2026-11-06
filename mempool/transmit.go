package mempool

import (
	"context"
	"fmt"

	"Tuna_v2/core"
	"Tuna_v2/network"
)

type Transmit struct {
	sender    *network.Sender
	receiver  *network.Receiver
	mempoolCh chan core.Message
	msgCh     chan *network.NetMessage
	committee core.Committee
}

func NewTransmit(sender *network.Sender, receiver *network.Receiver, committee core.Committee) *Transmit {
	return &Transmit{
		sender:    sender,
		receiver:  receiver,
		committee: committee,
		mempoolCh: make(chan core.Message, 10000),
		msgCh:     make(chan *network.NetMessage, 10000),
	}
}

func (tr *Transmit) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-tr.msgCh:
			if err := tr.sender.SendContext(ctx, message); err != nil {
				return
			}
		case message, ok := <-tr.receiver.RecvChannel():
			if !ok {
				return
			}
			if message.Module() == "mempool" {
				select {
				case tr.mempoolCh <- message:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (tr *Transmit) MempoolSend(from, to core.NodeID, message core.Message) error {
	var addresses []string

	if to == core.NONE {
		addresses = tr.committee.MempoolBroadCast(from)
	} else {
		if _, ok := tr.committee.Authorities[to]; !ok {
			return fmt.Errorf("unknown payload recipient")
		}

		addresses = []string{tr.committee.MempoolAddress(to)}
	}

	select {
	case tr.msgCh <- &network.NetMessage{
		Msg:     message,
		Address: addresses,
	}:
		return nil
	default:
		return fmt.Errorf("payload transport queue full")
	}
}

func (tr *Transmit) MempoolRecv() core.Message {
	return <-tr.mempoolCh
}

func (tr *Transmit) MempoolChannel() chan core.Message {
	return tr.mempoolCh
}
