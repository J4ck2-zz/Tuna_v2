package core

import (
	"context"
	"fmt"

	"Tuna_v2/network"
)

type Message interface {
	MsgType() int
	Module() string
}

// Transmitor forwards registered messages without interpreting their protocol.
type Transmitor struct {
	ctx       context.Context
	sender    *network.Sender
	recvCh    chan Message
	committee Committee
}

func NewTransmitor(sender *network.Sender, receiver *network.Receiver, committee Committee) *Transmitor {
	return NewTransmitorContext(context.Background(), sender, receiver, committee)
}

func NewTransmitorContext(ctx context.Context, sender *network.Sender, receiver *network.Receiver, committee Committee) *Transmitor {
	tr := &Transmitor{
		ctx:       ctx,
		sender:    sender,
		recvCh:    make(chan Message, 1000),
		committee: committee,
	}
	go func() {
		defer close(tr.recvCh)
		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-receiver.RecvChannel():
				if !ok {
					return
				}
				select {
				case tr.recvCh <- message:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return tr
}

func (tr *Transmitor) Send(from, to NodeID, msg Message) error {
	var addresses []string

	if to == NONE {
		addresses = tr.committee.BroadCast(from)
	} else {
		if _, exists := tr.committee.Authorities[to]; !exists {
			return fmt.Errorf("unknown recipient %d", to)
		}

		addresses = []string{tr.committee.Address(to)}
	}

	return tr.sender.SendContext(tr.ctx, &network.NetMessage{
		Msg:     msg,
		Address: addresses,
	})
}

func (tr *Transmitor) Recv() Message {
	return <-tr.recvCh
}

func (tr *Transmitor) RecvChannel() chan Message {
	return tr.recvCh
}
