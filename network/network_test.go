package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"reflect"
	"testing"
	"time"
)

type testMessage struct {
	Value int
}

func (*testMessage) MsgType() int {
	return 7
}

func (*testMessage) Module() string {
	return "test"
}

func testCodec() *Codec {
	return NewCodec(map[int]reflect.Type{
		7: reflect.TypeOf(testMessage{}),
	})
}

func TestCodecRejectsUnknownAndOversizedFrames(t *testing.T) {
	var wire bytes.Buffer

	codec := testCodec().Bind(&wire)
	if err := codec.Write(&testMessage{
		Value: 42,
	}); err != nil {
		t.Fatal(err)
	}

	message, err := codec.Read()
	if err != nil || message.(*testMessage).Value != 42 {
		t.Fatal("codec round trip failed")
	}

	var body bytes.Buffer

	gob.NewEncoder(&body).Encode(99)
	binary.Write(&wire, binary.BigEndian, uint32(body.Len()))
	wire.Write(body.Bytes())
	if _, err := codec.Read(); err == nil {
		t.Fatal("unknown message type accepted")
	}

	binary.Write(&wire, binary.BigEndian, uint32(MaxFrameSize+1))
	if _, err := codec.Read(); err == nil {
		t.Fatal("oversized frame accepted")
	}
}

func TestSenderReconnectsAndOfflinePeersDoNotBlockOthers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiverCtx, stopReceiver := context.WithCancel(ctx)
	receiver := NewReceiver("127.0.0.1:0", testCodec())
	if err := receiver.Start(receiverCtx); err != nil {
		t.Fatal(err)
	}

	address := receiver.Address()
	sender := NewSender(testCodec())
	done := make(chan struct{})
	go func() {
		sender.RunContext(ctx)
		close(done)
	}()
	sender.Send(&NetMessage{
		Msg: &testMessage{
			Value: 1,
		},
		Address: []string{"127.0.0.1:1", address},
	})
	select {
	case <-receiver.RecvChannel():
	case <-time.After(3 * time.Second):
		t.Fatal("offline peer blocked a live peer")
	}

	stopReceiver()
	select {
	case <-receiver.Done():
	case <-time.After(time.Second):
		t.Fatal("receiver did not shut down")
	}

	replacement := NewReceiver(address, testCodec())
	if err := replacement.Start(ctx); err != nil {
		t.Fatal(err)
	}

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(4 * time.Second)
	connected := false
	for !connected {
		select {
		case <-ticker.C:
			sender.Send(&NetMessage{
				Msg: &testMessage{
					Value: 2,
				},
				Address: []string{address},
			})
		case message := <-replacement.RecvChannel():
			if message.(*testMessage).Value == 2 {
				connected = true
			}
		case <-deadline:
			t.Fatal("sender did not reconnect")
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sender leaked workers")
	}

	select {
	case <-replacement.Done():
	case <-time.After(time.Second):
		t.Fatal("receiver leaked workers")
	}
}
