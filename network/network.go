package network

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"Tuna_v2/logger"
)

type NetMessage struct {
	Msg     Messgae
	Address []string
}

type Sender struct {
	msgCh chan *NetMessage
	cc    *Codec
}

func NewSender(codec *Codec) *Sender {
	return &Sender{
		msgCh: make(chan *NetMessage, 1000),
		cc:    codec,
	}
}

func (s *Sender) Run() {
	s.RunContext(context.Background())
}

func (s *Sender) RunContext(ctx context.Context) {
	s.RunWithCodec(ctx, s.cc)
}

// RunWithCodec owns all peer queues; each peer retries independently of offline peers.
func (s *Sender) RunWithCodec(ctx context.Context, codec *Codec) {
	peers := make(map[string]chan Messgae)

	var workers sync.WaitGroup

	defer workers.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-s.msgCh:
			if message == nil {
				continue
			}
			for _, addr := range message.Address {
				queue := peers[addr]
				if queue == nil {
					queue = make(chan Messgae, 1000)
					peers[addr] = queue
					workers.Add(1)
					go func(addr string, queue <-chan Messgae) {
						defer workers.Done()
						sendPeer(ctx, addr, queue, codec)
					}(
						addr,
						queue,
					)
				}

				select {
				case queue <- message.Msg:
				case <-ctx.Done():
					return
				default: /* periodic DAG heads and requests repair queue overflow */
				}
			}
		}
	}
}

func sendPeer(ctx context.Context, addr string, queue <-chan Messgae, codec *Codec) {
	var conn net.Conn

	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for {
		var message Messgae

		select {
		case <-ctx.Done():
			return
		case message = <-queue:
		}

		for {
			if ctx.Err() != nil {
				return
			}

			if conn == nil {
				var err error

				conn, err = (&net.Dialer{
					Timeout: 500 * time.Millisecond,
				}).DialContext(ctx, "tcp", addr)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					case <-time.After(100 * time.Millisecond):
					}

					continue
				}
			}

			conn.SetWriteDeadline(time.Now().Add(time.Second))
			if err := codec.Bind(conn).Write(message); err == nil {
				break
			}

			conn.Close()
			conn = nil
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func (s *Sender) Send(message *NetMessage) {
	s.msgCh <- message
}

func (s *Sender) SendContext(ctx context.Context, message *NetMessage) error {
	select {
	case s.msgCh <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sender) SendChannel() chan<- *NetMessage {
	return s.msgCh
}

type Receiver struct {
	addr     string
	msg      chan Messgae
	cc       *Codec
	mu       sync.Mutex
	listener net.Listener
	done     chan struct{}
}

func NewReceiver(addr string, codec *Codec) *Receiver {
	return &Receiver{
		addr: addr,
		msg:  make(chan Messgae, 1000),
		cc:   codec,
		done: make(chan struct{}),
	}
}

func (r *Receiver) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.listener = listener
	r.mu.Unlock()
	go r.serve(ctx, listener)
	return nil
}

func (r *Receiver) serve(ctx context.Context, listener net.Listener) {
	stop := context.AfterFunc(ctx, func() {
		listener.Close()
	})
	defer stop()
	defer listener.Close()

	var workers sync.WaitGroup

	defer func() {
		workers.Wait()
		close(r.msg)
		close(r.done)
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}

			logger.Warn.Printf("Accept: %v", err)
			continue
		}

		workers.Add(1)
		go func() {
			defer workers.Done()
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() {
				conn.Close()
			})
			defer stop()
			cc := r.cc.Bind(conn)
			for {
				message, err := cc.Read()
				if err != nil {
					if err != io.EOF && ctx.Err() == nil {
						logger.Debug.Printf("Receive: %v", err)
					}

					return
				}

				select {
				case r.msg <- message:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (r *Receiver) Address() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listener != nil {
		return r.listener.Addr().String()
	}

	return r.addr
}

func (r *Receiver) Run() {
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		panic(err)
	}

	select {}
}

func (r *Receiver) Recv() Messgae {
	return <-r.msg
}

func (r *Receiver) RecvChannel() <-chan Messgae {
	return r.msg
}

func (r *Receiver) Done() <-chan struct{} {
	return r.done
}
