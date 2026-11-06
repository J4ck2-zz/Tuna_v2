package network

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
)

type Messgae interface {
	MsgType() int
	Module() string
}

const MaxFrameSize = 16 << 20

// Codec uses independent, length-bounded Gob frames. Hashing never uses Gob.
type Codec struct {
	types map[int]reflect.Type
	conn  io.ReadWriter
}

func NewCodec(types map[int]reflect.Type) *Codec {
	return &Codec{
		types: types,
	}
}

func (c *Codec) Bind(conn io.ReadWriter) *Codec {
	return &Codec{
		types: c.types,
		conn:  conn,
	}
}

func (c *Codec) Write(message Messgae) error {
	if message == nil {
		return fmt.Errorf("nil message")
	}

	typ, ok := c.types[message.MsgType()]
	if !ok || reflect.TypeOf(message) != reflect.PointerTo(typ) {
		return fmt.Errorf("unregistered message type %d", message.MsgType())
	}

	var body bytes.Buffer

	encoder := gob.NewEncoder(&body)
	if err := encoder.Encode(message.MsgType()); err != nil {
		return err
	}

	if err := encoder.Encode(message); err != nil {
		return err
	}

	if body.Len() > MaxFrameSize {
		return fmt.Errorf("message exceeds frame limit")
	}

	var length [4]byte

	binary.BigEndian.PutUint32(length[:], uint32(body.Len()))
	frame := append(length[:], body.Bytes()...)
	for len(frame) > 0 {
		n, err := c.conn.Write(frame)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		frame = frame[n:]
	}

	return nil
}

func (c *Codec) Read() (Messgae, error) {
	var length [4]byte

	if _, err := io.ReadFull(c.conn, length[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(length[:])
	if size == 0 || size > MaxFrameSize {
		return nil, fmt.Errorf("invalid frame length %d", size)
	}

	data := make([]byte, int(size))
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return nil, err
	}

	decoder := gob.NewDecoder(bytes.NewReader(data))

	var id int

	if err := decoder.Decode(&id); err != nil {
		return nil, err
	}

	typ, ok := c.types[id]
	if !ok {
		return nil, fmt.Errorf("unknown message type %d", id)
	}

	message, ok := reflect.New(typ).Interface().(Messgae)
	if !ok {
		return nil, fmt.Errorf("invalid registered type %d", id)
	}

	if err := decoder.Decode(message); err != nil {
		return nil, err
	}

	if message.MsgType() != id {
		return nil, fmt.Errorf("message type mismatch")
	}

	return message, nil
}
