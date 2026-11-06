package mempool

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"reflect"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
	"Tuna_v2/pool"
)

type PoolLinkMesag struct {
	Author core.NodeID
}

func (*PoolLinkMesag) MsgType() int {
	return PoolLinkMesagType
}

func (*PoolLinkMesag) Module() string {
	return "mempool"
}

type Payload struct {
	Proposer  core.NodeID
	Batch     pool.Batch
	Signature crypto.Signature
}

func NewPload(proposer core.NodeID, batch pool.Batch, sigService *crypto.SigService) (*Payload, error) {
	payload := &Payload{
		Proposer: proposer,
		Batch:    batch,
	}
	sig, err := sigService.RequestSignature(payload.Hash())
	if err != nil {
		return nil, err
	}

	payload.Signature = sig
	return payload, nil
}

func (b *Payload) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(b); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b *Payload) Decode(data []byte) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(b)
}

func (b *Payload) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add([]byte("Tuna_v2/payload/v1"))

	var word [8]byte

	add := func(value uint64) {
		binary.BigEndian.PutUint64(word[:], value)
		hasher.Add(word[:])
	}
	add(uint64(b.Proposer))
	add(uint64(b.Batch.ID))
	add(uint64(len(b.Batch.Txs)))
	for _, tx := range b.Batch.Txs {
		add(uint64(len(tx)))
		hasher.Add(tx)
	}

	return hasher.Sum256(nil)
}

func (b *Payload) Verify(committee core.Committee) bool {
	if b == nil {
		return false
	}

	authority, exists := committee.Authorities[b.Proposer]
	return exists && b.Signature.Verify(authority.Name, b.Hash())
}

type OwnPayloadMsg struct {
	Payload *Payload
}

func (*OwnPayloadMsg) MsgType() int {
	return OwnPayloadType
}

func (*OwnPayloadMsg) Module() string {
	return "mempool"
}

type OtherPayloadMsg struct {
	Payload *Payload
}

func (*OtherPayloadMsg) MsgType() int {
	return OtherPayloadType
}

func (*OtherPayloadMsg) Module() string {
	return "mempool"
}

type RequestPayloadMsg struct {
	Digests []crypto.Digest
	Author  core.NodeID
	ReqId   int
}

func (*RequestPayloadMsg) MsgType() int {
	return RequestPayloadType
}

func (*RequestPayloadMsg) Module() string {
	return "mempool"
}

type ReplyPayloadRequestMsg struct {
	Payload []*Payload
	Author  core.NodeID
}

func (*ReplyPayloadRequestMsg) MsgType() int {
	return ReplyPayloadRequestType
}

func (*ReplyPayloadRequestMsg) Module() string {
	return "mempool"
}

type VerifyStatus int

type MempoolValidator interface {
	Verify(core.Committee) bool
}

const (
	OK VerifyStatus = iota
	Wait
	Reject
)

const (
	OwnPayloadType int = iota
	OtherPayloadType
	RequestPayloadType
	ReplyPayloadRequestType
	PoolLinkMesagType
)

// Only payload network messages belong in the wire codec.
var DefaultMessageTypeMap = map[int]reflect.Type{
	OwnPayloadType:          reflect.TypeOf(OwnPayloadMsg{}),
	OtherPayloadType:        reflect.TypeOf(OtherPayloadMsg{}),
	RequestPayloadType:      reflect.TypeOf(RequestPayloadMsg{}),
	ReplyPayloadRequestType: reflect.TypeOf(ReplyPayloadRequestMsg{}),
	PoolLinkMesagType:       reflect.TypeOf(PoolLinkMesag{}),
}
