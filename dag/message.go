package dag

import (
	"reflect"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

const (
	BlockMessageType = iota + 200
	BlockRequestType
	BlockResponseType
	StatusMessageType
)

type BlockMessage struct {
	Block *Block
}

func (*BlockMessage) MsgType() int {
	return BlockMessageType
}

func (*BlockMessage) Module() string {
	return "tuna_v2"
}

type BlockRequest struct {
	Author core.NodeID
	Refs   []BlockRef
}

func (*BlockRequest) MsgType() int {
	return BlockRequestType
}

func (*BlockRequest) Module() string {
	return "tuna_v2"
}

type BlockResponse struct {
	Blocks []*Block
}

func (*BlockResponse) MsgType() int {
	return BlockResponseType
}

func (*BlockResponse) Module() string {
	return "tuna_v2"
}

type StatusMessage struct {
	Author core.NodeID
	Domain crypto.Digest
	Heads  []BlockRef
}

func (*StatusMessage) MsgType() int {
	return StatusMessageType
}

func (*StatusMessage) Module() string {
	return "tuna_v2"
}

var MessageTypes = map[int]reflect.Type{
	BlockMessageType:  reflect.TypeOf(BlockMessage{}),
	BlockRequestType:  reflect.TypeOf(BlockRequest{}),
	BlockResponseType: reflect.TypeOf(BlockResponse{}),
	StatusMessageType: reflect.TypeOf(StatusMessage{}),
}
