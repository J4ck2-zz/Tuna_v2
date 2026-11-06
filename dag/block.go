package dag

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"Tuna_v2/core"
	"Tuna_v2/crypto"
)

type Block struct {
	Domain    crypto.Digest
	Round     Round
	Author    core.NodeID
	Parents   []BlockRef
	Payloads  []crypto.Digest
	Timestamp int64
	Signature crypto.Signature
}

func (b *Block) content() []byte {
	var buf bytes.Buffer

	buf.WriteString("Tuna_v2/block/v1")
	buf.Write(b.Domain[:])
	write := func(v uint64) {
		var word [8]byte

		binary.BigEndian.PutUint64(word[:], v)
		buf.Write(word[:])
	}
	write(uint64(b.Round))
	write(uint64(b.Author))
	write(uint64(b.Timestamp))
	write(uint64(len(b.Parents)))
	for _, parent := range b.Parents {
		write(uint64(parent.Round))
		write(uint64(parent.Author))
		buf.Write(parent.Digest[:])
	}

	write(uint64(len(b.Payloads)))
	for _, payload := range b.Payloads {
		buf.Write(payload[:])
	}

	return buf.Bytes()
}

func (b *Block) Hash() crypto.Digest {
	return crypto.NewHasher().Sum256(b.content())
}

func (b *Block) Ref() BlockRef {
	return BlockRef{
		Round:  b.Round,
		Author: b.Author,
		Digest: b.Hash(),
	}
}

func (b *Block) Clone() *Block {
	copy := *b
	copy.Parents = append([]BlockRef(nil), b.Parents...)
	copy.Payloads = append([]crypto.Digest(nil), b.Payloads...)
	copy.Signature.Sig = append([]byte(nil), b.Signature.Sig...)
	return &copy
}

func (b *Block) Encode() ([]byte, error) {
	return json.Marshal(b)
}

func DecodeBlock(data []byte) (*Block, error) {
	var b Block

	err := json.Unmarshal(data, &b)
	return &b, err
}

func Genesis(domain crypto.Digest, author core.NodeID) *Block {
	return &Block{
		Domain: domain,
		Author: author,
	}
}

func NewBlock(
	domain crypto.Digest,
	author core.NodeID,
	round Round,
	parents []BlockRef,
	payloads []crypto.Digest,
	timestamp int64,
	signer *crypto.SigService,
) (*Block, error) {
	block := (&Block{
		Domain:    domain,
		Author:    author,
		Round:     round,
		Parents:   parents,
		Payloads:  payloads,
		Timestamp: timestamp,
	}).Clone()
	signature, err := signer.RequestSignature(block.Hash())
	if err != nil {
		return nil, err
	}

	block.Signature = signature
	return block, nil
}

func (b *Block) Verify(domain crypto.Digest, committee core.Committee, q int, parameters Parameters) error {
	if b == nil {
		return fmt.Errorf("nil block")
	}

	if b.Domain != domain || b.Round == 0 || uint64(b.Round) > math.MaxUint64-3 {
		return fmt.Errorf("invalid domain or round")
	}

	if _, exists := committee.Authorities[b.Author]; !exists {
		return fmt.Errorf("unknown block author")
	}

	if len(b.Parents) == 0 ||
		len(b.Parents) > parameters.MaxParents ||
		len(b.Payloads) > parameters.MaxPayloads {
		return fmt.Errorf("block limit exceeded")
	}

	if b.Parents[0].Author != b.Author {
		return fmt.Errorf("first parent must be the previous own block")
	}

	parents := make(map[BlockRef]bool)
	voters := make(map[core.NodeID]bool)
	for _, parent := range b.Parents {
		if _, exists := committee.Authorities[parent.Author]; !exists {
			return fmt.Errorf("unknown parent author")
		}

		if parent.Round >= b.Round || parents[parent] {
			return fmt.Errorf("invalid or duplicate parent")
		}

		parents[parent] = true
		if parent.Round == 0 && parent != Genesis(domain, parent.Author).Ref() {
			return fmt.Errorf("invalid genesis reference")
		}

		if parent.Round == b.Round-1 {
			voters[parent.Author] = true
		}
	}

	if len(voters) < q {
		return fmt.Errorf("previous round quorum missing")
	}

	payloads := make(map[crypto.Digest]bool)
	for _, payload := range b.Payloads {
		if payloads[payload] {
			return fmt.Errorf("duplicate payload")
		}

		payloads[payload] = true
	}

	if len(b.content())+len(b.Signature.Sig) > parameters.MaxBlockBytes {
		return fmt.Errorf("block too large")
	}

	if !b.Signature.Verify(committee.Name(b.Author), b.Hash()) {
		return fmt.Errorf("invalid block signature")
	}

	return nil
}
