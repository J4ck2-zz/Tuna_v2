package core

import "Tuna_v2/crypto"

type NodeID int

const NONE NodeID = -1

type Authority struct {
	Name        crypto.PublickKey `json:"name"`
	Id          NodeID            `json:"node_id"`
	Addr        string            `json:"addr"`
	MempoolAddr string            `json:"mempool_addr"`
}

type Committee struct {
	Authorities map[NodeID]Authority `json:"authorities"`
}

func (c Committee) ID(name crypto.PublickKey) NodeID {
	for id, authority := range c.Authorities {
		if authority.Name.Pubkey.Equal(name.Pubkey) {
			return id
		}
	}

	return NONE
}

func (c Committee) Size() int {
	return len(c.Authorities)
}

func (c Committee) Name(id NodeID) crypto.PublickKey {
	a := c.Authorities[id]
	return a.Name
}

func (c Committee) Address(id NodeID) string {
	a := c.Authorities[id]
	return a.Addr
}

func (c Committee) BroadCast(id NodeID) []string {
	addrs := make([]string, 0)
	for nodeid, a := range c.Authorities {
		if nodeid != id {
			addrs = append(addrs, a.Addr)
		}
	}

	return addrs
}

// mempool
func (c Committee) MempoolAddress(id NodeID) string {
	a := c.Authorities[id]
	return a.MempoolAddr
}

func (c Committee) MempoolBroadCast(id NodeID) []string {
	addrs := make([]string, 0)
	for nodeid, a := range c.Authorities {
		if nodeid != id {
			addrs = append(addrs, a.MempoolAddr)
		}
	}

	return addrs
}
