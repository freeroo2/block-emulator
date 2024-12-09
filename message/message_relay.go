package message

import (
	"blockEmulator/core"
)

type TxProofResult struct {
	Found       bool
	BlockHash   []byte
	TxHash      []byte
	TxRoot      []byte
	BlockHeight uint64
	KeyList     [][]byte
	ValueList   [][]byte
	Error       string
}

// if transaction relaying is used, this message is used for sending sequence id, too
type Relay struct {
	Txs           []*core.Transaction
	SenderShardID uint64
	SenderSeq     uint64
}

// This struct is similar to Relay. Nodes receiving this message will validate this proof first.
type RelayWithProof struct {
	Txs           []*core.Transaction
	TxProofs      []TxProofResult
	SenderShardID uint64
	SenderSeq     uint64
}
