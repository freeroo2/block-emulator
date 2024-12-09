// Definition of transaction

package core

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"time"
)

// TransactionType 定义交易类型
type TransactionType int

const (
	Register TransactionType = iota
	Update
	Delete
	CreateUser
)

func StringToTransactionType(s string) TransactionType {
	switch s {
	case "Register":
		return Register
	case "Update":
		return Update
	case "Delete":
		return Delete
	case "CreateUser":
		return CreateUser
	default:
		return -1
	}
}

type Transaction struct {
	Sender    utils.Address
	Recipient utils.Address
	Nonce     uint64
	Signature []byte // not implemented now.
	Value     *big.Int
	TxHash    []byte

	Time time.Time // TimeStamp the tx proposed.

	// used in transaction relaying
	Relayed bool
	// used in broker, if the tx is not a broker1 or broker2 tx, these values should be empty.
	HasBroker      bool
	SenderIsBroker bool
	OriginalSender utils.Address
	FinalRecipient utils.Address
	RawTxHash      []byte

	// de-transaction
	IsDeTx          bool
	TxType          TransactionType
	Identifier      string
	Prefix          string
	Suffix          string
	IType           string
	Digest          []byte
	DataAddress     string
	MetaDataAddress string
}

func (tx *Transaction) PrintTx() string {
	vals := []interface{}{
		tx.Sender[:],
		tx.Recipient[:],
		tx.Value,
		string(tx.TxHash[:]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// Encode transaction for storing
func (tx *Transaction) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Decode transaction
func DecodeTx(to_decode []byte) *Transaction {
	var tx Transaction

	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}

	return &tx
}

// new a transaction
func NewTransaction(sender, recipient string, value *big.Int, nonce uint64, proposeTime time.Time) *Transaction {
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Time:      proposeTime,
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	tx.IsDeTx = false
	return tx
}

func NewDeTransaction(sender, recipient string, nonce uint64, proposeTime time.Time,
	txType, identifier, itype, dataAddress, metaDataAddress string, digest []byte, prefix, suffix string) *Transaction {

	tx := &Transaction{
		Sender:          sender,
		Recipient:       recipient,
		Nonce:           nonce,
		Time:            proposeTime,
		TxType:          StringToTransactionType(txType),
		Identifier:      identifier,
		IType:           itype,
		DataAddress:     dataAddress,
		MetaDataAddress: metaDataAddress,
		Prefix:          prefix,
		Digest:          digest,
		Suffix:          suffix,
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	tx.IsDeTx = true
	return tx
}
