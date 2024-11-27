// Account, AccountState
// Some basic operation about accountState

package core

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
	"time"
)

type DEAccount struct {
	AcAddress utils.Address
	PublicKey []byte
}

// IdentifierRecord 定义标识符记录的数据结构
type IdentifierRecord struct {
	Identifier      string    // 标识符
	Owner           string    // 所有者
	DataAddress     string    // 数据地址
	Data            []byte    // 数据
	MetaDataAddress string    // 元数据
	Timestamp       time.Time // 时间戳
	TTL             time.Time // 生存时间
}

// AccoutState record the details of an account, it will be saved in status trie
type DEState struct {
	IdentifierRecord
	AcAddress   utils.Address // this part is not useful, abort
	Nonce       uint64
	StorageRoot []byte // only for smart contract account
	CodeHash    []byte // only for smart contract account
}

// Encode AccountState in order to store in the MPT
func (das *DEState) Encode() []byte {
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	err := encoder.Encode(das)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

// Decode DEAccountState 待替换DecodeAS
func DecodeASDE(b []byte) *DEState {
	var das DEState

	decoder := gob.NewDecoder(bytes.NewReader(b))
	err := decoder.Decode(&das)
	if err != nil {
		log.Panic(err)
	}
	return &das
}

// Hash DEAccountState for computing the MPT Root
func (das *DEState) Hash() []byte {
	h := sha256.Sum256(das.Encode())
	return h[:]
}

// RegisterIdentifier 注册标识符
func (das *DEState) RegisterIdentifier(tx *Transaction) error {
	das.Identifier = tx.Identifier
	das.Owner = tx.Sender
	das.Data = tx.Data
	das.Timestamp = tx.Time
	das.DataAddress = tx.DataAddress
	das.MetaDataAddress = tx.MetaDataAddress
	return nil
}

// UpdateIdentifier 更新标识符
func (das *DEState) UpdateIdentifier(tx *Transaction) error {
	// record, exists := das.Identifiers[tx.Identifier]
	// if !exists {
	// 	return errors.New("identifier not found")
	// }

	// if record.Owner != tx.Sender {
	// 	return errors.New("only the owner can update the identifier")
	// }

	// record.Data = tx.Data
	// record.Timestamp = tx.Time
	// das.Identifiers[tx.Identifier] = record
	return nil
}

// DeleteIdentifier 删除标识符
func (das *DEState) DeleteIdentifier(tx *Transaction) error {
	// record, exists := das.Identifiers[tx.Identifier]
	// if !exists {
	// 	return errors.New("identifier not found")
	// }

	// if record.Owner != tx.Sender {
	// 	return errors.New("only the owner can delete the identifier")
	// }

	// delete(das.Identifiers, tx.Identifier)
	return nil
}
