package chain

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/utils"
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

// Generate proof for the tx which hash is txHash.
// Find all blocks in this chain.
func (bc *BlockChain) TxProofGenerate(txHash []byte) message.TxProofResult {
	nowblockHash := bc.CurrentBlock.Hash
	nowheight := bc.CurrentBlock.Header.Number

	for ; nowheight > 0; nowheight-- {
		// get a block from db
		block, err1 := bc.Storage.GetBlock(nowblockHash)
		if err1 != nil {
			return message.TxProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  err1.Error(),
			}
		}
		if ret := TxProofGenerateOnTheBlock(txHash, block); ret.Found {
			return ret
		}

		// go into next block
		nowblockHash = block.Header.ParentBlockHash
	}

	return message.TxProofResult{
		Found:  false,
		TxHash: txHash,
		Error:  errors.New("cannot find this tx").Error(),
	}
}

// Make Tx proof on a certain block.
func TxProofGenerateOnTheBlock(txHash []byte, block *core.Block) message.TxProofResult {
	// If no value in bloom filter, then the tx must not be in this block
	bitMapIdxofTx := utils.ModBytes(txHash, 2048)
	if !block.Header.Bloom.Test(bitMapIdxofTx) {
		return message.TxProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  errors.New("cannot find this tx").Error(),
		}
	}

	// now try to find whether this tx is in this block
	// check the correctness of this tx Trie
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range block.Body {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	if !bytes.Equal(transactionTree.Hash().Bytes(), block.Header.TxRoot) {
		return message.TxProofResult{
			Found:  false,
			TxHash: txHash,
			Error:  fmt.Errorf("tx root mismatch in height %d", block.Header.Number).Error(),
		}
	}

	// generate proof
	keylist, valuelist := make([][]byte, 0), make([][]byte, 0)
	proof := rawdb.NewMemoryDatabase()
	if err := transactionTree.Prove(txHash, 0, proof); err == nil {
		it := proof.NewIterator(nil, nil)
		for it.Next() {
			keylist = append(keylist, it.Key())
			valuelist = append(valuelist, it.Value())
		}
		return message.TxProofResult{
			Found:       true,
			BlockHash:   block.Hash,
			TxHash:      txHash,
			TxRoot:      block.Header.TxRoot,
			BlockHeight: block.Header.Number,
			KeyList:     keylist,
			ValueList:   valuelist,
		}
	}
	return message.TxProofResult{
		Found:  false,
		TxHash: txHash,
		Error:  errors.New("cannot find this tx").Error(),
	}
}

func TxProofBatchGenerateOnBlock(txHashes [][]byte, block *core.Block) []message.TxProofResult {
	txProofs := make([]message.TxProofResult, len(txHashes))
	// check the tx trie first.
	// check the correctness of this tx Trie
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range block.Body {
		transactionTree.Update(tx.TxHash, []byte{0})
	}
	if !bytes.Equal(transactionTree.Hash().Bytes(), block.Header.TxRoot) {
		for i := 0; i < len(txHashes); i++ {
			txProofs[i] = message.TxProofResult{
				Found:  false,
				TxHash: txHashes[i],
				Error:  fmt.Errorf("tx root mismatch in height %d", block.Header.Number).Error(),
			}
		}
		return txProofs
	}

	for idx, txHash := range txHashes {
		bitMapIdxofTx := utils.ModBytes(txHash, 2048)
		if !block.Header.Bloom.Test(bitMapIdxofTx) {
			txProofs[idx] = message.TxProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  errors.New("cannot find this tx").Error(),
			}
			continue
		}
		// generate proof
		keylist, valuelist := make([][]byte, 0), make([][]byte, 0)
		proof := rawdb.NewMemoryDatabase()
		if err := transactionTree.Prove(txHash, 0, proof); err == nil {
			it := proof.NewIterator(nil, nil)
			for it.Next() {
				keylist = append(keylist, it.Key())
				valuelist = append(valuelist, it.Value())
			}
			txProofs[idx] = message.TxProofResult{
				Found:       true,
				BlockHash:   block.Hash,
				TxHash:      txHash,
				TxRoot:      block.Header.TxRoot,
				BlockHeight: block.Header.Number,
				KeyList:     keylist,
				ValueList:   valuelist,
			}
		} else {
			txProofs[idx] = message.TxProofResult{
				Found:  false,
				TxHash: txHash,
				Error:  errors.New("cannot find this tx").Error(),
			}
		}
	}
	return txProofs
}

func TxProofVerify(txHash []byte, proof *message.TxProofResult) (bool, error) {
	if !proof.Found {
		return false, errors.New("the result shows not found")
	}

	// check the proof
	recoveredProof := rawdb.NewMemoryDatabase()
	listLen := len(proof.KeyList)
	for i := 0; i < listLen; i++ {
		recoveredProof.Put(proof.KeyList[i], proof.ValueList[i])
	}
	if _, err := trie.VerifyProof(common.BytesToHash(proof.TxRoot), []byte(proof.TxHash), recoveredProof); err != nil {
		return false, errors.New("wrong proof")
	}

	return true, nil
}
