package de_mis

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
)

func (de *DENode) BuildIdentifier(prefix, typ, suffix string) string {
	return prefix + "/" + typ + ":" + suffix
}

func (de *DENode) HandleRegister(tx *core.Transaction) error {
	// 1. 从用户表中查sender的身份标识符
	record, err := de.queryByUsername("user", tx.Sender)
	if err != nil {
		return err
	}

	// 2. 根据用户身份标识符和数据要素摘要为该数据要素标识符$identifier_{X}$生成唯一的身份标识符$identifier_{I}$。
	identity := record.Identifier
	combinedData := append([]byte(identity), tx.Digest...)
	hash := sha256.Sum256(combinedData)
	suffix := hex.EncodeToString(hash[:])
	t_identity := de.BuildIdentifier(tx.Prefix, params.Type0, suffix)

	de.Dl.Dlog.Printf("Registering identifier %s", t_identity)

	// 3. 查询是否已存在该身份标识符
	exist1, _ := de.isIdentifierExist(params.Type0, t_identity)
	exist2, _ := de.isIdentityExist(tx.IType, t_identity)

	if !exist1 {
		// 4. 若不存在则将该数据要素标识符对应的身份标识符$identifier_{I}$写入身份标识符表
		if err2 := de.insert2Identity(t_identity, tx.Sender, sql.NullTime{Time: tx.Time, Valid: true},
			sql.NullTime{}, tx.DataAddress, tx.MetaDataAddress, tx.Digest); err2 != nil {
			de.Dl.Dlog.Fatal("herararearaerar3333333333333")
			return err2
		}
	}

	// 5. 将该数据要素标识符$identifier_{X}$写入标识符表
	if !exist2 {
		if err3 := de.insert2Normal(tx.IType, tx.Identifier, t_identity, tx.Sender, sql.NullTime{Time: tx.Time, Valid: true},
			sql.NullTime{}, tx.DataAddress, tx.MetaDataAddress, tx.Digest); err3 != nil {
			return err3
		}
	}

	return nil
}
