package de_mis

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"database/sql"
	"errors"
	"fmt"
)

func (de *DENode) insertTx(tx *core.Transaction) error {
	// 插入交易到数据库的示例
	query := fmt.Sprintf(`INSERT INTO t_%s (identifier, username, registration_time, expiration_time, data_address, metadata_address, data) VALUES (?, ?, ?, ?, ?, ?, ?)`, tx.IType)
	_, err := de.db.Exec(query, tx.Identifier, tx.Sender, tx.Time, nil, tx.DataAddress, tx.MetaDataAddress, tx.Digest)
	return err
}

func (de *DENode) insert2Identity(identifier, username string, registrationTime, expirationTime sql.NullTime, dataAddress, metadataAddress string, digest []byte) error {
	query := `INSERT INTO t_type0 (identifier, username, registration_time, expiration_time, data_address, metadata_address, digest) VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := de.db.Exec(query, identifier, username, registrationTime, expirationTime, dataAddress, metadataAddress, digest)
	return err
}

func (de *DENode) insert2Normal(typ, identifier, identity, username string, registrationTime, expirationTime sql.NullTime,
	metadataAddress, dataAddress string, digest []byte) error {
	if typ == params.Type0 {
		return errors.New("type0 not allowed")
	}

	query := fmt.Sprintf(`INSERT INTO t_%s (identifier, identity, username, registration_time, expiration_time, metadata_address, data_address, digest) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, typ)
	_, err := de.db.Exec(query, identifier, identity, username, registrationTime, expirationTime, metadataAddress, dataAddress, digest)
	return err
}

func (de *DENode) queryByIdentifier(typ, identifier string) (*core.IdentifierRecord, error) {
	var identifierResult, identity, username string
	var dataAddress, metadataAddress sql.NullString
	var registrationTime, expirationTime sql.NullTime
	var digest []byte
	var record *core.IdentifierRecord

	if typ == params.Type0 {
		query := fmt.Sprintf(`SELECT identifier, username, registration_time, expiration_time, data_address, metadata_address, digest FROM t_%s WHERE identifier = ?`, typ)
		row := de.db.QueryRow(query, identifier)

		err := row.Scan(&identifierResult, &username, &registrationTime, &expirationTime, &dataAddress, &metadataAddress, &digest)
		if err != nil {
			return nil, err
		}
		record = &core.IdentifierRecord{
			Identifier:      identifierResult,
			Owner:           username,
			Digest:          digest,
			DataAddress:     dataAddress.String,
			MetaDataAddress: metadataAddress.String,
			Timestamp:       registrationTime.Time,
			TTL:             expirationTime.Time,
			Addr:            de.addr,
		}
	} else {
		query := fmt.Sprintf(`SELECT identifier, identity, username, registration_time, expiration_time, data_address, metadata_address, digest FROM t_%s WHERE identifier = ?`, typ)
		row := de.db.QueryRow(query, identifier)
		err := row.Scan(&identifierResult, &identity, &username, &registrationTime, &expirationTime, &dataAddress, &metadataAddress, &digest)
		if err != nil {
			return nil, err
		}
		record = &core.IdentifierRecord{
			Identifier:      identifierResult,
			Owner:           username,
			Digest:          digest,
			DataAddress:     dataAddress.String,
			MetaDataAddress: metadataAddress.String,
			Timestamp:       registrationTime.Time,
			TTL:             expirationTime.Time,
			Addr:            de.addr,
			Identity: 	     identity,
		}
	}

	return record, nil
}

func (de *DENode) queryByIdentity(typ, identity string) (*core.IdentifierRecord, error) {
	query := fmt.Sprintf(`SELECT identifier, username, registration_time, expiration_time, data_address, metadata_address, digest FROM t_%s WHERE identity = ?`, typ)
	row := de.db.QueryRow(query, identity)

	var identifierResult, username string
	var dataAddress, metadataAddress sql.NullString
	var registrationTime, expirationTime sql.NullTime
	var digest []byte

	err := row.Scan(&identifierResult, &username, &registrationTime, &expirationTime, &dataAddress, &metadataAddress, &digest)
	if err != nil {
		return nil, err
	}

	record := &core.IdentifierRecord{
		Identifier:      identifierResult,
		Owner:           username,
		Digest:          digest,
		DataAddress:     dataAddress.String,
		MetaDataAddress: metadataAddress.String,
		Timestamp:       registrationTime.Time,
		TTL:             expirationTime.Time,
		Addr:            de.addr,
	}

	de.Dl.Dlog.Printf("queryByIdentity: %v", record)
	de.Dl.Dlog.Printf("queryByIdentity addr: %s", record.Addr)

	return record, nil
}

func (de *DENode) queryByUsername(typ, un string) (*core.IdentifierRecord, error) {
	query := fmt.Sprintf(`SELECT username, domain, identifier, registration_time, expiration_time, metadata_address FROM t_%s WHERE username = ?`, typ)
	row := de.db.QueryRow(query, un)

	var username, domain, identifier string
	var metadataAddress sql.NullString
	var registrationTime, expirationTime sql.NullTime

	err := row.Scan(&username, &domain, &identifier, &registrationTime, &expirationTime, &metadataAddress)
	if err != nil {
		return nil, err
	}

	record := &core.IdentifierRecord{
		Identifier:      identifier,
		Owner:           username,
		MetaDataAddress: metadataAddress.String,
		Timestamp:       registrationTime.Time,
		TTL:             expirationTime.Time,
	}

	return record, nil
}

func (de *DENode) isIdentifierExist(typ, identifier string) (bool, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) FROM t_%s WHERE identifier = ?`, typ)
	row := de.db.QueryRow(query, identifier)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func (de *DENode) isIdentityExist(typ, identity string) (bool, error) {
	if typ == params.Type0 {
		return false, errors.New("type0 not allowed")
	}

	query := fmt.Sprintf(`SELECT COUNT(*) FROM t_%s WHERE identity = ?`, typ)
	row := de.db.QueryRow(query, identity)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}