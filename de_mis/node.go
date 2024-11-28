package de_mis

import (
	"blockEmulator/core"
	"blockEmulator/de_mis/cache"
	"blockEmulator/de_mis/demis_log"
	"blockEmulator/de_mis/utils"
	"blockEmulator/message"
	"blockEmulator/params"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type DENode struct {
	prefix      string
	addr        string
	level       uint64
	parentMap   map[string]string
	siblingMap  map[string]string
	childrenMap map[string]string // domain -> addr
	reqCh       chan interface{}
	txCh        chan *core.Transaction
	stopCh      chan struct{}

	// logger
	dl   *demis_log.DemisLog
	wait sync.WaitGroup

	// database
	db    *sql.DB
	cache cache.Cache
}

func NewDENode(shardID, nodeID uint64, cfg *params.DENodeConfig, deCh chan interface{}, txCh chan *core.Transaction) *DENode {
	de := &DENode{
		prefix:      cfg.Prefix,
		addr:        cfg.Addr,
		level:       calculateLevel(cfg.Prefix),
		parentMap:   cfg.ParentMap,
		siblingMap:  cfg.SiblingMap,
		childrenMap: cfg.ChildrenMap,
		reqCh:       deCh,
		txCh:        txCh,
	}
	dsn := "root:802157@tcp(127.0.0.1:3306)/de_mis?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Panic(err)
	}
	de.db = db
	de.dl = demis_log.NewDemisLog(shardID, nodeID)

	if params.CacheEnable {
		de.cache = cache.NewSieve(params.CacheSize)
	}
	return de
}

func (de *DENode) Start() {
	de.wait.Add(1)
	go de.process()
	de.wait.Wait()
}

func (de *DENode) Stop() {
	close(de.stopCh)
}

func (de *DENode) process() {
	defer de.wait.Done()

	for {
		select {
		case msg := <-de.reqCh:
			de.dl.Dlog.Printf("Received message: %v", msg)
			// 在这里处理接收到的消息
			// switch data := msg.(type) {
			// case *message.PrefixQueryMessage:
			// 	de.handlePrefixQuery(data)
			// case *message.QueryMessage:
			// 	de.handleQuery(data)
			// }
			de.handlePrefixQuery(msg.(*message.PrefixQueryMessage))
		case tx := <-de.txCh:
			de.dl.Dlog.Printf("Received transaction: %v", tx)
			switch tx.TxType {
			case core.Register:
				err := de.insertTx(tx) // todo  type --> table
				if err != nil {
					de.dl.Dlog.Printf("Error inserting transaction: %v", err)
				}
			}

		case <-de.stopCh:
			de.dl.Dlog.Printf("Stopping process goroutine")
			return
		}
	}
}

// func (de *DENode) createTables() {
//     // 创建表示例
//     createTableQueries := []string{
//         fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
//             id INT AUTO_INCREMENT PRIMARY KEY,
//             identifier VARCHAR(255) NOT NULL,
//             owner VARCHAR(255) NOT NULL,
//             data TEXT,
//             timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
//             data_address VARCHAR(255),
//             metadata_address VARCHAR(255)
//         )`, de.table),
//         // 可以在这里添加更多的表
//     }

//     for _, query := range createTableQueries {
//         _, err := de.db.Exec(query)
//         if err != nil {
//             log.Panic(err)
//         }
//     }
// }

func (de *DENode) insertTx(tx *core.Transaction) error {
	// 插入交易到数据库的示例
	query := fmt.Sprintf(`INSERT INTO t_%s (identifier, username, registration_time, expiration_time, data_address, metadata_address, data) VALUES (?, ?, ?, ?, ?, ?, ?)`, tx.IType)
	_, err := de.db.Exec(query, tx.Identifier, tx.Sender, tx.Time, nil, tx.DataAddress, tx.MetaDataAddress, tx.Data)
	return err
}

func (de *DENode) queryByIdentifier(typ, identifier string) (*core.IdentifierRecord, error) {
	query := fmt.Sprintf(`SELECT identifier, username, registration_time, expiration_time, data_address, metadata_address, data FROM t_%s WHERE identifier = ?`, typ)
	row := de.db.QueryRow(query, identifier)

	var identifierResult, username, dataAddress, metadataAddress string
	var registrationTime, expirationTime sql.NullTime
	var data []byte

	err := row.Scan(&identifierResult, &username, &registrationTime, &expirationTime, &dataAddress, &metadataAddress, &data)
	if err != nil {
		return nil, err
	}

	record := &core.IdentifierRecord{
		Identifier:      identifierResult,
		Owner:           username,
		Data:            data,
		DataAddress:     dataAddress,
		MetaDataAddress: metadataAddress,
		Timestamp:       registrationTime.Time,
		TTL:             expirationTime.Time,
	}

	return record, nil
}

func (de *DENode) handlePrefixQuery(msg *message.PrefixQueryMessage) {
	switch msg.Type {
	case message.REQUEST:
		{
			if msg.Status == message.PREFIX_QUERY_FIRST {
				go de.handleFirstQuery(msg)
			} else if msg.Status == message.PREFIX_QUERY_FORWARD {
				go de.handleRelayQuery(msg)
			}
		}
	case message.RESPONSE:
		{
			go de.handlePrefixQueryResp(msg)
		}
	}
}

func (de *DENode) handleFirstQuery(req *message.PrefixQueryMessage) {
	if params.CacheEnable {
		if record, exists := de.cache.Get(req.Prefix); exists {
			resp := message.PrefixQueryMessage{Identifier: req.Identifier, Record: *(record.(*core.IdentifierRecord))} // todo
			// tcp send
			de.dl.Dlog.Printf("ywb cache hit, resp to supervisor : %s\n", record)
			utils.WriteMsg(resp, message.CPrefixQuery, params.IPmap_nodeTable[params.SupervisorShard][0])
			return
		}
	}

	// startTime := time.Now()
	if req.Prefix == de.prefix {
		resp := message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.FINISH,
			Prefix: req.Prefix, Identifier: req.Identifier, TargetAddress: de.addr}
		record, err := de.queryHelper(req.Identifier)
		if err != nil {
			de.dl.Dlog.Fatalf("Error querying identifier: %s", err)
		} else {
			resp.Record = *record
		}

		// tcp send
		de.dl.Dlog.Printf("ywb sending finish to supervisor : %s\n", params.IPmap_nodeTable[params.SupervisorShard][0])
		utils.WriteMsg(resp, message.CPrefixQuery, params.IPmap_nodeTable[params.SupervisorShard][0])
		return
	}

	nextAddr := de.findNextAddr(req.Prefix)
	nextReq := message.PrefixQueryMessage{Type: message.REQUEST, Status: message.PREFIX_QUERY_FORWARD,
		Prefix: req.Prefix, Identifier: req.Identifier, ProxyAddress: de.addr}
	if nextAddr != "" {
		// tcp send
		de.dl.Dlog.Printf("ywb sending forward prefix query to %s \n", nextAddr)
		utils.WriteMsg(nextReq, message.CPrefixQuery, nextAddr)
	} else {
		// error todo
	}
}

func (de *DENode) handleRelayQuery(msg *message.PrefixQueryMessage) {
	if params.CacheEnable {
		if record, exists := de.cache.Get(msg.Prefix); exists {
			resp := message.PrefixQueryMessage{Identifier: msg.Identifier, Record: *(record.(*core.IdentifierRecord))} // todo
			// tcp send
			de.dl.Dlog.Printf("ywb cache hit, resp to proxy node : %s\n", record)
			utils.WriteMsg(resp, message.CPrefixQuery, msg.ProxyAddress)
			return
		}
	}

	if msg.Prefix == de.prefix {
		resp := message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.FINISH, Prefix: msg.Prefix,
			Identifier: msg.Identifier, ProxyAddress: msg.ProxyAddress, TargetAddress: de.addr}
		record, err := de.queryHelper(msg.Identifier)
		if err != nil {
			de.dl.Dlog.Fatalf("Error querying identifier: %s", err)
		} else {
			resp.Record = *record
		}
		// tcp send
		de.dl.Dlog.Printf("ywb sending finish prefix query to proxy node : %s\n", msg.ProxyAddress)
		utils.WriteMsg(resp, message.CPrefixQuery, msg.ProxyAddress)
		return
	}

	nextAddr := de.findNextAddr(msg.Prefix)
	if nextAddr != "" {
		// tcp send back to proxy node
		resp := message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.PREFIX_QUERY_FORWARD,
			Prefix: msg.Prefix, Identifier: msg.Identifier, ProxyAddress: de.addr, TargetAddress: nextAddr}

		de.dl.Dlog.Printf("ywb sending forward prefix resp with nextAddr %s  to proxy node %s \n", nextAddr, msg.ProxyAddress)
		utils.WriteMsg(resp, message.CPrefixQuery, msg.ProxyAddress)
	} else {
		// error todo
	}
}

func calculateLevel(prefix string) uint64 {
	parts := strings.Split(prefix, ".")
	var level uint64 = 0
	for _, part := range parts {
		if part != "" {
			level++
		}
	}
	return level
}

func (de *DENode) handlePrefixQueryResp(resp *message.PrefixQueryMessage) {
	// 在这里处理接收到的响应消息、
	var nextAddr string
	msg := *resp
	switch resp.Status {
	case message.PREFIX_QUERY_FORWARD:
		{
			de.dl.Dlog.Printf("Received forward response: %v", resp)
			msg.Type = message.REQUEST
			msg.Status = message.PREFIX_QUERY_FORWARD
			nextAddr = resp.TargetAddress
		}
	case message.FINISH:
		{
			de.dl.Dlog.Printf("Received finish response: %v", resp)
			// tcp send resp to spv
			nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
			// cache set todo test
			if params.CacheEnable {
				de.cache.Set(resp.Prefix, &resp.Record)
			}
		}
	case message.ERROR:
		{
			de.dl.Dlog.Printf("Received error response: %v", resp)
			// tcp send resp to spv
			msg.Type = message.RESPONSE
			msg.Status = message.ERROR
			nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
		}
	}

	de.dl.Dlog.Printf("Sending forward prefix response to %s\n", nextAddr)

	utils.WriteMsg(msg, message.CPrefixQuery, nextAddr)
}

// return 1 ~ n
func calFirstMismatchPos(targetParts, curParts []string) uint64 {
	log.Printf("ndParts: %v, targetParts: %v", curParts, targetParts)

	i := 0
	for i < len(curParts) && i < len(targetParts) {
		if curParts[i] != targetParts[i] {
			return uint64(i + 1)
		}
		i++
	}

	return uint64(i + 1)
}

func (de *DENode) findNextAddr(targetPrefix string) string {
	curParts := strings.Split(de.prefix, ".")
	targetParts := strings.Split(targetPrefix, ".")
	firstMismatchPos := calFirstMismatchPos(targetParts, curParts)
	de.dl.Dlog.Printf("Current node prefix: %s, targetPrefix: %s, firstMismatchPos: %d, cur node level %d\n",
		de.prefix, targetPrefix, firstMismatchPos, de.level)
	// 根据层级关系选择向下一个节点请求
	targetLevel := calculateLevel(targetPrefix)
	var nextAddr string = ""
	if targetLevel < de.level || firstMismatchPos < de.level {
		// 向父节点请求
		if len(de.parentMap) > 0 {
			for _, addr := range de.parentMap {
				nextAddr = addr
				break
			}
		}
	} else if firstMismatchPos == de.level {
		// 向兄弟节点请求
		nextPrefix := constructPrefix(targetParts, firstMismatchPos)
		de.dl.Dlog.Printf("下一跳 sibling prefix: %s", nextPrefix)
		if addr, exists := de.siblingMap[nextPrefix]; exists {
			nextAddr = addr
		}

	} else {
		// 向子节点请求
		nextPrefix := constructPrefix(targetParts, firstMismatchPos)
		de.dl.Dlog.Printf("下一跳 child prefix: %s", nextPrefix)
		if addr, exists := de.childrenMap[nextPrefix]; exists {
			nextAddr = addr
		}
	}
	return nextAddr
}

// [0, mismatchPos - 1]
func constructPrefix(segments []string, mismatchPos uint64) string {
	if mismatchPos <= 0 || int(mismatchPos) > len(segments) {
		return ""
	}
	return strings.Join(segments[:mismatchPos], ".")
}

// 查询本地标识符记录，与缓存无关
func (de *DENode) queryHelper(identifier string) (*core.IdentifierRecord, error) {
	// 在这里处理接收到的查询消息
	de.dl.Dlog.Printf("queryHelper received query : %v", identifier)
	parts := strings.Split(identifier, "/")
	if len(parts) != 2 {
		return nil, errors.New("invalid identifier format:" + identifier)
	}
	// prefix := parts[0]
	suffix := parts[1]
	subParts := strings.Split(suffix, ":")
	if len(subParts) != 2 {
		return nil, errors.New("invalid suffix format:" + suffix)
	}
	typ := subParts[0]

	record, err := de.queryByIdentifier(typ, identifier)
	if err != nil {
		return nil, err
	}

	de.dl.Dlog.Printf("Query result: %v", record)
	result := record
	return result, nil
}
