package demis

import (
	"blockEmulator/core"
	"blockEmulator/de-mis/demis_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"database/sql"
	"encoding/json"
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
	db *sql.DB
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
			switch data := msg.(type) {
			case *message.PrefixQueryMessage:
				de.handlePrefixQuery(data)
			case *message.QueryMessage:
				de.handleQuery(data)
			}
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
	// startTime := time.Now()
	if req.Prefix == de.prefix {
		res := message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.FINISH,
			Prefix: req.Prefix, Identifier: req.Identifier, TargetAddress: de.addr}
		// tcp send
		de.dl.Dlog.Printf("ywb sending forward prefix query to supervisor : %s\n", params.IPmap_nodeTable[params.SupervisorShard][0])
		itByte, err := json.Marshal(res)
		if err != nil {
			log.Panic(err)
		}
		send_msg := message.MergeMessage(message.CPrefixQuery, itByte)
		go networks.TcpDial(send_msg, params.IPmap_nodeTable[params.SupervisorShard][0])
		return
	}

	nextAddr := de.findNextAddr(req.Prefix)
	nextReq := message.PrefixQueryMessage{Type: message.REQUEST, Status: message.PREFIX_QUERY_FORWARD,
		Prefix: req.Prefix, Identifier: req.Identifier, ProxyAddress: de.addr}
	if nextAddr != "" {
		// tcp send
		de.dl.Dlog.Printf("ywb sending forward prefix query to %s \n", nextAddr)
		itByte, err := json.Marshal(nextReq)
		if err != nil {
			log.Panic(err)
		}
		send_msg := message.MergeMessage(message.CPrefixQuery, itByte)
		go networks.TcpDial(send_msg, nextAddr)
	} else {
		// error todo
	}
}

func (de *DENode) handleRelayQuery(msg *message.PrefixQueryMessage) {
	if msg.Prefix == de.prefix {
		res := message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.FINISH, Prefix: msg.Prefix,
			Identifier: msg.Identifier, ProxyAddress: msg.ProxyAddress, TargetAddress: de.addr}
		// tcp send
		de.dl.Dlog.Printf("ywb sending finish prefix query to proxy node : %s\n", msg.ProxyAddress)
		itByte, err := json.Marshal(res)
		if err != nil {
			log.Panic(err)
		}
		send_msg := message.MergeMessage(message.CPrefixQuery, itByte)
		go networks.TcpDial(send_msg, msg.ProxyAddress)
		return
	}

	nextAddr := de.findNextAddr(msg.Prefix)
	if nextAddr != "" {
		// tcp send back to proxy node
		resp := message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.PREFIX_QUERY_FORWARD,
			Prefix: msg.Prefix, Identifier: msg.Identifier, ProxyAddress: de.addr, TargetAddress: nextAddr}
		de.dl.Dlog.Printf("ywb sending forward prefix resp with nextAddr %s  to proxy node %s \n", nextAddr, msg.ProxyAddress)
		itByte, err := json.Marshal(resp)
		if err != nil {
			log.Panic(err)
		}
		send_msg := message.MergeMessage(message.CPrefixQuery, itByte)
		go networks.TcpDial(send_msg, msg.ProxyAddress)
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
	var msg message.PrefixQueryMessage
	switch resp.Status {
	case message.PREFIX_QUERY_FORWARD:
		{
			de.dl.Dlog.Printf("Received forward response: %v", resp)
			// tcp send
			msg = message.PrefixQueryMessage{Type: message.REQUEST, Status: message.PREFIX_QUERY_FORWARD,
				Prefix: resp.Prefix, Identifier: resp.Identifier, ProxyAddress: de.addr}
			nextAddr = resp.TargetAddress
		}
	case message.FINISH:
		{
			de.dl.Dlog.Printf("Received finish response: %v", resp)
			// tcp send resp to spv
			msg = message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.FINISH,
				Prefix: resp.Prefix, Identifier: resp.Identifier, ProxyAddress: de.addr, TargetAddress: resp.TargetAddress}
			nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
		}
	case message.ERROR:
		{
			de.dl.Dlog.Printf("Received error response: %v", resp)
			// tcp send resp to spv
			msg = message.PrefixQueryMessage{Type: message.RESPONSE, Status: message.ERROR,
				Prefix: resp.Prefix, Identifier: resp.Identifier, ProxyAddress: de.addr}
			nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
		}
	}

	de.dl.Dlog.Printf("Sending forward prefix response to %s\n", nextAddr)
	itByte, err := json.Marshal(msg)
	if err != nil {
		log.Panic(err)
	}
	send_msg := message.MergeMessage(message.CPrefixQuery, itByte)
	go networks.TcpDial(send_msg, nextAddr)
}

// return 1 ~ n
func calFirstMismatchPos(targetParts, curParts []string) uint64 {

	log.Printf("ndParts: %v, targetParts: %v", curParts, targetParts)

	// log.Info().Msgf("i: %d, ndParts[%d]: %s, targetParts[%d]: %s", i, i, ndParts[i], i, targetParts[i])

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
		// for prefix, addr := range de.siblingMap {
		// 	mismatchPos := calFirstMismatchPos(targetParts, strings.Split(prefix, "."))
		// 	de.dl.Dlog.Printf("sibling prefix: %s, targetPrefix: %s, mismatchIndex: %d", prefix, targetPrefix, mismatchPos)
		// 	if mismatchPos > firstMismatchPos {
		// 		nextAddr = addr
		// 		break
		// 	}
		// }

	} else {
		// 向子节点请求
		nextPrefix := constructPrefix(targetParts, firstMismatchPos)
		de.dl.Dlog.Printf("下一跳 child prefix: %s", nextPrefix)
		if addr, exists := de.childrenMap[nextPrefix]; exists {
			nextAddr = addr
		}
		// for prefix, addr := range de.childrenMap {
		// 	mismatchPos := calFirstMismatchPos(targetParts, strings.Split(prefix, "."))
		// 	de.dl.Dlog.Printf("child prefix: %s, targetPrefix: %s, mismatchIndex: %d", prefix, targetPrefix, mismatchPos)
		// 	if mismatchPos > firstMismatchPos {
		// 		nextAddr = addr
		// 		break
		// 	}
		// }
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

func (de *DENode) handleQuery(msg *message.QueryMessage) {
	// 在这里处理接收到的查询消息
	de.dl.Dlog.Printf("Received query message: %v", msg)

	parts := strings.Split(msg.Identifier, "/")
	if len(parts) != 2 {
		de.dl.Dlog.Printf("Invalid query format: %v", msg.Identifier)
		return
	}
	// prefix := parts[0]
	suffix := parts[1]
	subParts := strings.Split(suffix, ":")
	if len(subParts) != 2 {
		de.dl.Dlog.Printf("Invalid suffix format: %v", suffix)
		return
	}
	typ := subParts[0]
	de.dl.Dlog.Printf("Extracted query type: %s", typ)

	record, err := de.queryByIdentifier(typ, msg.Identifier)
	if err != nil {
		de.dl.Dlog.Fatalf("query error: %s", err)
		return
	}

	de.dl.Dlog.Printf("Query result: %v", record)
	msg.Result = *record
	itByte, err := json.Marshal(msg)
	if err != nil {
		log.Panic(err)
	}
	send_msg := message.MergeMessage(message.CIdentifierQuery, itByte)
	go networks.TcpDial(send_msg, params.IPmap_nodeTable[params.SupervisorShard][0])
}
