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
	"log"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type DENode struct {
	Prefix      string
	addr        string
	level       uint64
	parentMap   map[string]string
	siblingMap  map[string]string
	childrenMap map[string]string // domain -> addr
	reqCh       chan interface{}
	txCh        chan *core.Transaction
	stopCh      chan struct{}

	// logger
	Dl   *demis_log.DemisLog
	wait sync.WaitGroup

	// database
	db    *sql.DB
	cache cache.Cache
}

func NewDENode(shardID, nodeID uint64, cfg *params.DENodeConfig, deCh chan interface{}) *DENode {
	de := &DENode{
		Prefix:      cfg.Prefix,
		addr:        cfg.Addr,
		level:       calculateLevel(cfg.Prefix),
		parentMap:   cfg.ParentMap,
		siblingMap:  cfg.SiblingMap,
		childrenMap: cfg.ChildrenMap,
		reqCh:       deCh,
		txCh:        make(chan *core.Transaction, 1024),
	}
	dsn := "root:802157@tcp(127.0.0.1:3306)/de_mis?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Panic(err)
	}
	de.db = db
	de.Dl = demis_log.NewDemisLog(shardID, nodeID)

	if params.CacheEnable {
		de.cache = cache.NewSieve(params.CacheSize, shardID, nodeID)
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

func (de *DENode) RecvTx(tx *core.Transaction) {
	de.txCh <- tx
}

func (de *DENode) process() {
	defer de.wait.Done()

	for {
		select {
		case msg := <-de.reqCh:
			de.Dl.Dlog.Printf("Received message: %v", msg)
			// 在这里处理接收到的消息
			switch data := msg.(type) {
			case *message.PrefixQueryMessage:
				de.handlePrefixQuery(data)
			case *message.UnionQueryMessage:
				go de.handleUnionQuery(data)
			}
		case tx := <-de.txCh:
			// de.Dl.Dlog.Printf("Received transaction: %v", tx)
			switch tx.TxType {
			case core.Register:
				// err := de.insertTx(tx) // todo  type --> table
				// if err != nil {
				// 	de.Dl.Dlog.Printf("Error inserting transaction: %v", err)
				// }
			}

		case <-de.stopCh:
			de.Dl.Dlog.Printf("Stopping process goroutine")
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
	msg := *req

	if params.CacheEnable {
		de.Dl.Dlog.Println("ywb 查询cache")
		if value, exists := de.cache.Get(req.Identifier); exists {
			var nextAddr string
			switch data := value.(type) {
			case *core.IdentifierRecord:
				msg.Record = *data
				nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
			case string:
				msg.Status = message.PREFIX_QUERY_FORWARD
				nextAddr = data
			}

			// tcp send
			de.Dl.Dlog.Printf("ywb cache hit, send msg to : %v\n", value)
			utils.WriteMsg(msg, message.CPrefixQuery, nextAddr)
			return
		}
	}

	// startTime := time.Now()
	if req.Prefix == de.Prefix {
		msg.Type = message.RESPONSE
		msg.Status = message.FINISH
		record, err := de.queryHelper(req.Identifier)
		if err != nil {
			de.Dl.Dlog.Fatalf("Error querying identifier: %s", err)
		} else {
			msg.Record = *record
		}

		// tcp send
		de.Dl.Dlog.Printf("ywb sending finish to supervisor : %s\n", params.IPmap_nodeTable[params.SupervisorShard][0])
		utils.WriteMsg(msg, message.CPrefixQuery, params.IPmap_nodeTable[params.SupervisorShard][0])
		return
	}

	nextAddr := de.findNextAddr(req.Prefix)
	msg.Type = message.REQUEST
	msg.Status = message.PREFIX_QUERY_FORWARD
	if nextAddr != "" {
		// tcp send
		de.Dl.Dlog.Printf("ywb sending forward prefix query to %s \n", nextAddr)
		utils.WriteMsg(msg, message.CPrefixQuery, nextAddr)
	} else {
		// error todo
	}
}

func (de *DENode) handleRelayQuery(msg *message.PrefixQueryMessage) {
	resp := *msg
	if params.CacheEnable {
		if value, exists := de.cache.Get(msg.Identifier); exists {
			resp.Type = message.RESPONSE
			switch data := value.(type) {
			case *core.IdentifierRecord:
				resp.Status = message.FINISH
				resp.Record = *data
			case string:
				resp.Status = message.PREFIX_QUERY_FORWARD
				resp.TargetAddress = data
			}
			// tcp send
			de.Dl.Dlog.Printf("ywb cache hit, resp to proxy node : %v\n", value)
			utils.WriteMsg(resp, message.CPrefixQuery, msg.ProxyAddress)
			return
		}
	}

	if msg.Prefix == de.Prefix {
		resp.Type = message.RESPONSE
		resp.Status = message.FINISH
		resp.TargetAddress = de.addr
		record, err := de.queryHelper(msg.Identifier)
		if err != nil {
			de.Dl.Dlog.Fatalf("Error querying identifier: %s", err)
		} else {
			resp.Record = *record
		}
		// tcp send
		de.Dl.Dlog.Printf("ywb sending finish prefix query to proxy node : %s\n", msg.ProxyAddress)
		utils.WriteMsg(resp, message.CPrefixQuery, msg.ProxyAddress)
		return
	}

	nextAddr := de.findNextAddr(msg.Prefix)
	if nextAddr != "" {
		// tcp send back to proxy node
		resp.Type = message.RESPONSE
		resp.Status = message.PREFIX_QUERY_FORWARD
		resp.TargetAddress = nextAddr

		de.Dl.Dlog.Printf("ywb sending forward prefix resp with nextAddr %s  to proxy node %s \n", nextAddr, msg.ProxyAddress)
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
			de.Dl.Dlog.Printf("Received forward response: %v", resp)
			msg.Type = message.REQUEST
			msg.Status = message.PREFIX_QUERY_FORWARD
			nextAddr = resp.TargetAddress
		}
	case message.FINISH:
		{
			de.Dl.Dlog.Printf("Received finish response: %v", resp)
			// tcp send resp to spv
			nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
			// cache set todo test
			if params.CacheEnable {
				de.cache.Set(resp.Identifier, &resp.Record)
			}
		}
	case message.ERROR:
		{
			de.Dl.Dlog.Printf("Received error response: %v", resp)
			// tcp send resp to spv
			msg.Type = message.RESPONSE
			msg.Status = message.ERROR
			nextAddr = params.IPmap_nodeTable[params.SupervisorShard][0]
		}
	}

	de.Dl.Dlog.Printf("Sending forward prefix response to %s\n", nextAddr)

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
	curParts := strings.Split(de.Prefix, ".")
	targetParts := strings.Split(targetPrefix, ".")
	firstMismatchPos := calFirstMismatchPos(targetParts, curParts)
	de.Dl.Dlog.Printf("Current node prefix: %s, targetPrefix: %s, firstMismatchPos: %d, cur node level %d\n",
		de.Prefix, targetPrefix, firstMismatchPos, de.level)
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
		de.Dl.Dlog.Printf("下一跳 sibling prefix: %s", nextPrefix)
		if addr, exists := de.siblingMap[nextPrefix]; exists {
			nextAddr = addr
		}

	} else {
		// 向子节点请求
		nextPrefix := constructPrefix(targetParts, firstMismatchPos)
		de.Dl.Dlog.Printf("下一跳 child prefix: %s", nextPrefix)
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

func SplitIdentifier(identifier string) (string, string, string, error) {
	parts := strings.Split(identifier, "/")
	if len(parts) != 2 {
		return "", "", "", errors.New("invalid identifier format:" + identifier)
	}
	prefix := parts[0]
	subParts := strings.Split(parts[1], ":")
	if len(subParts) != 2 {
		return "", "", "", errors.New("invalid suffix format:" + parts[1])
	}
	typ := subParts[0]
	suffix := subParts[1]
	return prefix, typ, suffix, nil
}

// 查询本地标识符记录，与缓存无关
func (de *DENode) queryHelper(identifier string) (*core.IdentifierRecord, error) {
	// 在这里处理接收到的查询消息
	de.Dl.Dlog.Printf("queryHelper received query : %v", identifier)

	_, typ, _, _ := SplitIdentifier(identifier)
	record, err := de.queryByIdentifier(typ, identifier)
	if err != nil {
		return nil, err
	}

	de.Dl.Dlog.Printf("Query result: %v", record)
	result := record
	return result, nil
}

func (de *DENode) handleUnionQuery(msg *message.UnionQueryMessage) {
	// 在这里处理接收到的查询消息
	de.Dl.Dlog.Printf("Received query: %v", msg)

	prefix, typ, _, _ := SplitIdentifier(msg.Identifier)
	if prefix != de.Prefix {
		// todo forward
		return
	}

	res := make([]core.IdentifierRecord, 0)
	// 查询本地标识符记录
	if typ == params.Type0 {
		if exist, _ := de.isIdentifierExist(typ, msg.Identifier); exist {
			for _, t := range params.Types {
				record, err := de.queryByIdentity(t, msg.Identifier)
				if err != nil {
					de.Dl.Dlog.Printf("Error querying identifier: %v", err)
					continue
				}
				res = append(res, *record)
			}
		}
	} else {
		record, err := de.queryByIdentifier(typ, msg.Identifier)
		if err != nil {
			de.Dl.Dlog.Printf("Error querying identifier: %v", err)
			return
		}
		res = append(res, *record)
		identity := record.Identity
		for _, t := range params.Types {
			if t != typ {
				record, err := de.queryByIdentity(t, identity)
				if err != nil {
					de.Dl.Dlog.Printf("Error querying identifier: %v", err)
					continue
				}
				res = append(res, *record)
			}
		}
	}
	de.Dl.Dlog.Printf("Query result: %v", res)
	// tcp send
	msg.Result = res
	utils.WriteMsg(msg, message.CUnionQuery, params.IPmap_nodeTable[params.SupervisorShard][0])
}
