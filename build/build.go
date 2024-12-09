package build

import (
	"blockEmulator/consensus_shard/pbft_all"
	demis "blockEmulator/de_mis"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor"
	"fmt"
	"log"
	"time"
)

func initConfig(nid, nnm, sid, snm uint64) *params.ChainConfig {
	// Read the contents of ipTable.json
	ipMap := readIpTable("./ipTable.json")
	params.IPmap_nodeTable = ipMap
	params.SupervisorAddr = params.IPmap_nodeTable[params.SupervisorShard][0]

	// check the correctness of params
	if len(ipMap)-1 < int(snm) {
		log.Panicf("Input ShardNumber = %d, but only %d shards in ipTable.json.\n", snm, len(ipMap)-1)
	}
	for shardID := 0; shardID < len(ipMap)-1; shardID++ {
		if len(ipMap[uint64(shardID)]) < int(nnm) {
			log.Panicf("Input NodeNumber = %d, but only %d nodes in Shard %d.\n", nnm, len(ipMap[uint64(shardID)]), shardID)
		}
	}

	params.NodesInShard = int(nnm)
	params.ShardNum = int(snm)

	// init the network layer
	networks.InitNetworkTools()

	pcc := &params.ChainConfig{
		ChainID:        sid,
		NodeID:         nid,
		ShardID:        sid,
		Nodes_perShard: uint64(params.NodesInShard),
		ShardNums:      snm,
		BlockSize:      uint64(params.MaxBlockSize_global),
		BlockInterval:  uint64(params.Block_Interval),
		InjectSpeed:    uint64(params.InjectSpeed),
	}
	return pcc
}

func initDENodeConfig(nid, sid uint64) *params.DENodeConfig {
	// Read the contents of domainConfig.json
	domainMap := readDomainInfo(fmt.Sprintf("./nodes/S%d/N%d/domainConfig.json", sid, nid))
	fmt.Println(domainMap)
	parentMap := domainMap[params.Parent]
	siblingMap := domainMap[params.Sibling]
	childrenMap := domainMap[params.Children]
	ipMap := readIpTable("./ipTable.json")
	pcc := &params.DENodeConfig{
		Prefix:       domainMap[params.Prefix][params.CurPrefix],
		Addr:         ipMap[sid][nid],
		ParentMap:    parentMap,
		SiblingMap:   siblingMap,
		ChildrenMap:  childrenMap,
	}
	fmt.Println(pcc)
	fmt.Println("Prefix: ", pcc.Prefix)
	fmt.Println("Addr: ", pcc.Addr)
	return pcc
}

func BuildSupervisor(nnm, snm uint64) {
	methodID := params.ConsensusMethod
	var measureMod []string
	if methodID == 0 || methodID == 2 {
		measureMod = params.MeasureBrokerMod
	} else {
		measureMod = params.MeasureRelayMod
	}
	measureMod = append(measureMod, "Tx_Details")

	lsn := new(supervisor.Supervisor)
	lsn.NewSupervisor(params.SupervisorAddr, initConfig(123, nnm, 123, snm), params.CommitteeMethod[methodID], measureMod...)
	time.Sleep(10000 * time.Millisecond)
	go lsn.SupervisorTxHandling()
	go lsn.StartRPCServer()
	lsn.TcpListen()
}

func BuildNewPbftNode(nid, nnm, sid, snm uint64) {
	methodID := params.ConsensusMethod
	deReqCh := make(chan interface{}, 1024)
	// txCh := make(chan *core.Transaction, 1024)
	deNode := demis.NewDENode(sid, nid, initDENodeConfig(nid, sid), deReqCh)
	worker := pbft_all.NewPbftNode(sid, nid, initConfig(nid, nnm, sid, snm), params.CommitteeMethod[methodID], deReqCh, deNode)
	
	// ywb 这里nid是本节点的id，sid是本节点所在分片的id，可以根据这个去读取指定目录的domain的配置文件
	go worker.TcpListen()

	// ywb 启动一个goroutine，用来处理resolve消息
	go deNode.Start()

	worker.Propose()
}
