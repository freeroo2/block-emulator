// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	types "blockEmulator/rpc"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule

	// diy, add more structures or classes here ...
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.Ss = signal.NewStopSignal(3 * int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	default:
		d.comMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		default:
		}
	}
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		// d.sl.Slog.Println("here +++++++++++++++++")
		d.Ss.StopGap_Inc()
	} else {
		// d.sl.Slog.Println("here ----------------")
		d.Ss.StopGap_Reset()
	}

	d.comMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	// add codes here ...
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {

	// d.comMod.MsgSendingControl()

	// TxHandling is end
	// d.sl.Slog.Println("here 1111111111111111111")
	for !d.Ss.GapEnough() { // wait all txs to be handled
		// d.sl.Slog.Println("here 2222222222222")
		time.Sleep(time.Second)
	}
	// send stop message
	// stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	// d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	// for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
	// 	for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
	// 		networks.TcpDial(stopmsg, d.Ip_nodeTable[sid][nid])
	// 	}
	// }

	d.sl.Slog.Println("Supervisor: block here")
	select {} // Block indefinitely

	// make sure all stop messages are sent.
	time.Sleep(time.Duration(params.Delay+params.JitterRange+3) * time.Millisecond)

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
		// add codes for more functionality
	case message.CPrefixQuery:
		d.handlePrefixQueryResp(content)
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.tcpLock.Lock()
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// tcp listen for Supervisor
func (d *Supervisor) OldTcpListen() {
	ipaddr, err := net.ResolveTCPAddr("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	ln, err := net.ListenTCP("tcp", ipaddr)
	d.tcpLn = ln
	if err != nil {
		log.Panic(err)
	}
	d.sl.Slog.Printf("Supervisor begins listening：%s\n", d.IPaddr)

	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			if d.listenStop {
				return
			}
			log.Panic(err)
		}
		b, err := io.ReadAll(conn)
		if err != nil {
			log.Panic(err)
		}
		d.handleMessage(b)
		conn.(*net.TCPConn).SetLinger(0)
		defer conn.Close()
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}

// 启动 JSON-RPC 服务
func (d *Supervisor) StartRPCServer() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(d)

	listener, err := net.Listen("tcp", ":12345") // 监听端口
	if err != nil {
		log.Fatalf("Failed to start RPC server: %v", err)
	}
	defer listener.Close()

	log.Println("RPC server is listening on port 12345")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go rpcServer.ServeCodec(jsonrpc.NewServerCodec(conn))
	}
}

// rpc demo
func (d *Supervisor) GetStatus(args *types.GetStatusArgs, reply *types.GetStatusReply) error {
	*reply = types.GetStatusReply{Status: "Supervisor is running"}
	return nil
}

func (d *Supervisor) PrefixQuery(args *types.PrefixQueryArgs, reply *types.PrefixQueryReply) error {
	d.sl.Slog.Printf("ywb sending prefix query to proxy node %s \n", args.Addr)
	query := message.PrefixQueryMessage{
		Status:       message.PREFIX_QUERY_FIRST,
		Type:         message.REQUEST,
		ProxyAddress: args.Addr,
		Prefix:       args.Prefix,
		Identifier:   args.Identifier,
	}
	itByte, err := json.Marshal(query)
	if err != nil {
		log.Panic(err)
	}
	send_msg := message.MergeMessage(message.CPrefixQuery, itByte)
	go networks.TcpDial(send_msg, query.ProxyAddress)
	// *reply = message.PrefixQueryMessage{Status: "Supervisor is running"}
	return nil
}

// func (d *Supervisor) IdentifierQuery(args *types.IdentifierQueryArgs, reply *types.IdentifierQueryReply) error {
// 	d.sl.Slog.Printf("ywb sending identifier query to proxy node %s \n", args.Addr)
// 	query := message.QueryMessage{
// 		Identifier: args.Identifier,
// 	}
// 	itByte, err := json.Marshal(query)
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	send_msg := message.MergeMessage(message.CIdentifierQuery, itByte)
// 	go networks.TcpDial(send_msg, args.Addr)
// 	// *reply = message.QueryMessage{Status: "Supervisor is running"}
// 	return nil
// }

func (d *Supervisor) handlePrefixQueryResp(content []byte) {
	resp := new(message.PrefixQueryMessage)
	err := json.Unmarshal(content, resp)
	if err != nil {
		log.Panic()
	}

	// 得到前缀查询结果
	d.sl.Slog.Printf("The prefix query result : %v\n", resp) // todo time metrics 想办法和请求对上
}
