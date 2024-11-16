package demis

import (
	"blockEmulator/de-mis/demis_log"
	"blockEmulator/message"
	"blockEmulator/params"
	"sync"
)

type DENode struct {
	prefix           string
	addr             string
	level            uint32
	parentMap       map[string]string
	childrenMap     map[string]string  // domain -> addr
	recvCh 		 	chan *message.ResolveMessage
	stopCh          chan struct{}

	// logger
	dl *demis_log.DemisLog
	wait sync.WaitGroup
}

func NewDENode(cfg *params.DENodeConfig, deCh chan *message.ResolveMessage) *DENode {

	return &DENode{
		prefix:          cfg.Prefix,
		addr:            cfg.Addr,
		level: 		  	 0,
		parentMap:       cfg.ParentMap,
		childrenMap:     cfg.ChildrenMap,
		recvCh: 		 deCh,
	}
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
        case msg := <-de.recvCh:
            de.dl.Dlog.Printf("Received message: %v", msg)
            // 在这里处理接收到的消息
        case <-de.stopCh:
			de.dl.Dlog.Printf("Stopping process goroutine")
            return
        }
    }
}
