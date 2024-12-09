package main

import (
	drpc "blockEmulator/rpc"
	"fmt"
	"log"
	"net/rpc/jsonrpc"
	"net/rpc"
)

func main() {
	client, err := jsonrpc.Dial("tcp", "127.0.0.1:12345")
	if err != nil {
		log.Fatalf("Failed to connect to RPC server: %v", err)
	}

	sendPrefixQuery(client, "A.0.a", "A.0.a/type1:1051", "127.0.0.1:32517")
	// sendUnionQuery(client, "A.0.a/type0:00023c20c5c89eaaaebfe4901bbfb5128d55518314f67557ea24ddb6f3e870bc", "127.0.0.1:32417")
}

func sendPrefixQuery(client *rpc.Client, prefix, identifier, addr string) {
	req := drpc.PrefixQueryArgs{
		Prefix:     prefix,
		Identifier: identifier,
		Addr:       addr,
	}
	var reply drpc.GetStatusReply
	err := client.Call("Supervisor.PrefixQuery", &req, &reply)
	if err != nil {
		log.Fatalf("Failed to call RPC method: %v", err)
	}

	fmt.Printf("RPC response: %v\n", reply)
}

func sendUnionQuery(client *rpc.Client, identifier, addr string) {
	req := drpc.UnionQueryArgs{
		Identifier: identifier,
		Addr:       addr,
	}
	var reply drpc.UnionQueryReply
	err := client.Call("Supervisor.UnionQuery", &req, &reply)
	if err != nil {
		log.Fatalf("Failed to call RPC method: %v", err)
	}

	fmt.Printf("RPC response: %v\n", reply)
}

