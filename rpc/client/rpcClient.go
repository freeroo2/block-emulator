package main

import (
	"blockEmulator/rpc"
	"fmt"
	"log"
	"net/rpc/jsonrpc"

)

func main() {
    client, err := jsonrpc.Dial("tcp", "127.0.0.1:12345")
    if err != nil {
        log.Fatalf("Failed to connect to RPC server: %v", err)
    }

    // var reply types.GetStatusReply
    // err = client.Call("Supervisor.GetStatus", &types.GetStatusArgs{}, &reply)
    // if err != nil {
    //     log.Fatalf("Failed to call RPC method: %v", err)
    // }
	req := rpc.PrefixQueryArgs{
		Prefix: "A.0.a",
		Identifier: "A.0.a/type0:1063",
		Addr: "127.0.0.1:32517", // B.0
	}
	var reply rpc.GetStatusReply
    err = client.Call("Supervisor.PrefixQuery", &req, &reply)
    if err != nil {
        log.Fatalf("Failed to call RPC method: %v", err)
    }

    fmt.Printf("RPC response: %v\n", reply)
}