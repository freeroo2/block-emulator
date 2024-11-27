package rpc

// 定义参数结构体
type GetStatusArgs struct {
	// 可以根据需要添加字段
}

type GetStatusReply struct {
	Status string
}

type PrefixQueryArgs struct {
	Prefix     string
	Identifier string
	Addr       string
}

type PrefixQueryReply struct {
	Prefix string
	Addr   string
}
