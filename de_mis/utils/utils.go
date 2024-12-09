package utils

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"encoding/json"
	"log"
)

func WriteMsg(msg interface{}, msgType message.MessageType, addr string) {
	itByte, err := json.Marshal(msg)
	if err != nil {
		log.Panic(err)
	}
	send_msg := message.MergeMessage(msgType, itByte)
	go networks.TcpDial(send_msg, addr)
}