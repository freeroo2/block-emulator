package demis_log

import (
	"blockEmulator/params"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type DemisLog struct {
	Dlog *log.Logger
}

func NewDemisLog(sid, nid uint64) *DemisLog {
	pfx := fmt.Sprintf("S%dN%d: ", sid, nid)
	writer1 := os.Stdout

	dirpath := params.LogWrite_path + "/S" + strconv.Itoa(int(sid))
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	writer2, err := os.OpenFile(dirpath+"/N"+strconv.Itoa(int(nid))+"-Demis.log", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Panic(err)
	}
	dl := log.New(io.MultiWriter(writer1, writer2), pfx, log.Lshortfile|log.Ldate|log.Ltime)
	fmt.Println()

	return &DemisLog{
		Dlog: dl,
	}
}

type CacheLog struct {
	Clog *log.Logger
}

func NewCacheLog(sid, nid uint64) *CacheLog {
	pfx := fmt.Sprintf("S%dN%d: ", sid, nid)
	writer1 := os.Stdout

	dirpath := params.LogWrite_path + "/S" + strconv.Itoa(int(sid))
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	writer2, err := os.OpenFile(dirpath+"/N"+strconv.Itoa(int(nid))+"-Cache.log", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Panic(err)
	}
	cl := log.New(io.MultiWriter(writer1, writer2), pfx, log.Lshortfile|log.Ldate|log.Ltime)
	fmt.Println()

	return &CacheLog{
		Clog: cl,
	}
}


