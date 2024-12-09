package cache

import (
	// fifo "github.com/scalalang2/golang-fifo"
	// "time"

	// "fmt"

	"blockEmulator/core"
	"blockEmulator/de_mis/demis_log"

	sieve "github.com/scalalang2/golang-fifo/sieve"
	types "github.com/scalalang2/golang-fifo/types"
)

type Sieve struct {
	v types.Cache[string, *core.IdentifierRecord, string] // identifier, record, addr
	// logger
	sl *demis_log.CacheLog
}

func NewSieve(size int, shardID, nodeID uint64) Cache {
	sl := demis_log.NewCacheLog(shardID, nodeID)
	sl.Clog.Printf("Creating sieve cache, size=%d\n", size)
	evictCallback := func(key string, value *core.IdentifierRecord, reason types.EvictReason, cache types.Cache[string, *core.IdentifierRecord, string]) {
		// fmt.Printf("Key: %s, Value: %v, Reason: %d\n", key, value, reason)
		sl.Clog.Printf("Adding to ghost: key=%v, value=%v\n", key, value.Addr)
		cache.(*sieve.Sieve[string, *core.IdentifierRecord, string]).Ghost.Add(key, value.Addr)
	}
	sieve := sieve.New[string, *core.IdentifierRecord, string](size, 0)
	sieve.SetOnEvicted(evictCallback)
	return &Sieve{sieve, sl}
}

func (s *Sieve) Name() string {
	return "sieve"
}

func (s *Sieve) Get(key string) (any, bool) {
	value, ok := s.v.Get(key)
	return value, ok
}

func (s *Sieve) Set(key string, value *core.IdentifierRecord) {
	s.v.Set(key, value)
	s.sl.Clog.Printf("Set adding to sieve: key=%v, value=%v\n", key, value.Addr)
}

func (s *Sieve) Close() {

}
