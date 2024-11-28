package cache

import (
	// fifo "github.com/scalalang2/golang-fifo"
	// "time"

	// "fmt"

	"blockEmulator/core"

	sieve "github.com/scalalang2/golang-fifo/sieve"
	types "github.com/scalalang2/golang-fifo/types"
)

type Sieve struct {
	v types.Cache[string, *core.IdentifierRecord, string] // identifier, record, addr
}

func NewSieve(size int) Cache {
	evictCallback := func(key string, value *core.IdentifierRecord, reason types.EvictReason, cache types.Cache[string, *core.IdentifierRecord, string]) {
		// fmt.Printf("Key: %s, Value: %v, Reason: %d\n", key, value, reason)
		cache.(*sieve.Sieve[string, *core.IdentifierRecord, string]).AddToGhost(key, value)
	}
	sieve := sieve.New[string, *core.IdentifierRecord, string](size, 0)
	sieve.SetOnEvicted(evictCallback)
	return &Sieve{sieve}
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
}

func (s *Sieve) Close() {

}
