package cache

import "blockEmulator/core"

type Cache interface {
	Name() string
	Get(key string) (any, bool)
	Set(key string, value *core.IdentifierRecord)
	Close()
}
