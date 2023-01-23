package kvraft

import "sync"



type KeyValueStore interface {
	PutAppend(isPut bool, key string, val string)
	Get(key string) string
	Snapshot() []byte
}

type HashKeyValueStore struct {
	mu sync.RWMutex
	m map[string]string
}

func NewHashKeyValueStore(state []byte) *HashKeyValueStore {
	return &HashKeyValueStore{
		m: make(map[string]string),
	}
}

func (this *HashKeyValueStore) PutAppend(isPut bool, key string, val string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if isPut {
		this.m[key] = val
	} else if val1, ok := this.m[key]; ok {
		this.m[key] = val1 + val
	} else {
		this.m[key] = val
	}
}

func (this *HashKeyValueStore) Get(key string) string {
	this.mu.RLock()
	defer this.mu.RUnlock()
	if val, ok := this.m[key]; ok { return val }
	return ""
}

func (this *HashKeyValueStore) Snapshot() []byte {
	this.mu.Lock()
	defer this.mu.Unlock()
	
	return nil
}