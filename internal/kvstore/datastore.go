package datastore

import (
	"fmt"

	levelDb "github.com/syndtr/goleveldb/leveldb"
)

type Store interface {
	Read(key string) ([]byte, error)
	Write(key string, value []byte) error
	Delete(key string) error
}

type InMemDataStore struct {
	store map[string][]byte
}

func NewInMemDataStore() *InMemDataStore {
	return &InMemDataStore{
		store: make(map[string][]byte),
	}
}

func (d *InMemDataStore) Read(key string) ([]byte, error) {
	if value, exists := d.store[key]; exists {
		return value, nil
	}

	return nil, fmt.Errorf("no data found")
}

func (d *InMemDataStore) Write(key string, value []byte) error {
	d.store[key] = value
	return nil
}

func (d *InMemDataStore) Delete(key string) error {
	delete(d.store, key)
	return nil
}

// GetAll returns a copy of all key-value pairs in the store
func (d *InMemDataStore) GetAll() map[string][]byte {
	data := make(map[string][]byte, len(d.store))
	for k, v := range d.store {
		// Create a copy of the value to avoid sharing references
		valueCopy := make([]byte, len(v))
		copy(valueCopy, v)
		data[k] = valueCopy
	}
	return data
}

// Clear removes all entries from the store
func (d *InMemDataStore) Clear() {
	d.store = make(map[string][]byte)
}

// TODO: Implement LevelDBStore
type LevelDBStore struct {
	db *levelDb.DB
}

// TODO: Implement Store, raftLib.LogStore, raftLib.StableStore for LevelDBStore
