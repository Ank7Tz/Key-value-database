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
	db   *levelDb.DB
	path string
}

// NewLevelDBStore creates a new LevelDB store at the given path
// path: directory where LevelDB files will be stored (e.g., "./data/node-1")
func NewLevelDBStore(path string) (*LevelDBStore, error) {
	// Open the database with default options
	// LevelDB will create the directory if it doesn't exist
	db, err := levelDb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb at %s: %w", path, err)
	}

	return &LevelDBStore{
		db:   db,
		path: path,
	}, nil
}

// Read retrieves a value by key
func (l *LevelDBStore) Read(key string) ([]byte, error) {
	value, err := l.db.Get([]byte(key), nil)
	if err != nil {
		if err == levelDb.ErrNotFound {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, fmt.Errorf("failed to read key %s: %w", key, err)
	}
	return value, nil
}

// Write stores a key-value pair
func (l *LevelDBStore) Write(key string, value []byte) error {
	err := l.db.Put([]byte(key), value, nil)
	if err != nil {
		return fmt.Errorf("failed to write key %s: %w", key, err)
	}
	return nil
}

// Delete removes a key-value pair
func (l *LevelDBStore) Delete(key string) error {
	err := l.db.Delete([]byte(key), nil)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// Close closes the database connection
func (l *LevelDBStore) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}

// TODO: Implement Store, raftLib.LogStore, raftLib.StableStore for LevelDBStore
