package datastore

import (
	"fmt"

	levelDb "github.com/syndtr/goleveldb/leveldb"
)

type Store interface {
	Read(key string) ([]byte, error)
	Write(key string, value []byte) error
	Delete(key string) error
	GetAll() map[string][]byte
	Close() error
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

func (d *InMemDataStore) Close() error {
	// nothing
	return nil
}

func (d *InMemDataStore) GetAll() map[string][]byte {
	return nil
}

type LevelDBStore struct {
	db   *levelDb.DB
	path string
}

func NewLevelDBStore(path string) (*LevelDBStore, error) {
	db, err := levelDb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb at %s: %w", path, err)
	}

	return &LevelDBStore{
		db:   db,
		path: path,
	}, nil
}

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

func (l *LevelDBStore) Write(key string, value []byte) error {
	err := l.db.Put([]byte(key), value, nil)
	if err != nil {
		return fmt.Errorf("failed to write key %s: %w", key, err)
	}
	return nil
}

func (l *LevelDBStore) Delete(key string) error {
	err := l.db.Delete([]byte(key), nil)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

func (l *LevelDBStore) Close() error {
	if l.db != nil {
		return l.db.Close()
	}
	return nil
}

func (l *LevelDBStore) GetAll() map[string][]byte {
	response := map[string][]byte{}
	iter := l.db.NewIterator(nil, nil)

	for iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())

		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())

		response[string(key)] = value
	}

	return response
}
