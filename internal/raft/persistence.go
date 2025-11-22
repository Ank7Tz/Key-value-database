package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "key_value_store/internal/raft/RPC"
)

type PersistentState struct {
	CurrentTerm int64
	VotedFor    string
	Log         []*pb.LogEntry
}

type Persister struct {
	mu        sync.Mutex
	dir       string
	stateFile string
}

func NewPersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &Persister{
		dir:       dir,
		stateFile: filepath.Join(dir, "state.dat"),
	}, nil
}

func (p *Persister) SaveState(state *PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("failed to encode state: %v", err)
	}

	tempFile := p.stateFile + ".tmp"
	if err := os.WriteFile(tempFile, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %v", err)
	}

	if err := os.Rename(tempFile, p.stateFile); err != nil {
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename temp file: %v", err)
	}

	return nil
}

func (p *Persister) LoadState() (*PersistentState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return &PersistentState{
				CurrentTerm: 0,
				VotedFor:    "",
				Log:         []*pb.LogEntry{{Term: 0}},
			}, nil
		}
		return nil, fmt.Errorf("failed to read state file: %v", err)
	}

	var state PersistentState
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("failed to decode state: %v", err)
	}

	return &state, nil
}

func (p *Persister) StateSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, err := os.Stat(p.stateFile)
	if err != nil {
		return 0
	}
	return info.Size()
}

func (p *Persister) Clear() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return os.RemoveAll(p.dir)
}
