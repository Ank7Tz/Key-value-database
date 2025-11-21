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

// PersistentState holds the state that needs to be persisted
type PersistentState struct {
	CurrentTerm int64
	VotedFor    int64
	Log         []*pb.LogEntry
}

// Persister handles persistence of Raft state to disk
type Persister struct {
	mu        sync.Mutex
	dir       string
	stateFile string
}

// NewPersister creates a new persister
func NewPersister(nodeID int64, baseDir string) (*Persister, error) {
	dir := filepath.Join(baseDir, fmt.Sprintf("node-%d", nodeID))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	return &Persister{
		dir:       dir,
		stateFile: filepath.Join(dir, "state.dat"),
	}, nil
}

// SaveState saves the Raft state to disk
func (p *Persister) SaveState(state *PersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Encode state to bytes
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("failed to encode state: %v", err)
	}

	// Write to temporary file first, then rename for atomicity
	tempFile := p.stateFile + ".tmp"
	if err := os.WriteFile(tempFile, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %v", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, p.stateFile); err != nil {
		os.Remove(tempFile) // Clean up temp file
		return fmt.Errorf("failed to rename temp file: %v", err)
	}

	return nil
}

// LoadState loads the Raft state from disk
func (p *Persister) LoadState() (*PersistentState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			// No saved state, return empty state
			return &PersistentState{
				CurrentTerm: 0,
				VotedFor:    -1,
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

// StateSize returns the size of the saved state in bytes
func (p *Persister) StateSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, err := os.Stat(p.stateFile)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Clear removes all persisted state
func (p *Persister) Clear() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return os.RemoveAll(p.dir)
}
