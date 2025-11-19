package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	datastore "key_value_store/internal/kvstore"
	"os"
	"sync"
	"time"

	raftLib "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type RaftLogMessage struct {
	Key   string
	Value []byte
}

func (rkvMsg *RaftLogMessage) Marshal() []byte {
	// 4 bytes for key length
	// key bytes
	// 4 bytes for value length
	// value bytes (max data size less than 4GB)
	keyBytes := []byte(rkvMsg.Key)
	keyLen := len(keyBytes)
	valueLen := len(rkvMsg.Value)

	totalSize := 4 + keyLen + 4 + valueLen

	data := make([]byte, totalSize)

	offset := 0

	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(keyLen))
	offset += 4

	copy(data[offset:offset+keyLen], keyBytes)
	offset += keyLen

	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(valueLen))
	offset += 4

	copy(data[offset:offset+valueLen], rkvMsg.Value)

	return data
}

func (rkvMsg *RaftLogMessage) UnMarshal(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("data is too small")
	}

	offset := 0

	keyLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	keyBytes := data[offset : offset+int(keyLen)]
	offset += int(keyLen)

	rkvMsg.Key = string(keyBytes)

	valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	rkvMsg.Value = make([]byte, valueLen)

	copy(rkvMsg.Value, data[offset:offset+int(valueLen)])

	return nil
}

type FSM struct {
	mtx     sync.RWMutex
	KvStore datastore.Store
	shardId uint64
}

func (f *FSM) Apply(log *raftLib.Log) any {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	var record RaftLogMessage

	if err := record.UnMarshal(log.Data); err != nil {
		return err
	}

	if err := f.KvStore.Write(record.Key, record.Value); err != nil {
		return err
	}

	return nil
}

func (f *FSM) Snapshot() (raftLib.FSMSnapshot, error) {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	// TODO - Take a snapshot of leveldb and return

	return &FSMSnapshot{kvStore: f.KvStore, shardId: f.shardId}, nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	defer snapshot.Close()

	// Read all snapshot data
	// 4 bytes: key length
	// key bytes
	// 4 bytes: value length
	// value bytes

	data := make([]byte, 4096)
	buffer := make([]byte, 0)

	for {
		n, err := snapshot.Read(data)
		if n > 0 {
			buffer = append(buffer, data[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read snapshot: %w", err)
		}
	}

	offset := 0
	for offset < len(buffer) {
		if offset+4 > len(buffer) {
			break
		}

		keyLen := binary.BigEndian.Uint32(buffer[offset : offset+4])
		offset += 4

		if offset+int(keyLen) > len(buffer) {
			return fmt.Errorf("invalid snapshot: incomplete key")
		}

		key := string(buffer[offset : offset+int(keyLen)])
		offset += int(keyLen)

		if offset+4 > len(buffer) {
			return fmt.Errorf("invalid snapshot: incomplete value length")
		}

		valueLen := binary.BigEndian.Uint32(buffer[offset : offset+4])
		offset += 4

		if offset+int(valueLen) > len(buffer) {
			return fmt.Errorf("invalid snapshot: incomplete value")
		}

		value := make([]byte, valueLen)
		copy(value, buffer[offset:offset+int(valueLen)])
		offset += int(valueLen)

		if err := f.KvStore.Write(key, value); err != nil {
			return fmt.Errorf("failed to restore key %s: %w", key, err)
		}
	}

	return nil
}

type FSMSnapshot struct {
	kvStore datastore.Store
	shardId uint64
}

func (s *FSMSnapshot) Persist(sink raftLib.SnapshotSink) error {
	// TODO: Implement for levelDB
	// iterate through the snapshot db, and marshal all the key-value records
	// write to sink

	return sink.Close()
}

func (s *FSMSnapshot) Release() {
	// TODO: Release any resources held by the snapshot
}

type RaftGroup struct {
	groupId string
	shardId uint64
	members map[string]Node
	mtx     sync.RWMutex

	raft          *raftLib.Raft
	fsm           raftLib.FSM
	config        *raftLib.Config
	logStore      raftLib.LogStore
	stableStore   raftLib.StableStore
	snapshotStore raftLib.SnapshotStore

	KVStore datastore.Store

	localAddr raftLib.ServerAddress
}

func NewRaftGroup(groupId string, shardId uint64, localNode Node, nodes []Node, dataDir string, transport raftLib.Transport) (*RaftGroup, error) {
	rGroup := &RaftGroup{
		groupId:   groupId,
		shardId:   shardId,
		members:   make(map[string]Node),
		localAddr: transport.LocalAddr(),
	}

	for i := range nodes {
		rGroup.members[nodes[i].NodeId] = nodes[i]
	}

	rGroup.KVStore = datastore.NewInMemDataStore()

	rGroup.fsm = &FSM{KvStore: rGroup.KVStore, shardId: shardId}

	config := raftLib.DefaultConfig()
	config.LocalID = raftLib.ServerID(localNode.NodeId)
	rGroup.config = config

	// Create shard directory if it doesn't exist
	shardDir := fmt.Sprintf("%s/shard_%d", dataDir, shardId)
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shard directory: %w", err)
	}

	// Create log store
	logPath := fmt.Sprintf("%s/raft_log.db", shardDir)
	logStore, err := raftboltdb.NewBoltStore(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}
	rGroup.logStore = logStore

	stablePath := fmt.Sprintf("%s/raft_stable.db", shardDir)
	stableStore, err := raftboltdb.NewBoltStore(stablePath)
	if err != nil {
		logStore.Close()
		return nil, err
	}

	rGroup.stableStore = stableStore

	snapshotPath := fmt.Sprintf("%s/snapshots", shardDir)
	snapshotStore, err := raftLib.NewFileSnapshotStore(snapshotPath, 3, nil)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, err
	}

	rGroup.snapshotStore = snapshotStore

	return rGroup, nil
}

func (rg *RaftGroup) Start(transport raftLib.Transport, bootstrap bool) error {
	rg.mtx.Lock()
	defer rg.mtx.Unlock()

	r, err := raftLib.NewRaft(rg.config, rg.fsm, rg.logStore, rg.stableStore, rg.snapshotStore, transport)
	if err != nil {
		return err
	}

	rg.raft = r

	if bootstrap {
		// Check if already bootstrapped by looking at the configuration
		configFuture := rg.raft.GetConfiguration()
		if err := configFuture.Error(); err != nil {
			return fmt.Errorf("failed to get configuration: %w", err)
		}

		// Only bootstrap if cluster has no servers configured
		if len(configFuture.Configuration().Servers) == 0 {
			configuration := raftLib.Configuration{
				Servers: []raftLib.Server{{
					ID:      rg.config.LocalID,
					Address: rg.localAddr,
				}},
			}
			future := rg.raft.BootstrapCluster(configuration)
			if err := future.Error(); err != nil {
				return fmt.Errorf("failed to bootstrap cluster: %w", err)
			}
		}
		// If already bootstrapped, just continue
	}
	return nil
}

func (rg *RaftGroup) AddMember(nodeId string, address string) error {
	future := rg.raft.AddVoter(raftLib.ServerID(nodeId), raftLib.ServerAddress(address), 0, 0)
	return future.Error()
}

func (rg *RaftGroup) RemoveMember(nodeId string) error {
	future := rg.raft.RemoveServer(raftLib.ServerID(nodeId), 0, 0)
	return future.Error()
}

func (rg *RaftGroup) isLeader() bool {
	return rg.raft.State() == raftLib.Leader
}

func (rg *RaftGroup) FindGroupLeader() string {
	rg.mtx.RLock()
	defer rg.mtx.Unlock()

	_, leaderId := rg.raft.LeaderWithID()
	return string(leaderId)
}

func (rg *RaftGroup) Apply(cmd []byte, timeout time.Duration) error {
	future := rg.raft.Apply(cmd, timeout)
	return future.Error()
}

func (rg *RaftGroup) shutdown() error {
	rg.mtx.Lock()
	defer rg.mtx.Unlock()

	var errs []error

	if rg.raft != nil {
		if err := rg.raft.Shutdown().Error(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}

	return nil
}
