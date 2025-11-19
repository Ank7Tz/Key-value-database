package raft

import (
	"errors"
	"fmt"
	"net"
	"sync"

	ch "key_value_store/internal/consistenthash"

	raftgrpctransport "github.com/Jille/raft-grpc-transport"
	raftLib "github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type Node struct {
	NodeId string
	Addr   string
	Port   int
}

type MultiRaft struct {
	localNode Node
	dataDir   string

	grpcServer *grpc.Server
	tm         *raftgrpctransport.Manager

	numShards      uint32
	raftGroups     map[uint64]*RaftGroup
	consistentHash ch.ConsistentHash
	rgMtx          sync.RWMutex

	clusterSize       int
	replicationFactor int
	nodeId            string
	isBootstrapped    bool
}

func NewMultiRaft(localNode Node, numShards uint32, clusterSize int, replicationFactor int, dataDir string) (*MultiRaft, error) {
	if clusterSize < 0 {
		return nil, fmt.Errorf("cluster size cannot be negative")
	}

	grpcServer := grpc.NewServer()

	tm := raftgrpctransport.New(raftLib.ServerAddress(fmt.Sprintf("%s:%d", localNode.Addr, localNode.Port)), []grpc.DialOption{
		grpc.WithInsecure(),
	})

	multiRaft := &MultiRaft{
		localNode:         localNode,
		numShards:         numShards,
		clusterSize:       clusterSize,
		replicationFactor: replicationFactor,
		dataDir:           dataDir,
		nodeId:            localNode.NodeId,
		grpcServer:        grpcServer,
		tm:                tm,
		raftGroups:        make(map[uint64]*RaftGroup),
		consistentHash:    ch.NewSimpleConsistentHash(int(numShards)),
		isBootstrapped:    false,
	}

	return multiRaft, nil
}

func (mr *MultiRaft) Init() error {
	go func() {
		addr := fmt.Sprintf("%s:%d", mr.localNode.Addr, mr.localNode.Port)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			panic(err)
		}

		if err := mr.grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	mr.isBootstrapped = true
	return nil
}

func (mr *MultiRaft) CreateRaftGroup(shardId uint32, nodes []Node) error {
	mr.rgMtx.Lock()
	defer mr.rgMtx.Unlock()

	if _, exists := mr.raftGroups[uint64(shardId)]; exists {
		return fmt.Errorf("raft group for %d already exists", shardId)
	}

	groupId := fmt.Sprintf("shard_%d", shardId)

	transport := mr.tm.Transport()

	rg, err := NewRaftGroup(groupId, uint64(shardId), mr.localNode, nodes, mr.dataDir, transport)
	if err != nil {
		return err
	}

	bootstrap := len(nodes) == 1
	if err := rg.Start(transport, bootstrap); err != nil {
		return err
	}

	mr.raftGroups[uint64(shardId)] = rg
	return nil
}

func (mr *MultiRaft) GetRaftGroup(key string) (*RaftGroup, error) {
	shardID := mr.consistentHash.GetShardIdForKey(key)

	mr.rgMtx.RLock()
	rg, exists := mr.raftGroups[uint64(shardID)]
	mr.rgMtx.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no raft group for shardId %d", shardID)
	}

	return rg, nil
}

func (mr *MultiRaft) GetRaftGroupByShardId(shardId uint32) (*RaftGroup, error) {
	mr.rgMtx.RLock()
	defer mr.rgMtx.RUnlock()

	rg, exists := mr.raftGroups[uint64(shardId)]
	if !exists {
		return nil, fmt.Errorf("no raft group for shardId %d", shardId)
	}

	return rg, nil
}

func (mr *MultiRaft) Get(key string) (string, error) {
	rg, err := mr.GetRaftGroup(key)
	if err != nil {
		return "", err
	}

	value, err := rg.KVStore.Read(key)
	if err != nil {
		return "", err
	}

	return string(value), nil
}

func (mr *MultiRaft) Put(key, value string) error {
	rg, err := mr.GetRaftGroup(key)
	if err != nil {
		return err
	}

	if !rg.isLeader() {
		return errors.New("not leader, redirect to: " + rg.FindGroupLeader())
	}

	// Create properly marshaled message
	msg := &RaftLogMessage{
		Key:   key,
		Value: []byte(value),
	}
	cmd := msg.Marshal()

	return rg.Apply(cmd, 10*1000000000)
}

func (mr *MultiRaft) ShutdownAll() error {
	mr.rgMtx.Lock()
	defer mr.rgMtx.Unlock()

	var errs []error

	for _, rg := range mr.raftGroups {
		if err := rg.shutdown(); err != nil {
			errs = append(errs, err)
		}
	}

	mr.grpcServer.GracefulStop()

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}

	return nil
}
