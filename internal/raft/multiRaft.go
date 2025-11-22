package raft

import (
	"context"
	"fmt"
	"key_value_store/internal/consistenthash"
	datastore "key_value_store/internal/kvstore"
	pb "key_value_store/internal/raft/RPC"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type RaftServer struct {
	pb.UnimplementedRaftServer
	RaftGroups map[string]*RaftNode
}

func (s *RaftServer) SendAppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	reply := &pb.AppendEntriesReply{}
	raftNode, exists := s.RaftGroups[req.ShardId]
	if !exists {
		return &pb.AppendEntriesReply{
			Term:    0,
			Success: false,
			ShardId: req.ShardId,
		}, nil
	}
	err := raftNode.AppendEntriesHandler(req, reply)
	return reply, err
}

func (s *RaftServer) SendRequestVote(ctx context.Context, req *pb.RequestVote) (*pb.RequestVoteReply, error) {
	reply := &pb.RequestVoteReply{}
	raftNode, exists := s.RaftGroups[req.ShardId]
	if !exists {
		return &pb.RequestVoteReply{
			Term:        0,
			VoteGranted: false,
		}, nil
	}
	err := raftNode.RequestVoteRPCHandler(req, reply)
	return reply, err
}

func (s *RaftServer) ForwardWrite(ctx context.Context, req *pb.ForwardWriteRequest) (*pb.ForwardWriteReply, error) {
	raftNode, exists := s.RaftGroups[req.ShardId]
	if !exists {
		return &pb.ForwardWriteReply{
			Success: false,
			Error:   "shard not found on this node",
		}, nil
	}

	logIdx, pchannel, success := raftNode.ProposeCommand(&pb.Command{
		Op:    "write",
		Key:   req.Key,
		Value: req.Value,
	})

	if !success {
		return &pb.ForwardWriteReply{
			Success:  false,
			Error:    "not the leader",
			LeaderId: raftNode.getLeader(),
		}, nil
	}

	select {
	case <-pchannel:
		return &pb.ForwardWriteReply{
			Success: true,
		}, nil
	case <-time.After(5 * time.Second):
		return &pb.ForwardWriteReply{
			Success: false,
			Error:   fmt.Sprintf("timeout waiting for commit at index %d", logIdx),
		}, nil
	case <-ctx.Done():
		return &pb.ForwardWriteReply{
			Success: false,
			Error:   "request cancelled",
		}, nil
	}
}

func (s *RaftServer) ForwardRead(ctx context.Context, req *pb.ForwardReadRequest) (*pb.ForwardReadReply, error) {
	raftNode, exists := s.RaftGroups[req.ShardId]
	if !exists {
		return &pb.ForwardReadReply{
			Success: false,
			Error:   "shard not found on this node",
		}, nil
	}

	if !raftNode.IsLeader() {
		return &pb.ForwardReadReply{
			Success:  false,
			Error:    "not the leader",
			LeaderId: raftNode.getLeader(),
		}, nil
	}

	value, err := raftNode.KVStore.Read(req.Key)
	if err != nil {
		return &pb.ForwardReadReply{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.ForwardReadReply{
		Success: true,
		Value:   value,
	}, nil
}

type RaftClient struct {
	Client pb.RaftClient
}

func (c *RaftClient) SendVoteRequest(groupId string, req *pb.RequestVote, reply *pb.RequestVoteReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Client.SendRequestVote(ctx, req)
	if err != nil {
		return false
	}
	reply.ShardId = resp.ShardId
	reply.Term = resp.Term
	reply.VoteGranted = resp.VoteGranted
	return true
}

func (c *RaftClient) SendAppendEntriesRequest(groupId string, req *pb.AppendEntriesRequest, reply *pb.AppendEntriesReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.Client.SendAppendEntries(ctx, req)
	if err != nil {
		return false
	}
	reply.ShardId = resp.ShardId
	reply.Success = resp.Success
	reply.Term = resp.Term
	return true
}

func (c *RaftClient) ForwardWrite(shardId, key string, value []byte) (*pb.ForwardWriteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	req := &pb.ForwardWriteRequest{
		ShardId: shardId,
		Key:     key,
		Value:   value,
	}

	return c.Client.ForwardWrite(ctx, req)
}

func (c *RaftClient) ForwardRead(shardId, key string) (*pb.ForwardReadReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.ForwardReadRequest{
		ShardId: shardId,
		Key:     key,
	}

	return c.Client.ForwardRead(ctx, req)
}

type MultiRaft struct {
	nodeId      string
	peers       []string
	raftGroups  map[string]*RaftNode
	raftClients map[string]*RaftClient
	raftServer  RaftServer
	ch          consistenthash.ConsistentHash
	KVStore     datastore.Store
}

func NewMultiRaft(peers map[string]string, shardCount int, nodeId string) *MultiRaft {
	peerList := []string{}

	for peerId := range peers {
		peerList = append(peerList, peerId)
	}

	nodeCount := len(peerList)

	ch := consistenthash.NewConsistentHash(shardCount, nodeCount)
	store, err := datastore.NewLevelDBStore(fmt.Sprintf("./data/%s/db", nodeId))
	if err != nil {
		panic(err)
	}

	mr := &MultiRaft{
		nodeId:      nodeId,
		peers:       peerList,
		ch:          ch,
		raftGroups:  map[string]*RaftNode{},
		raftClients: map[string]*RaftClient{},
		KVStore:     store,
	}

	allClients := map[string]RaftClientLayer{}

	allClientConns := map[string]*grpc.ClientConn{}

	for i := range nodeCount {
		peerId := fmt.Sprintf("node_%d", i+1)

		if peerId == nodeId {
			continue
		}

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             3 * time.Second,
				PermitWithoutStream: true,
			}),
		}

		conn, err := grpc.NewClient(peers[peerId], opts...)
		if err != nil {
			panic(err)
		}
		allClientConns[peerId] = conn
	}

	for i := range nodeCount {
		peerId := fmt.Sprintf("node_%d", i+1)

		if peerId == nodeId {
			continue
		}

		client := &RaftClient{
			Client: pb.NewRaftClient(allClientConns[peerId]),
		}
		allClients[peerId] = client
		mr.raftClients[peerId] = client
	}

	for i := range shardCount {
		shardId := fmt.Sprintf("shard_%d", i+1)
		nodesToReplicate := ch.GetPhysicalNodesForShardId(shardId)

		for _, member := range nodesToReplicate {
			if nodeId == member {
				persisterForRaft, err := NewPersister(fmt.Sprintf("./data/%s/log/%s", nodeId, shardId))

				if err != nil {
					panic(err)
				}

				clients := map[string]RaftClientLayer{}
				peers := []string{}
				for _, peerId := range nodesToReplicate {
					if peerId == nodeId {
						continue
					}
					peers = append(peers, peerId)
					clients[peerId] = allClients[peerId]
				}

				mr.raftGroups[shardId] = NewRaftNode(nodeId, shardId, peers, clients, store, persisterForRaft)
				break
			}
		}
	}

	for _, raftNode := range mr.raftGroups {
		raftNode.Start()
	}

	mr.raftServer = RaftServer{
		RaftGroups: mr.raftGroups,
	}

	return mr
}

func (mr *MultiRaft) Write(key string, value []byte) error {
	shardId := mr.ch.GetShardIdForKey(key)

	nodesHoldingData := mr.ch.GetPhysicalNodesForShardId(shardId)
	contains := false

	for _, node := range nodesHoldingData {
		if node == mr.nodeId {
			contains = true
			break
		}
	}

	if contains {
		raftNode := mr.raftGroups[shardId]
		logIdx, pchannel, success := raftNode.ProposeCommand(&pb.Command{
			Op:    "write",
			Key:   key,
			Value: value,
		})

		if !success {
			leaderId := raftNode.getLeader()
			if leaderId == "" {
				return fmt.Errorf("no leader available for shard %s", shardId)
			}

			client, exists := mr.raftClients[leaderId]
			if !exists {
				return fmt.Errorf("no client connection to leader %s", leaderId)
			}

			reply, err := client.ForwardWrite(shardId, key, value)
			if err != nil {
				return fmt.Errorf("failed to forward write to leader: %w", err)
			}

			if !reply.Success {
				return fmt.Errorf("write failed on leader: %s", reply.Error)
			}

			return nil
		}

		select {
		case <-pchannel:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout waiting for commit at index %d", logIdx)
		}

	} else {
		for _, nodeId := range nodesHoldingData {
			client, exists := mr.raftClients[nodeId]
			if !exists {
				continue
			}

			reply, err := client.ForwardWrite(shardId, key, value)
			if err != nil {
				continue
			}

			if reply.Success {
				return nil
			}

			if reply.LeaderId != "" && reply.LeaderId != nodeId {
				leaderClient, exists := mr.raftClients[reply.LeaderId]
				if exists {
					reply, err = leaderClient.ForwardWrite(shardId, key, value)
					if err == nil && reply.Success {
						return nil
					}
				}
			}
		}

		return fmt.Errorf("failed to write to any node holding shard %s", shardId)
	}
}

func (mr *MultiRaft) Read(key string) ([]byte, error) {
	shardId := mr.ch.GetShardIdForKey(key)

	nodesHoldingData := mr.ch.GetPhysicalNodesForShardId(shardId)
	contains := false

	for _, node := range nodesHoldingData {
		if node == mr.nodeId {
			contains = true
			break
		}
	}

	if contains {
		raftNode := mr.raftGroups[shardId]

		if !raftNode.IsLeader() {
			leaderId := raftNode.getLeader()
			if leaderId == "" {
				return nil, fmt.Errorf("no leader available for shard %s", shardId)
			}

			client, exists := mr.raftClients[leaderId]
			if !exists {
				return nil, fmt.Errorf("no client connection to leader %s", leaderId)
			}

			reply, err := client.ForwardRead(shardId, key)
			if err != nil {
				return nil, fmt.Errorf("failed to forward read to leader: %w", err)
			}

			if !reply.Success {
				return nil, fmt.Errorf("read failed on leader: %s", reply.Error)
			}

			return reply.Value, nil
		}

		value, err := mr.KVStore.Read(key)
		if err != nil {
			return nil, fmt.Errorf("failed to read key %s: %w", key, err)
		}
		return value, nil
	}

	for _, nodeId := range nodesHoldingData {
		client, exists := mr.raftClients[nodeId]
		if !exists {
			continue
		}

		reply, err := client.ForwardRead(shardId, key)
		if err != nil {
			continue
		}

		if reply.Success {
			return reply.Value, nil
		}

		if reply.LeaderId != "" && reply.LeaderId != nodeId {
			leaderClient, exists := mr.raftClients[reply.LeaderId]
			if exists {
				reply, err = leaderClient.ForwardRead(shardId, key)
				if err == nil && reply.Success {
					return reply.Value, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("failed to read from any node holding shard %s", shardId)
}

func (mr *MultiRaft) StartGrpcServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)

	pb.RegisterRaftServer(grpcServer, &mr.raftServer)

	log.Printf("Starting gRPC server on %s for node %s", address, mr.nodeId)
	return grpcServer.Serve(lis)
}
