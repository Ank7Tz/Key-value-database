package raft

import (
	"context"
	"fmt"
	"key_value_store/internal/consistenthash"
	datastore "key_value_store/internal/kvstore"
	pb "key_value_store/internal/raft/RPC"
	"log"
	"net"
	"strings"
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

	if !raftNode.IsLeader() {
		return &pb.ForwardWriteReply{
			Success:  false,
			Error:    "not the leader",
			LeaderId: raftNode.GetLeader(),
		}, nil
	}

	logIndex, doneChan, success := raftNode.ProposeCommand(&pb.Command{
		Op:    "write",
		Key:   req.Key,
		Value: req.Value,
	})

	if !success {
		return &pb.ForwardWriteReply{
			Success:  false,
			Error:    "leadership lost",
			LeaderId: raftNode.GetLeader(),
		}, nil
	}

	select {
	case <-doneChan:
		return &pb.ForwardWriteReply{
			Success: true,
		}, nil
	case <-time.After(5 * time.Second):
		return &pb.ForwardWriteReply{
			Success: false,
			Error:   fmt.Sprintf("timeout waiting for commit at index %d", logIndex),
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
			LeaderId: raftNode.GetLeader(),
		}, nil
	}

	if req.StrongConsistency {
		logIdx, pchannel, success := raftNode.ProposeCommand(&pb.Command{
			Op: "noops",
		})
		if success {
			select {
			case <-pchannel:
				value, err := raftNode.KVStore.Read(req.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to read key %s: %w", req.Key, err)
				}
				return &pb.ForwardReadReply{
					Success: true,
					Value:   value,
				}, nil
			case <-time.After(5 * time.Second):
				return nil, fmt.Errorf("timeout waiting for strong consistent read (logindex: %d)", logIdx)
			}
		}
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

func (s *RaftServer) ForwardDelete(ctx context.Context, req *pb.ForwardDeleteRequest) (*pb.ForwardDeleteReply, error) {
	raftNode, exists := s.RaftGroups[req.ShardId]
	if !exists {
		return &pb.ForwardDeleteReply{
			Success: false,
			Error:   "shard not found on this node",
		}, nil
	}

	if !raftNode.IsLeader() {
		return &pb.ForwardDeleteReply{
			Success:  false,
			Error:    "not the leader",
			LeaderId: raftNode.GetLeader(),
		}, nil
	}

	logIndex, doneChan, success := raftNode.ProposeCommand(&pb.Command{
		Op:  "delete",
		Key: req.Key,
	})

	if !success {
		return &pb.ForwardDeleteReply{
			Success:  false,
			Error:    "leadership lost",
			LeaderId: raftNode.GetLeader(),
		}, nil
	}

	select {
	case <-doneChan:
		return &pb.ForwardDeleteReply{
			Success: true,
		}, nil
	case <-time.After(5 * time.Second):
		return &pb.ForwardDeleteReply{
			Success: false,
			Error:   fmt.Sprintf("timeout waiting for commit at index %d", logIndex),
		}, nil
	}
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

func (c *RaftClient) ForwardRead(shardId, key string, sc bool) (*pb.ForwardReadReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.ForwardReadRequest{
		ShardId:           shardId,
		Key:               key,
		StrongConsistency: sc,
	}

	return c.Client.ForwardRead(ctx, req)
}

func (c *RaftClient) ForwardDelete(shardId, key string) (*pb.ForwardDeleteReply, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.ForwardDeleteRequest{
		ShardId: shardId,
		Key:     key,
	}

	return c.Client.ForwardDelete(ctx, req)
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
			leaderId := raftNode.GetLeader()
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

func (mr *MultiRaft) Read(key string, sc bool) ([]byte, error) {
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

		if sc {
			logIdx, pchannel, success := raftNode.ProposeCommand(&pb.Command{
				Op: "noops",
			})
			if success {
				select {
				case <-pchannel:
					value, err := mr.KVStore.Read(key)
					if err != nil {
						return nil, fmt.Errorf("failed to read key %s: %w", key, err)
					}
					return value, nil
				case <-time.After(5 * time.Second):
					return nil, fmt.Errorf("timeout waiting for strong consistent read (logindex: %d)", logIdx)
				}
			}
		} else {
			value, err := mr.KVStore.Read(key)
			if err != nil {
				return nil, fmt.Errorf("failed to read key %s: %w", key, err)
			}
			return value, nil
		}

		if !raftNode.IsLeader() {
			leaderId := raftNode.GetLeader()
			if leaderId == "" {
				return nil, fmt.Errorf("no leader available for shard %s", shardId)
			}

			client, exists := mr.raftClients[leaderId]
			if !exists {
				return nil, fmt.Errorf("no client connection to leader %s", leaderId)
			}

			reply, err := client.ForwardRead(shardId, key, sc)
			if err != nil {
				return nil, fmt.Errorf("failed to forward read to leader: %w", err)
			}

			if !reply.Success {
				return nil, fmt.Errorf("read failed on leader: %s", reply.Error)
			}

			return reply.Value, nil
		}

		// value, err := mr.KVStore.Read(key)
		// if err != nil {
		// 	return nil, fmt.Errorf("failed to read key %s: %w", key, err)
		// }
		// return value, nil
		// return nil, fmt.Errorf("failed to read key %s", key)
	}

	for _, nodeId := range nodesHoldingData {
		client, exists := mr.raftClients[nodeId]
		if !exists {
			continue
		}

		reply, err := client.ForwardRead(shardId, key, sc)
		if err != nil {
			continue
		}

		if reply.Success {
			return reply.Value, nil
		}

		if strings.Contains(reply.Error, "key not found") {
			return nil, fmt.Errorf("%s", reply.Error)
		}

		if reply.LeaderId != "" && reply.LeaderId != nodeId {
			leaderClient, exists := mr.raftClients[reply.LeaderId]
			if exists {
				reply, err = leaderClient.ForwardRead(shardId, key, sc)
				if err == nil && reply.Success {
					return reply.Value, nil
				} else {
					if strings.Contains(reply.Error, "key not found") {
						return nil, fmt.Errorf("%s", reply.Error)
					}
				}
			}
		}
	}

	return nil, fmt.Errorf("failed to read from any node holding shard %s", shardId)
}

func (mr *MultiRaft) Delete(key string) error {
	shardId := mr.ch.GetShardIdForKey(key)
	nodesHoldingData := mr.ch.GetPhysicalNodesForShardId(shardId)

	contains := false

	for _, node := range nodesHoldingData {
		if mr.nodeId == node {
			contains = true
			break
		}
	}

	if contains {
		raftNode := mr.raftGroups[shardId]

		if !raftNode.IsLeader() {
			leaderNodeId := raftNode.GetLeader()
			if leaderNodeId == "" {
				return fmt.Errorf("no leader found for group %s", shardId)
			}

			client, exists := mr.raftClients[leaderNodeId]
			if !exists {
				return fmt.Errorf("no client connection for leader (%s)", leaderNodeId)
			}

			reply, err := client.ForwardDelete(shardId, key)
			if err != nil {
				return fmt.Errorf("failed to forward delete to leader: %s", err)
			}

			if !reply.Success {
				return fmt.Errorf("delete failed on leader: %s", reply.Error)
			}

			return nil
		}

		logIndex, doneChan, success := raftNode.ProposeCommand(&pb.Command{
			Op:  "delete",
			Key: key,
		})

		if !success {
			return fmt.Errorf("leadership lost")
		}

		select {
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout waiting for commit at index %d", logIndex)
		case <-doneChan:
			return nil
		}
	} else {
		for _, nodeId := range nodesHoldingData {
			client, exists := mr.raftClients[nodeId]

			if !exists {
				continue
			}

			reply, err := client.ForwardDelete(shardId, key)

			if err != nil {
				continue
			}

			if reply.Success {
				return nil
			}

			if strings.Contains(reply.Error, "key not found") {
				return fmt.Errorf("%s", reply.Error)
			}

			if reply.LeaderId != "" && reply.LeaderId != mr.nodeId {
				leaderClient, exists := mr.raftClients[reply.LeaderId]
				if exists {
					reply, err := leaderClient.ForwardDelete(shardId, key)
					if err == nil && reply.Success {
						return nil
					} else {
						if strings.Contains(reply.Error, "key not found") {
							return fmt.Errorf("%s", reply.Error)
						}
					}
				}
			}
		}
	}

	return nil
}

type StatsResponse struct {
	ShardMapping map[string][]string
	ShardGroups  map[string]*ShardGroupContent `json:"shardGroups"`
}

type ShardGroupContent struct {
	Store    map[string]string `json:"store"`
	Replicas *[]string         `json:"replicas"`
	Leader   string            `json:"leader"`
}

func (mr *MultiRaft) Stats() *StatsResponse {
	content := mr.KVStore.GetAll()
	shardGroups := map[string]*ShardGroupContent{}

	for key, value := range content {
		shardId := mr.ch.GetShardIdForKey(key)
		sgc, exists := shardGroups[shardId]
		if !exists {
			replicas := mr.ch.GetPhysicalNodesForShardId(shardId)
			shardGroups[shardId] = &ShardGroupContent{
				Store:    make(map[string]string),
				Replicas: &replicas,
			}
			sgc = shardGroups[shardId]

			// sgc.Leader = mr.raftGroups[shardId].GetLeader()
			if mr.raftGroups[shardId] != nil {
				sgc.Leader = mr.raftGroups[shardId].GetLeader()
			} else {
				sgc.Leader = fmt.Sprintf("missing for shard - %s", shardId)
			}
		}

		sgc.Store[key] = string(value)
	}

	response := &StatsResponse{
		ShardMapping: mr.ch.GetMapping(),
		ShardGroups:  shardGroups,
	}

	return response
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
