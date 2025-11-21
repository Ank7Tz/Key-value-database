package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	database "key_value_store/internal/kvstore"
	"key_value_store/internal/raft"
	pb "key_value_store/internal/raft/RPC"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server wraps the Raft node and provides RPC handlers
type Server struct {
	pb.UnimplementedRaftServer
	node *raft.RaftNode
}

// gRPC RPC Handlers
func (s *Server) SendAppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	reply := &pb.AppendEntriesReply{}
	err := s.node.AppendEntriesHandler(req, reply)
	return reply, err
}

func (s *Server) SendRequestVote(ctx context.Context, req *pb.RequestVote) (*pb.RequestVoteReply, error) {
	reply := &pb.RequestVoteReply{}
	err := s.node.RequestVoteRPCHandler(req, reply)
	return reply, err
}

// RaftClient implements RaftClientLayer for gRPC communication
type RaftClient struct {
	client pb.RaftClient
	conn   *grpc.ClientConn
}

func (c *RaftClient) SendVoteRequest(req *pb.RequestVote, reply *pb.RequestVoteReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.client.SendRequestVote(ctx, req)
	if err != nil {
		return false
	}
	*reply = *resp
	return true
}

func (c *RaftClient) SendAppendEntriesRequest(req *pb.AppendEntriesRequest, reply *pb.AppendEntriesReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := c.client.SendAppendEntries(ctx, req)
	if err != nil {
		return false
	}
	*reply = *resp
	return true
}

// HTTP API Handlers
type KVServer struct {
	node *raft.RaftNode
}

type WriteRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ReadRequest struct {
	Key string `json:"key"`
}

type Response struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Leader  int64  `json:"leader,omitempty"`
}

func (kv *KVServer) WriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, Response{Success: false, Error: "Invalid request"})
		return
	}

	// Check if this node is the leader
	if !kv.node.IsLeader() {
		respondJSON(w, http.StatusServiceUnavailable, Response{
			Success: false,
			Error:   "Not the leader",
		})
		return
	}

	// Propose the command to Raft
	cmd := &pb.Command{
		Op:    "write",
		Key:   req.Key,
		Value: []byte(req.Value),
	}

	index, commitCh, ok := kv.node.ProposeCommand(cmd)
	if !ok {
		respondJSON(w, http.StatusServiceUnavailable, Response{Success: false, Error: "Not the leader"})
		return
	}

	// Wait for commit with timeout
	select {
	case <-commitCh:
		log.Printf("Command committed at index %d", index)
		respondJSON(w, http.StatusOK, Response{Success: true})
	case <-time.After(5 * time.Second):
		respondJSON(w, http.StatusRequestTimeout, Response{Success: false, Error: "Timeout waiting for commit"})
	}
}

func (kv *KVServer) ReadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var key string
	if r.Method == http.MethodGet {
		key = r.URL.Query().Get("key")
	} else {
		var req ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondJSON(w, http.StatusBadRequest, Response{Success: false, Error: "Invalid request"})
			return
		}
		key = req.Key
	}

	if key == "" {
		respondJSON(w, http.StatusBadRequest, Response{Success: false, Error: "Key is required"})
		return
	}

	// Read from local store (linearizable reads would require checking leader status)
	value, err := kv.node.KVStore.Read(key)
	if err != nil {
		respondJSON(w, http.StatusNotFound, Response{Success: false, Error: "Key not found"})
		return
	}

	respondJSON(w, http.StatusOK, Response{Success: true, Value: string(value)})
}

func (kv *KVServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var key string
	if r.Method == http.MethodDelete {
		key = r.URL.Query().Get("key")
	} else {
		var req ReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondJSON(w, http.StatusBadRequest, Response{Success: false, Error: "Invalid request"})
			return
		}
		key = req.Key
	}

	if key == "" {
		respondJSON(w, http.StatusBadRequest, Response{Success: false, Error: "Key is required"})
		return
	}

	// Check if this node is the leader
	if !kv.node.IsLeader() {
		respondJSON(w, http.StatusServiceUnavailable, Response{Success: false, Error: "Not the leader"})
		return
	}

	// Propose delete command
	cmd := &pb.Command{
		Op:  "delete",
		Key: key,
	}

	index, commitCh, ok := kv.node.ProposeCommand(cmd)
	if !ok {
		respondJSON(w, http.StatusServiceUnavailable, Response{Success: false, Error: "Not the leader"})
		return
	}

	// Wait for commit
	select {
	case <-commitCh:
		log.Printf("Delete committed at index %d", index)
		respondJSON(w, http.StatusOK, Response{Success: true})
	case <-time.After(5 * time.Second):
		respondJSON(w, http.StatusRequestTimeout, Response{Success: false, Error: "Timeout waiting for commit"})
	}
}

func (kv *KVServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	state, term := kv.node.GetState()
	stateStr := map[raft.NodeState]string{
		raft.Follower:  "follower",
		raft.Candidate: "candidate",
		raft.Leader:    "leader",
	}[state]

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"node_id": kv.node.Id,
		"state":   stateStr,
		"term":    term,
	})
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func main() {
	// Command line flags
	nodeID := flag.Int64("id", 1, "Node ID")
	grpcPort := flag.Int("grpc-port", 50051, "gRPC port for Raft communication")
	httpPort := flag.Int("http-port", 8080, "HTTP port for client API")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:50052,localhost:50053)")
	dataDir := flag.String("data-dir", "./raft-data", "Directory for persisting Raft state")
	flag.Parse()

	log.Printf("Starting node %d with gRPC on port %d and HTTP on port %d", *nodeID, *grpcPort, *httpPort)

	// Create KV store
	store := database.NewInMemDataStore()

	// Create persister for Raft state
	persister, err := raft.NewPersister(*nodeID, *dataDir)
	if err != nil {
		log.Fatalf("Failed to create persister: %v", err)
	}

	// Parse peer addresses and create clients
	var peerIDs []int64
	clients := make(map[int64]raft.RaftClientLayer)

	if *peers != "" {
		// Map peer addresses to IDs
		// Format: localhost:50051 -> node 1, localhost:50052 -> node 2, etc.
		peerAddrs := parsePeers(*peers)
		for _, addr := range peerAddrs {
			// Extract port and derive peer ID
			peerID := getPeerIDFromAddr(addr)
			if peerID == *nodeID {
				continue // Skip self
			}

			// Create connection options with proper credentials
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			}

			// Try to connect with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := grpc.NewClient(addr, opts...)
			if err != nil {
				log.Printf("Failed to create client for peer %s: %v", addr, err)
				continue
			}

			// Test connectivity
			go func(c *grpc.ClientConn) {
				<-ctx.Done()
			}(conn)

			clients[peerID] = &RaftClient{
				client: pb.NewRaftClient(conn),
				conn:   conn,
			}
			peerIDs = append(peerIDs, peerID)
			log.Printf("Node %d configured peer: ID=%d, addr=%s", *nodeID, peerID, addr)
		}
	}

	// Create Raft node with persister
	node := raft.NewRaftNode(*nodeID, peerIDs, clients, store, persister)
	node.Start()

	// Start gRPC server for Raft communication
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, &Server{node: node})

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port: %v", err)
	}

	go func() {
		log.Printf("gRPC server listening on :%d", *grpcPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Start HTTP server for client API
	kvServer := &KVServer{node: node}
	http.HandleFunc("/write", kvServer.WriteHandler)
	http.HandleFunc("/read", kvServer.ReadHandler)
	http.HandleFunc("/delete", kvServer.DeleteHandler)
	http.HandleFunc("/status", kvServer.StatusHandler)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: http.DefaultServeMux,
	}

	go func() {
		log.Printf("HTTP server listening on :%d", *httpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to serve HTTP: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	grpcServer.GracefulStop()
	httpServer.Shutdown(context.Background())
}

func parsePeers(peers string) []string {
	if peers == "" {
		return nil
	}
	var result []string
	for i := 0; i < len(peers); i++ {
		start := i
		for i < len(peers) && peers[i] != ',' {
			i++
		}
		result = append(result, peers[start:i])
	}
	return result
}

// getPeerIDFromAddr extracts peer ID from address
// Maps: localhost:50051 -> 1, localhost:50052 -> 2, localhost:50053 -> 3
func getPeerIDFromAddr(addr string) int64 {
	// Extract port from address
	var port string
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			port = addr[i+1:]
			break
		}
	}

	// Map ports to IDs: 50051->1, 50052->2, 50053->3, etc.
	switch port {
	case "50051":
		return 1
	case "50052":
		return 2
	case "50053":
		return 3
	default:
		// Fallback: try to parse port and derive ID
		var portNum int64
		fmt.Sscanf(port, "%d", &portNum)
		return portNum - 50050 // 50051->1, 50052->2, etc.
	}
}
