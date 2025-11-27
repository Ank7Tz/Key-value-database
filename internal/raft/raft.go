package raft

import (
	database "key_value_store/internal/kvstore"
	pb "key_value_store/internal/raft/RPC"
	"log"
	"math/rand"
	"sync"
	"time"
)

type RaftClientLayer interface {
	SendVoteRequest(groupId string, req *pb.RequestVote, reply *pb.RequestVoteReply) bool
	SendAppendEntriesRequest(groupId string, req *pb.AppendEntriesRequest, reply *pb.AppendEntriesReply) bool
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type RaftNode struct {
	mtx sync.Mutex

	groupId string

	Id    string
	state NodeState
	peers []string

	currentLeader string

	currentTerm int64
	votedFor    string

	log []*pb.LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  []int64
	matchIndex []int64

	electionTimeout  time.Duration
	lastHeartBeat    time.Time
	heartBeatTimeout time.Duration

	KVStore database.Store
	applyCh chan pb.LogEntry

	pendingCommands map[int64]chan bool

	clients map[string]RaftClientLayer

	persister *Persister
}

func NewRaftNode(id string, groupId string, peers []string, clients map[string]RaftClientLayer, store database.Store, persister *Persister) *RaftNode {
	// Try to load persisted state
	var persistedState *PersistentState
	if persister != nil {
		var err error
		persistedState, err = persister.LoadState()
		if err != nil {
			log.Printf("Node %s failed to load persisted state: %v, starting fresh", id, err)
			persistedState = nil
		}
	}

	node := &RaftNode{
		Id:            id,
		groupId:       groupId,
		state:         Follower,
		peers:         peers,
		currentTerm:   0,
		currentLeader: "",
		votedFor:      "",
		log: []*pb.LogEntry{{
			Term: 0,
		}},
		commitIndex:      0,
		lastApplied:      0,
		KVStore:          store,
		applyCh:          make(chan pb.LogEntry, 100),
		heartBeatTimeout: 1000 * time.Millisecond,
		pendingCommands:  make(map[int64]chan bool),
		clients:          clients,
		persister:        persister,
	}

	// Restore from persisted state if available
	if persistedState != nil {
		node.currentTerm = persistedState.CurrentTerm
		node.votedFor = persistedState.VotedFor
		node.log = persistedState.Log
		log.Printf("Node %s restored state: term=%d, log entries=%d",
			id, node.currentTerm, len(node.log))

		// Replay log to rebuild state
		// node.replayLog()
	}

	node.resetElectionTimeout()

	return node
}

func (node *RaftNode) resetElectionTimeout() {
	node.electionTimeout = time.Duration(1000+rand.Intn(500)) * time.Millisecond
	node.lastHeartBeat = time.Now()
}

func (node *RaftNode) persist() {
	if node.persister == nil {
		return
	}

	state := &PersistentState{
		CurrentTerm: node.currentTerm,
		VotedFor:    node.votedFor,
		Log:         node.log,
	}

	if err := node.persister.SaveState(state); err != nil {
		log.Printf("Node %s failed to persist state: %v", node.Id, err)
	}
}

func (node *RaftNode) Start() {
	go node.ElectionTimer()
	go node.applyCommittedEntries()
}

func (node *RaftNode) ElectionTimer() {
	for {
		time.Sleep(100 * time.Millisecond)

		node.mtx.Lock()
		if node.state == Leader {
			node.mtx.Unlock()
			continue
		}

		if time.Since(node.lastHeartBeat) > node.electionTimeout {
			node.mtx.Unlock()
			node.StartElection()
			node.mtx.Lock()
		}
		node.mtx.Unlock()
	}
}

func (node *RaftNode) StartElection() {
	node.mtx.Lock()
	node.state = Candidate
	node.currentTerm++
	node.votedFor = node.Id
	node.persist()

	node.resetElectionTimeout()

	currentTerm := node.currentTerm
	lastLogIndex := node.getLastLogIndex()
	lastLogTerm := node.getLogTerm(lastLogIndex)
	node.mtx.Unlock()

	log.Printf("Node %s starting election (group - %s) for term %d", node.Id, node.groupId, currentTerm)

	votes := 1
	var voteMtx sync.Mutex

	// In single-node cluster, become leader immediately
	if len(node.clients) == 0 {
		node.mtx.Lock()
		node.becomeLeader()
		node.mtx.Unlock()
		return
	}

	for _, conn := range node.clients {
		go func(conn RaftClientLayer) {
			voteRequest := &pb.RequestVote{
				ShardId:      node.groupId,
				Term:         currentTerm,
				CandidateId:  node.Id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			voteRequestReply := &pb.RequestVoteReply{}

			if conn.SendVoteRequest(node.groupId, voteRequest, voteRequestReply) {
				node.mtx.Lock()
				defer node.mtx.Unlock()

				if voteRequestReply.Term > node.currentTerm {
					node.becomeFollower(voteRequestReply.Term)
					return
				}

				if node.state == Candidate && voteRequestReply.Term == currentTerm && voteRequestReply.VoteGranted {
					voteMtx.Lock()
					votes++
					currentVotes := votes
					voteMtx.Unlock()

					if currentVotes > (len(node.peers)+1)/2 {
						node.becomeLeader()
					}
				}
			}

		}(conn)
	}
}

func (node *RaftNode) becomeFollower(term int64) {
	node.state = Follower
	node.currentTerm = term
	node.votedFor = ""
	node.persist()
	node.resetElectionTimeout()
	log.Printf("Node %s became follower (group -%s) for term %d", node.Id, node.groupId, term)
}

func (node *RaftNode) becomeLeader() {
	if node.state != Candidate {
		return
	}

	log.Printf("Node %s became leader (group - %s) for term %d", node.Id, node.groupId, node.currentTerm)
	node.state = Leader
	node.currentLeader = node.Id
	node.nextIndex = make([]int64, len(node.peers))
	node.matchIndex = make([]int64, len(node.peers))

	lastLogIndex := int64(len(node.log))
	for i := range node.nextIndex {
		node.nextIndex[i] = lastLogIndex
		node.matchIndex[i] = 0
	}

	go node.sendHeartBeats()
}

func (node *RaftNode) sendHeartBeats() {
	for {
		node.mtx.Lock()
		if node.state != Leader {
			node.mtx.Unlock()
			return
		}

		for i, peers := range node.peers {
			go node.SendAppendEntries(i, peers)
		}

		node.mtx.Unlock()
		time.Sleep(node.heartBeatTimeout)
	}
}

func (node *RaftNode) SendAppendEntries(peerIdx int, peerId string) {
	node.mtx.Lock()
	if node.state != Leader {
		node.mtx.Unlock()
		return
	}

	prevLogIndex := node.nextIndex[peerIdx] - 1
	prevLogTerm := node.log[prevLogIndex].Term
	entries := node.log[node.nextIndex[peerIdx]:]

	req := pb.AppendEntriesRequest{
		ShardId:           node.groupId,
		Term:              node.currentTerm,
		LeaderId:          node.Id,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: node.commitIndex,
	}

	node.mtx.Unlock()

	reply := pb.AppendEntriesReply{}
	if node.appendEntriesRPC(peerId, &req, &reply) {
		node.mtx.Lock()
		defer node.mtx.Unlock()

		if reply.Term > node.currentTerm {
			node.becomeFollower(reply.Term)
			return
		}

		if node.state == Leader {
			if reply.Success {
				node.nextIndex[peerIdx] = prevLogIndex + int64(len(entries)) + 1
				node.matchIndex[peerIdx] = prevLogIndex + int64(len(entries))
				node.updateCommitIndex()
			} else {
				node.nextIndex[peerIdx] = max(1, node.nextIndex[peerIdx]-1)
			}
		}
	}
}

func (node *RaftNode) appendEntriesRPC(peerId string, req *pb.AppendEntriesRequest, reply *pb.AppendEntriesReply) bool {
	client := node.clients[peerId]
	return client.SendAppendEntriesRequest(node.groupId, req, reply)
}

func (node *RaftNode) updateCommitIndex() {
	for i := node.commitIndex + 1; i < int64(len(node.log)); i++ {
		if node.log[i].Term != node.currentTerm {
			continue
		}

		replicaCount := 1
		for j := range node.peers {
			if node.matchIndex[j] >= i {
				replicaCount++
			}
		}

		if replicaCount > (len(node.peers)+1)/2 {
			node.commitIndex = i
		}
	}
}

func (node *RaftNode) applyCommittedEntries() {
	for {
		time.Sleep(10 * time.Millisecond)

		node.mtx.Lock()
		for node.lastApplied < node.commitIndex {
			node.lastApplied++

			entry := node.log[node.lastApplied]
			node.applyToStateMachine(entry)

			if ch, ok := node.pendingCommands[node.lastApplied]; ok {
				ch <- true
				close(ch)
				delete(node.pendingCommands, node.lastApplied)
			}
		}
		node.mtx.Unlock()
	}
}

func (node *RaftNode) applyToStateMachine(entry *pb.LogEntry) {
	switch entry.Command.Op {
	case "write":
		node.KVStore.Write(entry.Command.Key, entry.Command.Value)
		log.Printf("Node %s applied: WRITE %s=%s", node.Id, entry.Command.Key, entry.Command.Value)
	case "delete":
		node.KVStore.Delete(entry.Command.Key)
		log.Printf("Node %s applied: DELETE %s", node.Id, entry.Command.Key)
	}
}

func (node *RaftNode) RequestVoteRPCHandler(req *pb.RequestVote, reply *pb.RequestVoteReply) error {
	node.mtx.Lock()
	defer node.mtx.Unlock()

	if req.Term > node.currentTerm {
		node.becomeFollower(req.Term)
	}

	reply.Term = node.currentTerm
	reply.VoteGranted = false

	if req.Term < node.currentTerm {
		return nil
	}

	lastLogIndex := int64(len(node.log)) - 1
	lastLogTerm := node.getLogTerm(lastLogIndex)

	logUpToDate := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)
	if logUpToDate && (node.votedFor == "" || node.votedFor == req.CandidateId) {
		reply.VoteGranted = true
		node.votedFor = req.CandidateId
		node.persist()
		node.resetElectionTimeout()
	}

	return nil
}

func (node *RaftNode) AppendEntriesHandler(req *pb.AppendEntriesRequest, reply *pb.AppendEntriesReply) error {
	node.mtx.Lock()
	defer node.mtx.Unlock()

	if req.Term > node.currentTerm {
		node.becomeFollower(req.Term)
	}

	reply.Term = node.currentTerm
	reply.Success = false

	if req.Term < node.currentTerm {
		return nil
	}

	node.resetElectionTimeout()
	node.currentLeader = req.LeaderId

	// Check log consistency
	if req.PrevLogIndex >= int64(len(node.log)) || node.getLogTerm(req.PrevLogIndex) != req.PrevLogTerm {
		return nil
	}

	// Delete conflicting entries and append new ones
	// Keep entries up to and including prevLogIndex, then append new entries
	node.log = node.log[:req.PrevLogIndex+1]
	node.log = append(node.log, req.Entries...)
	node.persist()

	// Update commit index
	if req.LeaderCommitIndex > node.commitIndex {
		node.commitIndex = min(req.LeaderCommitIndex, int64(len(node.log)-1))
	}

	reply.Success = true
	return nil
}

func (node *RaftNode) getLogTerm(index int64) int64 {
	if index < 0 || index >= int64(len(node.log)) {
		return 0
	}
	return node.log[index].Term
}

func (node *RaftNode) ProposeCommand(cmd *pb.Command) (int64, chan bool, bool) {
	node.mtx.Lock()
	defer node.mtx.Unlock()

	if node.state != Leader {
		return -1, nil, false
	}

	entry := &pb.LogEntry{
		Term:    node.currentTerm,
		Command: cmd,
	}

	node.log = append(node.log, entry)
	logIndex := int64(len(node.log) - 1)
	node.persist()

	ch := make(chan bool, 1)
	node.pendingCommands[logIndex] = ch

	log.Printf("Node %s proposed command at index %d: %s %s", node.Id, logIndex, cmd.Op, cmd.Key)

	if len(node.peers) == 0 {
		node.commitIndex = logIndex
		log.Printf("Node %s (single-node) committed index %d", node.Id, logIndex)
	}

	return logIndex, ch, true
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (node *RaftNode) GetState() (NodeState, int64) {
	node.mtx.Lock()
	defer node.mtx.Unlock()
	return node.state, node.currentTerm
}

func (node *RaftNode) IsLeader() bool {
	node.mtx.Lock()
	defer node.mtx.Unlock()
	return node.state == Leader
}

func (node *RaftNode) getLastLogIndex() int64 {
	return int64(len(node.log)) - 1
}

func (node *RaftNode) GetLeader() string {
	node.mtx.Lock()
	defer node.mtx.Unlock()

	return node.currentLeader
}
