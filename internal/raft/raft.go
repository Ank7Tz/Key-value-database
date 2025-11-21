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
	SendVoteRequest(*pb.RequestVote, *pb.RequestVoteReply) bool
	SendAppendEntriesRequest(*pb.AppendEntriesRequest, *pb.AppendEntriesReply) bool
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type RaftNode struct {
	mtx sync.Mutex

	Id    int64
	state NodeState
	peers []int64

	currentTerm int64
	votedFor    int64

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

	clients map[int64]RaftClientLayer

	persister *Persister
}

func NewRaftNode(id int64, peers []int64, clients map[int64]RaftClientLayer, store database.Store, persister *Persister) *RaftNode {
	// Try to load persisted state
	var persistedState *PersistentState
	if persister != nil {
		var err error
		persistedState, err = persister.LoadState()
		if err != nil {
			log.Printf("Node %d failed to load persisted state: %v, starting fresh", id, err)
			persistedState = nil
		}
	}

	node := &RaftNode{
		Id:          id,
		state:       Follower,
		peers:       peers,
		currentTerm: 0,
		votedFor:    -1,
		log: []*pb.LogEntry{{
			Term: 0,
		}},
		commitIndex:      0,
		lastApplied:      0,
		KVStore:          store,
		applyCh:          make(chan pb.LogEntry, 100),
		heartBeatTimeout: 150 * time.Millisecond,
		pendingCommands:  make(map[int64]chan bool),
		clients:          clients,
		persister:        persister,
	}

	// Restore from persisted state if available
	if persistedState != nil {
		node.currentTerm = persistedState.CurrentTerm
		node.votedFor = persistedState.VotedFor
		node.log = persistedState.Log
		log.Printf("Node %d restored state: term=%d, log entries=%d",
			id, node.currentTerm, len(node.log))

		// Replay log to rebuild state
		node.replayLog()
	}

	node.resetElectionTimeout()

	return node
}

func (node *RaftNode) resetElectionTimeout() {
	node.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
	node.lastHeartBeat = time.Now()
}

// persist saves the current state to disk
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
		log.Printf("Node %d failed to persist state: %v", node.Id, err)
	}
}

// replayLog replays all log entries to rebuild the state machine
func (node *RaftNode) replayLog() {
	// Apply all entries from lastApplied+1 up to the last log entry
	lastIndex := int64(len(node.log)) - 1

	for i := node.lastApplied + 1; i <= lastIndex; i++ {
		if i >= 0 && i < int64(len(node.log)) {
			entry := node.log[i]
			if entry.Command != nil {
				node.applyToStateMachine(entry)
				node.lastApplied = i
			}
		}
	}

	if node.lastApplied > 0 {
		log.Printf("Node %d replayed log: applied %d entries", node.Id, node.lastApplied)
	}
}

func (node *RaftNode) Start() {
	go node.ElectionTimer()
	go node.applyCommittedEntries()
}

func (node *RaftNode) ElectionTimer() {
	for {
		time.Sleep(10 * time.Millisecond)

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

	log.Printf("Node %d starting election for term %d", node.Id, currentTerm)

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
				Term:         currentTerm,
				CandidateId:  node.Id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			voteRequestReply := &pb.RequestVoteReply{}

			if conn.SendVoteRequest(voteRequest, voteRequestReply) {
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
	node.votedFor = -1
	node.persist()
	node.resetElectionTimeout()
	log.Printf("Node %d became follower for term %d", node.Id, term)
}

func (node *RaftNode) becomeLeader() {
	if node.state != Candidate {
		return
	}

	log.Printf("Node %d became leader for term %d", node.Id, node.currentTerm)
	node.state = Leader
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

func (node *RaftNode) SendAppendEntries(peerIdx int, peerId int64) {
	node.mtx.Lock()
	if node.state != Leader {
		node.mtx.Unlock()
		return
	}

	prevLogIndex := node.nextIndex[peerIdx] - 1
	prevLogTerm := node.log[prevLogIndex].Term
	entries := node.log[node.nextIndex[peerIdx]:]

	req := pb.AppendEntriesRequest{
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

func (node *RaftNode) appendEntriesRPC(peer int64, req *pb.AppendEntriesRequest, reply *pb.AppendEntriesReply) bool {
	client := node.clients[peer]
	return client.SendAppendEntriesRequest(req, reply)
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
		log.Printf("Node %d applied: SET %s=%s", node.Id, entry.Command.Key, entry.Command.Value)
	case "delete":
		node.KVStore.Delete(entry.Command.Key)
		log.Printf("Node %d applied: DELETE %s", node.Id, entry.Command.Key)
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
	if logUpToDate && (node.votedFor == -1 || node.votedFor == req.CandidateId) {
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

	log.Printf("Node %d proposed command at index %d: %s %s", node.Id, logIndex, cmd.Op, cmd.Key)

	// In single-node cluster, commit immediately
	if len(node.peers) == 0 {
		node.commitIndex = logIndex
		log.Printf("Node %d (single-node) committed index %d", node.Id, logIndex)
	}

	return logIndex, ch, true
}

// Helper functions
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

// GetState returns the current state and term
func (node *RaftNode) GetState() (NodeState, int64) {
	node.mtx.Lock()
	defer node.mtx.Unlock()
	return node.state, node.currentTerm
}

// IsLeader returns whether this node is currently the leader
func (node *RaftNode) IsLeader() bool {
	node.mtx.Lock()
	defer node.mtx.Unlock()
	return node.state == Leader
}

// getLastLogIndex returns the index of the last log entry
func (node *RaftNode) getLastLogIndex() int64 {
	return int64(len(node.log)) - 1
}

// getLogEntry returns the log entry at the given index
func (node *RaftNode) getLogEntry(index int64) *pb.LogEntry {
	if index < 0 || index >= int64(len(node.log)) {
		return nil
	}
	return node.log[index]
}
