package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type ConsistentHash interface {
	// Find the node for key
	GetShardIdForKey(key string) uint64
	// Find the successor nodes for a partition
	GetPhysicalNodesForShardId(partitionId uint64, n int) uint64
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type member string

func (m member) String() string {
	return string(m)
}

type RingBasedConsistentHash struct {
	numShards    uint32
	numNodes     uint32
	replicaCount int
	ring         *consistent.Consistent // The consistent hash ring from the library
	shardToNodes map[uint64][]uint64    // Pre-computed: shard -> physical node IDs
	nodeNameToID map[string]uint64      // Maps node name to node ID (0-4)
	nodeIDToName map[uint64]string      // Maps node ID to node name
	mtx          sync.RWMutex
}

func NewRingBasedConsistentHash(shardCount, nodeCount, partitionCount, replicationFactor, replicaCount int) *RingBasedConsistentHash {
	cfg := consistent.Config{
		PartitionCount:    partitionCount,    // Higher = better distribution (e.g., 271)
		ReplicationFactor: replicationFactor, // How many places each member appears (e.g., 20)
		Load:              1.25,              // Load balancing factor
		Hasher:            hasher{},          // Use xxhash for hashing
	}

	ch := &RingBasedConsistentHash{
		numShards:    uint32(shardCount),
		numNodes:     uint32(nodeCount),
		replicaCount: replicaCount,
		ring:         consistent.New(nil, cfg),
		shardToNodes: make(map[uint64][]uint64),
		nodeNameToID: make(map[string]uint64),
		nodeIDToName: make(map[uint64]string),
	}

	// Add all physical nodes to the ring
	ch.initializeNodes(nodeCount)

	// Pre-compute which nodes manage which shards
	ch.computeShardMappings(replicationFactor)

	return ch
}

// initializeNodes adds all physical nodes to the ring
func (c *RingBasedConsistentHash) initializeNodes(nodeCount int) {
	members := []consistent.Member{}

	for nodeId := 0; nodeId < nodeCount; nodeId++ {
		nodeName := fmt.Sprintf("node-%d", nodeId)
		m := member(nodeName)
		members = append(members, m)

		// Store bidirectional mapping
		c.nodeNameToID[nodeName] = uint64(nodeId)
		c.nodeIDToName[uint64(nodeId)] = nodeName
	}

	// Add all members to the ring at once
	for _, m := range members {
		c.ring.Add(m)
	}
}

// computeShardMappings pre-computes which nodes manage each shard
func (c *RingBasedConsistentHash) computeShardMappings(replicaCount int) {
	for shardId := uint64(0); shardId < uint64(c.numShards); shardId++ {
		// Create a key for this shard
		shardKey := fmt.Sprintf("shard-%d", shardId)

		// Use the library to find which nodes should handle this shard
		// LocateKey returns the primary node for a key
		primaryMember := c.ring.LocateKey([]byte(shardKey))
		primaryNodeID := c.nodeNameToID[primaryMember.String()]

		// Get additional replicas using GetClosestN
		closestMembers, err := c.ring.GetClosestN([]byte(shardKey), c.replicaCount)
		if err != nil {
			// Handle error - fallback to just primary node
			c.shardToNodes[shardId] = []uint64{primaryNodeID}
			continue
		}

		// Convert members to node IDs
		nodes := make([]uint64, 0, len(closestMembers))
		seen := make(map[uint64]bool)

		for _, m := range closestMembers {
			nodeID := c.nodeNameToID[m.String()]
			if !seen[nodeID] {
				nodes = append(nodes, nodeID)
				seen[nodeID] = true
			}
		}

		// Ensure primary node is first
		finalNodes := make([]uint64, 0, replicaCount)
		finalNodes = append(finalNodes, primaryNodeID)
		for _, nodeID := range nodes {
			if nodeID != primaryNodeID && len(finalNodes) < replicaCount {
				finalNodes = append(finalNodes, nodeID)
			}
		}

		c.shardToNodes[shardId] = finalNodes
	}
}

// GetShardIdForKey returns which shard a key belongs to
func (c *RingBasedConsistentHash) GetShardIdForKey(key string) uint64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	// Hash the key and mod by number of shards
	hash := crc32.ChecksumIEEE([]byte(key))
	return uint64(hash % c.numShards)
}

// GetPhysicalNodesForShardId returns which nodes manage a shard
func (c *RingBasedConsistentHash) GetPhysicalNodesForShardId(shardId uint64, replicaCount int) []uint64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	// Return pre-computed mapping
	if nodes, exists := c.shardToNodes[shardId]; exists {
		// Return only the requested number of replicas
		if replicaCount > len(nodes) {
			replicaCount = len(nodes)
		}
		return nodes[:replicaCount]
	}

	return []uint64{}
}

// GetAllShardMappings returns all shard-to-nodes mappings (useful for debugging/stats)
func (c *RingBasedConsistentHash) GetAllShardMappings() map[uint64][]uint64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	// Return a copy to prevent external modification
	result := make(map[uint64][]uint64)
	for k, v := range c.shardToNodes {
		result[k] = append([]uint64{}, v...)
	}
	return result
}

// GetNodeStats returns statistics about shard distribution
func (c *RingBasedConsistentHash) GetNodeStats() map[uint64]int {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	stats := make(map[uint64]int)

	// Count how many shards each node is responsible for
	for _, nodes := range c.shardToNodes {
		for _, nodeID := range nodes {
			stats[nodeID]++
		}
	}

	return stats
}

type SimpleConsistentHash struct {
	numShard     uint32
	sortedHashes []uint32
	mtx          sync.RWMutex
}

func NewSimpleConsistentHash(shardCount int) ConsistentHash {
	return &SimpleConsistentHash{
		numShard:     uint32(shardCount), // Default to 1 to avoid division by zero
		sortedHashes: make([]uint32, 0),
	}
}

func (c *SimpleConsistentHash) GetShardIdForKey(key string) uint64 {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	hash := crc32.ChecksumIEEE([]byte(key))
	return uint64(hash % c.numShard)
}

func (c *SimpleConsistentHash) GetPhysicalNodesForShardId(partitionId uint64, n int) uint64 {
	// TODO
	return 0
}
