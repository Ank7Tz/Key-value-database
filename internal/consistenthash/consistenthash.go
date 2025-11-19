package consistenthash

import (
	"hash/crc32"
	"sync"
)

type ConsistentHash interface {
	// Find the node for key
	GetShardIdForKey(key string) uint64
	// Find the successor nodes for a partition
	GetPhysicalNodesForShardId(partitionId uint64, n int) uint64
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
