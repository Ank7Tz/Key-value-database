package consistenthash

import (
	"fmt"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type ConsistentHash interface {
	GetShardIdForKey(key string) string
	GetPhysicalNodesForShardId(shardIf string) []string
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type member string

func (m member) String() string {
	return string(m)
}

type ConsistentHashImpl struct {
	numShards    int
	numNodes     int
	ring1        *consistent.Consistent
	ring2        *consistent.Consistent
	shardToNodes map[string][]string
}

func NewConsistentHash(shardCount int, nodeCount int) *ConsistentHashImpl {
	cfgForShards := consistent.Config{
		PartitionCount:    shardCount, // Higher = better distribution (e.g., 271)
		ReplicationFactor: 20,         // How many places each member appears (e.g., 20)
		Load:              1.25,       // Load balancing factor
		Hasher:            hasher{},   // Use xxhash for hashing
	}

	cfgForNodes := consistent.Config{
		PartitionCount:    nodeCount,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}

	ch := &ConsistentHashImpl{
		numShards:    shardCount,
		numNodes:     nodeCount,
		ring1:        consistent.New(nil, cfgForShards),
		ring2:        consistent.New(nil, cfgForNodes),
		shardToNodes: make(map[string][]string),
	}

	ch.initializeNodes()
	ch.initializeShards()

	return ch
}

func (c *ConsistentHashImpl) initializeShards() {
	for i := range c.numShards {
		shardId := fmt.Sprintf("shard_%d", i+1)
		m := member(shardId)
		c.ring1.Add(m)
	}
}

func (c *ConsistentHashImpl) initializeNodes() {
	for i := range c.numNodes {
		nodeId := fmt.Sprintf("node_%d", i+1)
		m := member(nodeId)
		c.ring2.Add(m)
	}

	for i := range c.numShards {
		shardId := fmt.Sprintf("shard_%d", i+1)
		members, err := c.ring2.GetClosestN([]byte(shardId), 3)

		if err != nil {
			panic(err)
		}

		nodes := []string{}
		for _, member := range members {
			nodes = append(nodes, member.String())
		}

		c.shardToNodes[shardId] = nodes
	}
}

func (c *ConsistentHashImpl) GetShardIdForKey(key string) string {
	return c.ring1.LocateKey([]byte(key)).String()
}

func (c *ConsistentHashImpl) GetPhysicalNodesForShardId(shardId string) []string {
	return c.shardToNodes[shardId]
}
