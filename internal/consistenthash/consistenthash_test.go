package consistenthash

import (
	"fmt"
	"testing"
)

// Test 1: Basic functionality - does it create without crashing?
func TestNewRingBasedConsistentHash(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	if ch == nil {
		t.Fatal("Expected non-nil ConsistentHash")
	}

	if ch.numShards != 53 {
		t.Errorf("Expected 53 shards, got %d", ch.numShards)
	}

	if ch.numNodes != 5 {
		t.Errorf("Expected 5 nodes, got %d", ch.numNodes)
	}

	fmt.Println("✓ Test 1 PASSED: Ring created successfully")
}

// Test 2: Same key always maps to same shard (consistency)
func TestGetShardIdForKey_Consistency(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	testKeys := []string{
		"user:alice",
		"user:bob",
		"product:12345",
		"session:abc-def-ghi",
	}

	for _, key := range testKeys {
		shard1 := ch.GetShardIdForKey(key)
		shard2 := ch.GetShardIdForKey(key)
		shard3 := ch.GetShardIdForKey(key)

		if shard1 != shard2 || shard2 != shard3 {
			t.Errorf("Key '%s' mapped to different shards: %d, %d, %d", key, shard1, shard2, shard3)
		}

		if shard1 >= 53 {
			t.Errorf("Shard ID %d is out of range [0-52]", shard1)
		}

		fmt.Printf("  Key '%s' → Shard %d\n", key, shard1)
	}

	fmt.Println("✓ Test 2 PASSED: Keys consistently map to same shards")
}

// Test 3: Each shard gets exactly 3 replica nodes
func TestGetPhysicalNodesForShardId_ReplicaCount(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	// Check all shards
	for shardId := uint64(0); shardId < 53; shardId++ {
		nodes := ch.GetPhysicalNodesForShardId(shardId, 3)

		if len(nodes) != 3 {
			t.Errorf("Shard %d has %d replicas, expected 3", shardId, len(nodes))
		}
	}

	fmt.Println("✓ Test 3 PASSED: All shards have exactly 3 replicas")
}

// Test 4: Replica nodes are distinct (no duplicates)
func TestGetPhysicalNodesForShardId_Distinct(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	for shardId := uint64(0); shardId < 53; shardId++ {
		nodes := ch.GetPhysicalNodesForShardId(shardId, 3)

		seen := make(map[uint64]bool)
		for _, nodeID := range nodes {
			if seen[nodeID] {
				t.Errorf("Shard %d has duplicate node %d in replicas: %v", shardId, nodeID, nodes)
			}
			seen[nodeID] = true

			// Check node ID is valid (0-4)
			if nodeID >= 5 {
				t.Errorf("Invalid node ID %d for shard %d", nodeID, shardId)
			}
		}
	}

	fmt.Println("✓ Test 4 PASSED: All replica nodes are distinct")
}

// Test 5: Distribution - check if shards are reasonably balanced across nodes
func TestShardDistribution(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	stats := ch.GetNodeStats()

	fmt.Println("\n--- Shard Distribution ---")
	totalReplicas := 0
	for nodeID := uint64(0); nodeID < 5; nodeID++ {
		count := stats[nodeID]
		totalReplicas += count
		fmt.Printf("  Node %d: %d shard replicas\n", nodeID, count)
	}

	// Total should be 53 shards * 3 replicas = 159
	expectedTotal := 53 * 3
	if totalReplicas != expectedTotal {
		t.Errorf("Total replicas is %d, expected %d", totalReplicas, expectedTotal)
	}

	// Check that no node is completely empty or overloaded
	// With 159 replicas across 5 nodes, average is ~32 per node
	// Allow range of 20-45 (reasonable variance)
	for nodeID, count := range stats {
		if count < 20 || count > 45 {
			t.Errorf("Node %d has %d replicas, which seems unbalanced (expected ~20-45)", nodeID, count)
		}
	}

	fmt.Println("✓ Test 5 PASSED: Shard distribution is reasonable")
}

// Test 6: End-to-end workflow
func TestEndToEndWorkflow(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	fmt.Println("\n--- End-to-End Test ---")

	// Simulate a client request
	key := "user:charlie:profile"

	// Step 1: Find shard
	shardId := ch.GetShardIdForKey(key)
	fmt.Printf("  Step 1: Key '%s' maps to Shard %d\n", key, shardId)

	// Step 2: Find replica nodes
	nodes := ch.GetPhysicalNodesForShardId(shardId, 3)
	fmt.Printf("  Step 2: Shard %d is managed by nodes: %v\n", shardId, nodes)

	// Step 3: Verify we can route to these nodes
	if len(nodes) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(nodes))
	}

	fmt.Printf("  Step 3: Would send request to Raft group with nodes %v\n", nodes)
	fmt.Printf("           (Node %d is likely the leader)\n", nodes[0])

	fmt.Println("✓ Test 6 PASSED: End-to-end workflow successful")
}

// Test 7: GetAllShardMappings returns all 53 shards
func TestGetAllShardMappings(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	mappings := ch.GetAllShardMappings()

	if len(mappings) != 53 {
		t.Errorf("Expected 53 shard mappings, got %d", len(mappings))
	}

	fmt.Println("✓ Test 7 PASSED: All shard mappings computed")
}

// Test 8: Different keys map to different shards (not all colliding)
func TestKeyDistribution(t *testing.T) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	shardCounts := make(map[uint64]int)

	// Generate 1000 test keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		shardId := ch.GetShardIdForKey(key)
		shardCounts[shardId]++
	}

	// Check that we're using multiple shards (not all keys going to one shard)
	uniqueShards := len(shardCounts)
	if uniqueShards < 40 {
		t.Errorf("Only %d shards used out of 53, distribution seems poor", uniqueShards)
	}

	fmt.Printf("  1000 keys distributed across %d shards\n", uniqueShards)
	fmt.Println("✓ Test 8 PASSED: Keys distribute across multiple shards")
}

// Benchmark: How fast is key lookup?
func BenchmarkGetShardIdForKey(b *testing.B) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)
	key := "user:benchmark:test"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.GetShardIdForKey(key)
	}
}

// Benchmark: How fast is node lookup?
func BenchmarkGetPhysicalNodesForShardId(b *testing.B) {
	ch := NewRingBasedConsistentHash(53, 5, 271, 20, 3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.GetPhysicalNodesForShardId(uint64(i%53), 3)
	}
}
