package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"key_value_store/internal/raft"
)

var (
	nodeId    = flag.String("node", "node1", "Node ID")
	raftport  = flag.Int("raftport", 7000, "Raft port number")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap this node as first in cluster")
	dataDir   = flag.String("data", "./data", "Data directory")
)

func main() {
	flag.Parse()

	// Define local node
	localNode := raft.Node{
		NodeId: *nodeId,
		Addr:   "127.0.0.1",
		Port:   *raftport,
	}

	// Create MultiRaft instance
	// 4 shard, cluster size 3, replication factor 3
	mr, err := raft.NewMultiRaft(localNode, 4, 3, 3, *dataDir)
	if err != nil {
		log.Fatalf("Failed to create MultiRaft: %v", err)
	}

	// Initialize and start gRPC server
	if err := mr.Init(); err != nil {
		log.Fatalf("Failed to initialize MultiRaft: %v", err)
	}
	log.Printf("MultiRaft initialized for node %s on port %d", *nodeId, *raftport)

	// Wait a bit for gRPC server to start
	time.Sleep(1 * time.Second)

	// Create Raft groups for each shard
	if *bootstrap {
		log.Println("Bootstrapping cluster...")

		// Create 4 shard with this node as the only member (bootstrap)
		nodes := []raft.Node{localNode}

		for i := uint32(0); i < 4; i++ {
			log.Printf("Creating shard %d...", i)
			if err := mr.CreateRaftGroup(i, nodes); err != nil {
				log.Fatalf("Failed to create raft group for shard_%d: %v", i, err)
			}
			time.Sleep(500 * time.Millisecond)
		}

		log.Println("All shards created successfully")

		// Wait for leaders to be elected
		time.Sleep(2 * time.Second)

		// Test Put/Get operations
		log.Println("\n=== Testing Put/Get operations ===")

		testKeys := []string{"key1", "key2", "key3", "key4"}
		for i, key := range testKeys {
			value := fmt.Sprintf("value%d", i+1)
			log.Printf("Putting %s=%s", key, value)

			if err := mr.Put(key, value); err != nil {
				log.Printf("ERROR: Failed to put %s: %v", key, err)
			} else {
				log.Printf("✓ Successfully put %s=%s", key, value)
			}

			time.Sleep(100 * time.Millisecond)
		}

		time.Sleep(1 * time.Second)

		// Get values back
		log.Println("\n=== Reading values ===")
		for _, key := range testKeys {
			value, err := mr.Get(key)
			if err != nil {
				log.Printf("ERROR: Failed to get %s: %v", key, err)
			} else {
				log.Printf("✓ Got %s=%s", key, value)
			}
		}

		log.Println("\n=== Test completed ===")
		log.Println("Press Ctrl+C to exit")
	} else {
		log.Printf("Node %s started and waiting to join cluster...", *nodeId)
		log.Println("To join an existing cluster, implement the join logic")
		log.Println("Press Ctrl+C to exit")
	}

	// Keep running
	select {}
}
