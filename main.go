package main

import (
	"flag"
	"fmt"
	"key_value_store/internal/raft"
	"key_value_store/internal/server"
	"os"
	"strings"
)

func main() {
	nodeId := flag.String("nodeId", "", "provide nodeId - node_x where x is a number")
	raftPort := flag.String("raftPort", "", "provide raft port number")
	peers := flag.String("peers", "", "peer list of the cluster format - node_2#127.0.0.1:11000,node_3#127.0.0.1:12000")
	restPort := flag.String("restPort", "", "provide port for the rest server")
	shardCount := flag.Int("shardCount", 3, "number of shards in the cluster")
	flag.Parse()

	fmt.Println(*nodeId)
	fmt.Println(*raftPort)
	fmt.Println(*peers)
	fmt.Println(*restPort)

	if *nodeId == "" {
		fmt.Println("nodeId not provided")
		flag.Usage()
		os.Exit(1)
	}

	if *raftPort == "" {
		fmt.Println("raft port not provided")
		flag.Usage()
		os.Exit(1)
	}

	if *peers == "" {
		fmt.Println("peers flag is mandatory")
		flag.Usage()
		os.Exit(1)
	}

	if *restPort == "" {
		fmt.Println("restport not provided")
		flag.Usage()
		os.Exit(1)
	}

	peerMap := make(map[string]string)
	if *peers != "" {
		peerList := strings.Split(*peers, ",")
		for _, peerEntry := range peerList {
			parts := strings.Split(peerEntry, "#")
			if len(parts) != 2 {
				fmt.Printf("Invalid peer format: %s\n", peerEntry)
				os.Exit(1)
			}
			peerId := strings.TrimSpace(parts[0])
			peerAddr := strings.TrimSpace(parts[1])
			peerMap[peerId] = peerAddr
		}
	}

	peerMap[*nodeId] = fmt.Sprintf("127.0.0.1:%s", *raftPort)

	mr := raft.NewMultiRaft(peerMap, *shardCount, *nodeId)

	go func() {
		if err := mr.StartGrpcServer(fmt.Sprintf(":%s", *raftPort)); err != nil {
			fmt.Printf("gRPC server failed: %v\n", err)
			os.Exit(1)
		}
	}()

	restServer := server.NewRestServer(mr)

	fmt.Printf("Starting REST server on port %s for node %s\n", *restPort, *nodeId)
	restServer.Run(*restPort)
}
