//go:build ignore

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type StatsResponse struct {
	ShardMapping map[string][]string `json:"ShardMapping"`
	ShardGroups  map[string]struct {
		Store    map[string]string `json:"store"`
		Replicas []string          `json:"replicas"`
		Leader   string            `json:"leader"`
	} `json:"shardGroups"`
}

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	nodes := map[string]string{
		"node_1": "localhost:8000",
		"node_2": "localhost:8001",
		"node_3": "localhost:8002",
		"node_4": "localhost:8003",
		"node_5": "localhost:8004",
	}

	client := &http.Client{}

	data := map[string]string{
		"apple":   "fruit",
		"bob":     "designer",
		"hannah":  "director",
		"island":  "isolated",
		"ivan":    "scientist",
		"neptune": "ocean",
		"sarah":   "accountant",
		"violet":  "purple",
	}

	fmt.Println("=== PHASE 1: FETCHING STATS ===")
	statsURL := fmt.Sprintf("http://%s/api/data/stats", nodes["node_1"])
	resp, err := client.Get(statsURL)
	if err != nil {
		fmt.Printf("Error fetching stats: %v\n", err)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var stats StatsResponse
	if err := json.Unmarshal(body, &stats); err != nil {
		fmt.Printf("Error parsing stats: %v\n", err)
		return
	}

	shard7, ok := stats.ShardGroups["shard_7"]
	if !ok {
		fmt.Println("shard_7 not found in stats")
		return
	}

	leaderNode := shard7.Leader
	fmt.Printf("Shard 7 leader: %s\n", leaderNode)
	fmt.Printf("Shard 7 replicas: %v\n", shard7.Replicas)
	fmt.Println()

	fmt.Println("=== PHASE 2: STOPPING ALL NODES EXCEPT LEADER ===")
	for nodeName, nodeAddr := range nodes {
		if nodeName == leaderNode {
			fmt.Printf("KEEPING %s (%s) - shard_7 leader\n", nodeName, nodeAddr)
			continue
		}

		stopURL := fmt.Sprintf("http://%s/api/stop", nodeAddr)
		resp, err := client.Get(stopURL)
		if err != nil {
			fmt.Printf("Error stopping %s: %v\n", nodeName, err)
		} else {
			fmt.Printf("STOPPED %s (%s) - status: %d\n", nodeName, nodeAddr, resp.StatusCode)
			resp.Body.Close()
		}
	}
	fmt.Println()

	leaderAddr := nodes[leaderNode]
	baseURL := fmt.Sprintf("http://%s/api/data", leaderAddr)

	fmt.Println("=== Press ENTER to start read tests ===")
	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')

	fmt.Println("=== PHASE 3: REGULAR READS ===")
	passed := 0
	failed := 0

	for key, expectedValue := range data {
		url := fmt.Sprintf("%s/%s", baseURL, key)

		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("GET %s: ERROR - %v\n", key, err)
			failed++
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("GET %s: FAILED (status: %d)\n", key, resp.StatusCode)
			failed++
			continue
		}

		var result Response
		if err := json.Unmarshal(body, &result); err != nil {
			fmt.Printf("GET %s: ERROR parsing - %v\n", key, err)
			failed++
			continue
		}

		if result.Value == expectedValue {
			fmt.Printf("GET %s: PASS (got: %s)\n", key, result.Value)
			passed++
		} else {
			fmt.Printf("GET %s: FAIL (expected: %s, got: %s)\n", key, expectedValue, result.Value)
			failed++
		}
	}

	fmt.Println("\n=== REGULAR READ RESULTS ===")
	fmt.Printf("Passed: %d/%d\n", passed, len(data))
	fmt.Printf("Failed: %d/%d\n", failed, len(data))

	fmt.Println("\n=== Press ENTER to start strong consistency reads ===")
	reader.ReadString('\n')

	fmt.Println("=== PHASE 4: STRONG CONSISTENCY READS ===")
	strongPassed := 0
	strongFailed := 0

	for key, expectedValue := range data {
		url := fmt.Sprintf("%s/%s?strong_consistency=true", baseURL, key)

		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			fmt.Printf("STRONG GET %s: ERROR creating request - %v\n", key, err)
			strongFailed++
			continue
		}
		req.Header.Set("X-Consistency", "strong")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("STRONG GET %s: ERROR - %v\n", key, err)
			strongFailed++
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		statusStr := fmt.Sprintf("status: %d", resp.StatusCode)
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusInternalServerError {
				fmt.Printf("STRONG GET %s: QUORUM UNAVAILABLE (%s) - EXPECTED\n", key, statusStr)
			} else {
				fmt.Printf("STRONG GET %s: FAILED (%s)\n", key, statusStr)
			}
			strongFailed++
			continue
		}

		var result Response
		if err := json.Unmarshal(body, &result); err != nil {
			fmt.Printf("STRONG GET %s: ERROR parsing - %v\n", key, err)
			strongFailed++
			continue
		}

		if result.Value == expectedValue {
			fmt.Printf("STRONG GET %s: PASS (got: %s)\n", key, result.Value)
			strongPassed++
		} else {
			fmt.Printf("STRONG GET %s: FAIL (expected: %s, got: %s)\n", key, expectedValue, result.Value)
			strongFailed++
		}
	}

	fmt.Println("\n=== STRONG CONSISTENCY READ RESULTS ===")
	fmt.Printf("Passed: %d/%d\n", strongPassed, len(data))
	fmt.Printf("Failed: %d/%d\n", strongFailed, len(data))

	fmt.Println("\n=== ALL PHASES COMPLETE ===")
}
