//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Payload struct {
	Value string `json:"value"`
}

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Result struct {
	Key      string
	Success  bool
	Duration time.Duration
	Error    string
}

type Stats struct {
	Results       []Result
	WallClockTime time.Duration
}

func main() {
	nodes := []string{
		"localhost:8000",
		"localhost:8001",
		"localhost:8002",
		"localhost:8003",
		"localhost:8004",
	}

	// Generate test data
	data := generateTestData(500)

	fmt.Println("=== PERFORMANCE TEST ===")
	fmt.Printf("Nodes: %d\n", len(nodes))
	fmt.Printf("Keys: %d\n", len(data))
	fmt.Println()

	// Phase 1: Parallel Writes
	fmt.Println("=== PHASE 1: PARALLEL WRITES (Round Robin) ===")
	writeStats := parallelWrites(nodes, data)
	printStats("Write", writeStats)

	// Small delay between phases
	time.Sleep(500 * time.Millisecond)

	// Phase 2: Parallel Reads
	fmt.Println("\n=== PHASE 2: PARALLEL READS (Round Robin) ===")
	readStats := parallelReads(nodes, data)
	printStats("Read", readStats)

	// Phase 3: Cleanup
	fmt.Println("\n=== PHASE 3: CLEANUP ===")
	deleteStats := parallelDeletes(nodes, data)
	printStats("Delete", deleteStats)

	fmt.Println("\n=== TEST COMPLETE ===")
}

func generateTestData(count int) map[string]string {
	data := make(map[string]string)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%04d", i)
		value := fmt.Sprintf("value_%04d", i)
		data[key] = value
	}
	return data
}

func parallelWrites(nodes []string, data map[string]string) Stats {
	var wg sync.WaitGroup
	results := make([]Result, 0, len(data))
	resultChan := make(chan Result, len(data))

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	startTime := time.Now()

	for i, key := range keys {
		wg.Add(1)
		node := nodes[i%len(nodes)]
		value := data[key]

		go func(node, key, value string) {
			defer wg.Done()
			result := doWrite(node, key, value)
			resultChan <- result
		}(node, key, value)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		results = append(results, result)
	}

	wallClockTime := time.Since(startTime)

	return Stats{Results: results, WallClockTime: wallClockTime}
}

func parallelReads(nodes []string, data map[string]string) Stats {
	var wg sync.WaitGroup
	results := make([]Result, 0, len(data))
	resultChan := make(chan Result, len(data))

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	startTime := time.Now()

	for i, key := range keys {
		wg.Add(1)
		node := nodes[i%len(nodes)]
		expectedValue := data[key]

		go func(node, key, expectedValue string) {
			defer wg.Done()
			result := doRead(node, key, expectedValue)
			resultChan <- result
		}(node, key, expectedValue)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		results = append(results, result)
	}

	wallClockTime := time.Since(startTime)

	return Stats{Results: results, WallClockTime: wallClockTime}
}

func parallelDeletes(nodes []string, data map[string]string) Stats {
	var wg sync.WaitGroup
	results := make([]Result, 0, len(data))
	resultChan := make(chan Result, len(data))

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	startTime := time.Now()

	for i, key := range keys {
		wg.Add(1)
		node := nodes[i%len(nodes)]

		go func(node, key string) {
			defer wg.Done()
			result := doDelete(node, key)
			resultChan <- result
		}(node, key)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		results = append(results, result)
	}

	wallClockTime := time.Since(startTime)

	return Stats{Results: results, WallClockTime: wallClockTime}
}

func doWrite(node, key, value string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s", node, key)

	payload := Payload{Value: value}
	jsonData, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: err.Error()}
	}
	defer resp.Body.Close()

	success := resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated
	return Result{Key: key, Success: success, Duration: time.Since(start)}
}

func doRead(node, key, expectedValue string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s", node, key)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("status: %d", resp.StatusCode)}
	}

	body, _ := io.ReadAll(resp.Body)
	var result Response
	if err := json.Unmarshal(body, &result); err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: "parse error"}
	}

	success := result.Value == expectedValue
	return Result{Key: key, Success: success, Duration: time.Since(start)}
}

func doDelete(node, key string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s", node, key)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: err.Error()}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: err.Error()}
	}
	defer resp.Body.Close()

	success := resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent
	return Result{Key: key, Success: success, Duration: time.Since(start)}
}

func printStats(operation string, stats Stats) {
	results := stats.Results
	var successCount, failCount int
	var minDuration, maxDuration time.Duration
	var totalLatency time.Duration

	minDuration = time.Hour

	for _, r := range results {
		if r.Success {
			successCount++
		} else {
			failCount++
		}
		totalLatency += r.Duration
		if r.Duration < minDuration {
			minDuration = r.Duration
		}
		if r.Duration > maxDuration {
			maxDuration = r.Duration
		}
	}

	avgLatency := totalLatency / time.Duration(len(results))
	throughput := float64(len(results)) / stats.WallClockTime.Seconds()

	fmt.Printf("\n%s Statistics:\n", operation)
	fmt.Printf("  Total:          %d\n", len(results))
	fmt.Printf("  Success:        %d\n", successCount)
	fmt.Printf("  Failed:         %d\n", failCount)
	fmt.Printf("  Min Latency:    %v\n", minDuration)
	fmt.Printf("  Max Latency:    %v\n", maxDuration)
	fmt.Printf("  Avg Latency:    %v\n", avgLatency)
	fmt.Printf("  Wall Clock:     %v\n", stats.WallClockTime)
	fmt.Printf("  Throughput:     %.2f ops/sec\n", throughput)
}
