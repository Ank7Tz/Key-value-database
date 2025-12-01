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

var clientPoolSizes = []int{16, 32, 64, 128}

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

type WriteJob struct {
	Node  string
	Key   string
	Value string
}

type ReadJob struct {
	Node          string
	Key           string
	ExpectedValue string
}

type DeleteJob struct {
	Node string
	Key  string
}

func main() {
	nodes := []string{
		"10.200.125.75:18000",
		"10.200.125.76:18001",
		"10.200.125.77:18002",
		"10.200.125.78:18003",
		"10.200.125.79:18004",
	}

	data := generateTestData(500)

	fmt.Println("=== PERFORMANCE TEST ===")
	fmt.Printf("Nodes: %d\n", len(nodes))
	fmt.Printf("Keys: %d\n", len(data))
	fmt.Printf("Worker Pool Sizes: %v\n", clientPoolSizes)
	fmt.Println()

	for _, poolSize := range clientPoolSizes {
		fmt.Printf("\n%s\n", repeatChar('=', 60))
		fmt.Printf("=== TESTING WITH POOL SIZE: %d ===\n", poolSize)
		fmt.Printf("%s\n", repeatChar('=', 60))

		fmt.Println("\n=== PHASE 1: PARALLEL WRITES (Round Robin) ===")
		writeStats := parallelWrites(nodes, data, poolSize)
		printStats("Write", writeStats)

		time.Sleep(2 * time.Second)

		fmt.Println("\n=== PHASE 2: PARALLEL READS (Round Robin) ===")
		readStats := parallelReads(nodes, data, poolSize)
		printStats("Read", readStats)

		fmt.Println("\n=== PHASE 3: PARALLEL CONSISTENT READS (Round Robin) ===")
		consistentReadStats := parallelConsistentReads(nodes, data, poolSize)
		printStats("Consistent Read", consistentReadStats)

		fmt.Println("\n=== PHASE 3: PARALLEL DELETES (Round Robin) ===")
		deleteStats := parallelDeletes(nodes, data, poolSize)
		printStats("Delete", deleteStats)

		fmt.Printf("\n=== POOL SIZE %d COMPLETE ===\n", poolSize)

		if poolSize != clientPoolSizes[len(clientPoolSizes)-1] {
			time.Sleep(10 * time.Second)
		}
	}

	fmt.Println("\n=== ALL TESTS COMPLETE ===")
}

func repeatChar(c rune, n int) string {
	result := make([]rune, n)
	for i := range result {
		result[i] = c
	}
	return string(result)
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

func parallelWrites(nodes []string, data map[string]string, poolSize int) Stats {
	jobs := make(chan WriteJob, len(data))
	results := make(chan Result, len(data))

	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- doWrite(job.Node, job.Key, job.Value)
			}
		}()
	}

	startTime := time.Now()

	i := 0
	for key, value := range data {
		jobs <- WriteJob{Node: nodes[i%len(nodes)], Key: key, Value: value}
		i++
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []Result
	for r := range results {
		allResults = append(allResults, r)
	}

	return Stats{Results: allResults, WallClockTime: time.Since(startTime)}
}

func parallelReads(nodes []string, data map[string]string, poolSize int) Stats {
	jobs := make(chan ReadJob, len(data))
	results := make(chan Result, len(data))

	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- doRead(job.Node, job.Key, job.ExpectedValue)
			}
		}()
	}

	startTime := time.Now()

	i := 0
	for key, value := range data {
		jobs <- ReadJob{Node: nodes[i%len(nodes)], Key: key, ExpectedValue: value}
		i++
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []Result
	for r := range results {
		allResults = append(allResults, r)
	}

	return Stats{Results: allResults, WallClockTime: time.Since(startTime)}
}

func parallelConsistentReads(nodes []string, data map[string]string, poolSize int) Stats {
	jobs := make(chan ReadJob, len(data))
	results := make(chan Result, len(data))

	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- doConsistentReads(job.Node, job.Key, job.ExpectedValue)
			}
		}()
	}

	startTime := time.Now()

	i := 0
	for key, value := range data {
		jobs <- ReadJob{Node: nodes[i%len(nodes)], Key: key, ExpectedValue: value}
		i++
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []Result
	for r := range results {
		allResults = append(allResults, r)
	}

	return Stats{Results: allResults, WallClockTime: time.Since(startTime)}
}

func parallelDeletes(nodes []string, data map[string]string, poolSize int) Stats {
	jobs := make(chan DeleteJob, len(data))
	results := make(chan Result, len(data))

	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- doDelete(job.Node, job.Key)
			}
		}()
	}

	startTime := time.Now()

	i := 0
	for key := range data {
		jobs <- DeleteJob{Node: nodes[i%len(nodes)], Key: key}
		i++
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var allResults []Result
	for r := range results {
		allResults = append(allResults, r)
	}

	return Stats{Results: allResults, WallClockTime: time.Since(startTime)}
}

func doWrite(node, key, value string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s", node, key)

	payload := Payload{Value: value}
	jsonData, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("request_error: %v", err)}
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("connection_error: %v", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("http_%d: %s", resp.StatusCode, string(body))}
	}

	return Result{Key: key, Success: true, Duration: time.Since(start)}
}

func doRead(node, key, expectedValue string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s", node, key)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("connection_error: %v", err)}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("http_%d: %s", resp.StatusCode, string(body))}
	}

	var result Response
	if err := json.Unmarshal(body, &result); err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("parse_error: %v", err)}
	}

	if result.Value != expectedValue {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("value_mismatch: expected=%s, got=%s", expectedValue, result.Value)}
	}

	return Result{Key: key, Success: true, Duration: time.Since(start)}
}

func doConsistentReads(node, key, expectedValue string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s?strong_consistency=true", node, key)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("connection_error: %v", err)}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("http_%d: %s", resp.StatusCode, string(body))}
	}

	var result Response
	if err := json.Unmarshal(body, &result); err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("parse_error: %v", err)}
	}

	if result.Value != expectedValue {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("value_mismatch: expected=%s, got=%s", expectedValue, result.Value)}
	}

	return Result{Key: key, Success: true, Duration: time.Since(start)}
}

func doDelete(node, key string) Result {
	start := time.Now()
	url := fmt.Sprintf("http://%s/api/data/%s", node, key)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("request_error: %v", err)}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("connection_error: %v", err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return Result{Key: key, Success: false, Duration: time.Since(start), Error: fmt.Sprintf("http_%d: %s", resp.StatusCode, string(body))}
	}

	return Result{Key: key, Success: true, Duration: time.Since(start)}
}

func printStats(operation string, stats Stats) {
	results := stats.Results
	var successCount, failCount int
	var minDuration, maxDuration time.Duration
	var totalLatency time.Duration

	minDuration = time.Hour
	errorCounts := make(map[string]int)

	for _, r := range results {
		if r.Success {
			successCount++
		} else {
			failCount++
			errorType := categorizeError(r.Error)
			errorCounts[errorType]++
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

	if failCount > 0 {
		fmt.Printf("\n  Error Breakdown:\n")
		for errType, count := range errorCounts {
			percentage := float64(count) / float64(failCount) * 100
			fmt.Printf("    %-20s %d (%.1f%%)\n", errType+":", count, percentage)
		}
	}
}

func categorizeError(err string) string {
	if len(err) == 0 {
		return "unknown"
	}

	prefixes := []string{
		"connection_error", "request_error", "parse_error", "value_mismatch",
		"http_400", "http_401", "http_403", "http_404",
		"http_500", "http_502", "http_503", "http_504",
	}

	for _, prefix := range prefixes {
		if len(err) >= len(prefix) && err[:len(prefix)] == prefix {
			return prefix
		}
	}

	if len(err) >= 5 && err[:5] == "http_" {
		for i := 5; i < len(err); i++ {
			if err[i] == ':' || err[i] == ' ' {
				return err[:i]
			}
		}
		if len(err) >= 8 {
			return err[:8]
		}
	}

	return "other"
}
