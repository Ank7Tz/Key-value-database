//go:build ignore

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Payload struct {
	Value string `json:"value"`
}

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	node1 := "10.200.125.75:18000"
	node2 := "10.200.125.76:18001"
	node3 := "10.200.125.77:18002"
	node4 := "10.200.125.78:18003"
	node5 := "10.200.125.79:18004"

	// node1 := "localhost:8000"
	// node2 := "localhost:8001"
	// node3 := "localhost:8002"
	// node4 := "localhost:8003"
	// node5 := "localhost:8004"

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

	fmt.Println("=== PHASE 1: STOPPING NODES 2, 3, 4 ===")
	nodesToStop := []string{node2, node3, node4}
	for _, node := range nodesToStop {
		stopURL := fmt.Sprintf("http://%s/api/stop", node)
		resp, err := client.Get(stopURL)
		if err != nil {
			fmt.Printf("Error stopping %s: %v\n", node, err)
		} else {
			fmt.Printf("Stopped %s (status: %d)\n", node, resp.StatusCode)
			resp.Body.Close()
		}
	}
	fmt.Println()

	fmt.Println("=== PHASE 2: WRITING DATA TO NODE 1 ===")
	baseURL := fmt.Sprintf("http://%s/api/data", node1)
	for key, value := range data {
		url := fmt.Sprintf("%s/%s", baseURL, key)
		payload := Payload{Value: value}
		jsonData, _ := json.Marshal(payload)

		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error creating request for %s: %v\n", key, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error writing %s: %v\n", key, err)
			continue
		}
		resp.Body.Close()
		fmt.Printf("PUT %s -> %s (status: %d)\n", key, value, resp.StatusCode)
	}
	fmt.Println("\nDone writing 8 key-value pairs to node1.")

	fmt.Println("\n=== Press ENTER to start read tests ===")
	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')

	fmt.Println("=== PHASE 3: READING DATA FROM NODE 1 ===")
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
			fmt.Printf("GET %s: ERROR parsing response - %v\n", key, err)
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

	fmt.Println("\n=== READ RESULTS ===")
	fmt.Printf("Passed: %d/%d\n", passed, len(data))
	fmt.Printf("Failed: %d/%d\n", failed, len(data))

	fmt.Println("\n=== PHASE 4: DELETING DATA USING NODE 5 ===")
	deleteBaseURL := fmt.Sprintf("http://%s/api/data", node5)
	deleted := 0
	deleteFailed := 0

	for key := range data {
		url := fmt.Sprintf("%s/%s", deleteBaseURL, key)

		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			fmt.Printf("DELETE %s: ERROR creating request - %v\n", key, err)
			deleteFailed++
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("DELETE %s: ERROR - %v\n", key, err)
			deleteFailed++
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
			fmt.Printf("DELETE %s: OK (status: %d)\n", key, resp.StatusCode)
			deleted++
		} else {
			fmt.Printf("DELETE %s: FAILED (status: %d)\n", key, resp.StatusCode)
			deleteFailed++
		}
	}

	fmt.Println("\n=== DELETE RESULTS ===")
	fmt.Printf("Deleted: %d/%d\n", deleted, len(data))
	fmt.Printf("Failed: %d/%d\n", deleteFailed, len(data))

	fmt.Println("\n=== ALL PHASES COMPLETE ===")
}
