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
	normalNode := "localhost:8000"
	crashNode := "localhost:8002"
	writeURL := fmt.Sprintf("http://%s/api/data", normalNode)
	stopURL := fmt.Sprintf("http://%s/api/stop", crashNode)
	readURL := fmt.Sprintf("http://%s/api/data", crashNode)
	client := &http.Client{}

	fmt.Println("=== STOPPING NODE ===")
	resp, err := client.Get(stopURL)
	if err != nil {
		fmt.Printf("Error calling stop API: %v\n", err)
	} else {
		fmt.Printf("Stop API called (status: %d)\n", resp.StatusCode)
		resp.Body.Close()
	}
	fmt.Println()

	data := map[string]string{
		"tokyo":      "japan",
		"paris":      "france",
		"london":     "england",
		"berlin":     "germany",
		"rome":       "italy",
		"madrid":     "spain",
		"oslo":       "norway",
		"vienna":     "austria",
		"prague":     "czech",
		"dublin":     "ireland",
		"coffee":     "morning",
		"tea":        "afternoon",
		"pizza":      "dinner",
		"salad":      "lunch",
		"cake":       "dessert",
		"redis":      "cache",
		"postgres":   "database",
		"nginx":      "proxy",
		"docker":     "container",
		"kubernetes": "orchestration",
	}

	fmt.Println("=== PHASE 1: POPULATING DATA ===")
	for key, value := range data {
		url := fmt.Sprintf("%s/%s", writeURL, key)
		payload := Payload{Value: value}
		jsonData, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("Error marshaling JSON for %s: %v\n", key, err)
			continue
		}

		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error creating request for %s: %v\n", key, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error sending request for %s: %v\n", key, err)
			continue
		}
		resp.Body.Close()
		fmt.Printf("PUT %s -> %s (status: %d)\n", key, value, resp.StatusCode)
	}
	fmt.Println("\nDone populating 20 key-value pairs.")

	fmt.Println("\n=== Press ENTER to start read tests ===")
	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')

	fmt.Println("=== PHASE 2: READ TESTS ===")
	passed := 0
	failed := 0

	for key, expectedValue := range data {
		url := fmt.Sprintf("%s/%s", readURL, key)

		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("GET %s: ERROR - %v\n", key, err)
			failed++
			continue
		}

		body, err := io.ReadAll(resp.Body)
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

	fmt.Println("\n=== RESULTS ===")
	fmt.Printf("Passed: %d/%d\n", passed, len(data))
	fmt.Printf("Failed: %d/%d\n", failed, len(data))

	fmt.Println("\n=== PHASE 3: DELETING DATA ===")
	deleted := 0
	deleteFailed := 0

	for key := range data {
		url := fmt.Sprintf("%s/%s", readURL, key)

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

}
