//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Payload struct {
	Value string `json:"value"`
}

func main() {
	baseURL := "http://10.200.125.75:18000/api/data"
	// baseURL := "http://localhost:8000/api/data"

	data := map[string]string{
		"alice":    "engineer",
		"bob":      "designer",
		"charlie":  "manager",
		"diana":    "analyst",
		"edward":   "developer",
		"fiona":    "architect",
		"george":   "consultant",
		"hannah":   "director",
		"ivan":     "scientist",
		"julia":    "researcher",
		"kevin":    "administrator",
		"laura":    "coordinator",
		"michael":  "specialist",
		"nancy":    "supervisor",
		"oscar":    "technician",
		"patricia": "strategist",
		"quinn":    "planner",
		"robert":   "executive",
		"sarah":    "accountant",
		"thomas":   "marketer",
		"umbrella": "protection",
		"violet":   "purple",
		"william":  "conqueror",
		"xena":     "warrior",
		"yolanda":  "sunshine",
		"zachary":  "remembered",
		"apple":    "fruit",
		"banana":   "yellow",
		"cherry":   "red",
		"dragon":   "mythical",
		"elephant": "large",
		"falcon":   "swift",
		"guitar":   "instrument",
		"horizon":  "distant",
		"island":   "isolated",
		"jungle":   "dense",
		"kingdom":  "royal",
		"lantern":  "light",
		"mountain": "tall",
		"neptune":  "ocean",
	}

	client := &http.Client{}

	fmt.Println("=== PHASE 1: POPULATING DATA ===")
	for key, value := range data {
		url := fmt.Sprintf("%s/%s", baseURL, key)
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
	fmt.Println("\nDone populating 40 key-value pairs.")

	fmt.Println("=== PHASE 2a: READING & VERIFYING DATA (STRONG CONSISTENCY) ===")
	strongPassed, strongFailed := 0, 0

	for key, expectedValue := range data {
		url := fmt.Sprintf("%s/%s?strong_consistency=true", baseURL, key)

		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("FAIL: GET %s - error: %v\n", key, err)
			strongFailed++
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("FAIL: GET %s - read error: %v\n", key, err)
			strongFailed++
			continue
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("FAIL: GET %s - status: %d\n", key, resp.StatusCode)
			strongFailed++
			continue
		}

		var result Payload
		if err := json.Unmarshal(body, &result); err != nil {
			if string(body) == expectedValue {
				fmt.Printf("PASS: %s = %s (strong)\n", key, expectedValue)
				strongPassed++
				continue
			}
			fmt.Printf("FAIL: GET %s - parse error: %v (body: %s)\n", key, err, string(body))
			strongFailed++
			continue
		}

		if result.Value == expectedValue {
			fmt.Printf("PASS: %s = %s (strong)\n", key, expectedValue)
			strongPassed++
		} else {
			fmt.Printf("FAIL: %s expected %s, got %s (strong)\n", key, expectedValue, result.Value)
			strongFailed++
		}
	}

	fmt.Printf("\nStrong Read Results - Passed: %d, Failed: %d\n\n", strongPassed, strongFailed)

	fmt.Println("=== PHASE 2b: READING & VERIFYING DATA (stale semantic read) ===")
	passed, failed := 0, 0

	for key, expectedValue := range data {
		url := fmt.Sprintf("%s/%s", baseURL, key)

		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("FAIL: GET %s - error: %v\n", key, err)
			failed++
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("FAIL: GET %s - read error: %v\n", key, err)
			failed++
			continue
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("FAIL: GET %s - status: %d\n", key, resp.StatusCode)
			failed++
			continue
		}

		var result Payload
		if err := json.Unmarshal(body, &result); err != nil {
			if string(body) == expectedValue {
				fmt.Printf("PASS: %s = %s\n", key, expectedValue)
				passed++
				continue
			}
			fmt.Printf("FAIL: GET %s - parse error: %v (body: %s)\n", key, err, string(body))
			failed++
			continue
		}

		if result.Value == expectedValue {
			fmt.Printf("PASS: %s = %s\n", key, expectedValue)
			passed++
		} else {
			fmt.Printf("FAIL: %s expected %s, got %s\n", key, expectedValue, result.Value)
			failed++
		}
	}

	fmt.Printf("\nstale semantics Read Results - Passed: %d, Failed: %d\n\n", passed, failed)

	fmt.Println("=== PHASE 3: DELETING DATA ===")
	deleted, deleteFailed := 0, 0

	for key := range data {
		url := fmt.Sprintf("%s/%s", baseURL, key)

		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			fmt.Printf("Error creating delete request for %s: %v\n", key, err)
			deleteFailed++
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("FAIL: DELETE %s - error: %v\n", key, err)
			deleteFailed++
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
			fmt.Printf("DELETE %s (status: %d)\n", key, resp.StatusCode)
			deleted++
		} else {
			fmt.Printf("FAIL: DELETE %s - status: %d\n", key, resp.StatusCode)
			deleteFailed++
		}
	}

	fmt.Printf("\nDelete Results - Deleted: %d, Failed: %d\n\n", deleted, deleteFailed)

	time.Sleep(2 * time.Second)

	fmt.Println("=== PHASE 4: VERIFYING DELETION ===")
	confirmed, notDeleted := 0, 0

	for key := range data {
		url := fmt.Sprintf("%s/%s", baseURL, key)

		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("CONFIRMED: %s - connection error (deleted)\n", key)
			confirmed++
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound ||
			strings.Contains(string(body), "key not found") ||
			strings.Contains(string(body), "not found") {
			fmt.Printf("CONFIRMED: %s deleted\n", key)
			confirmed++
		} else {
			fmt.Printf("NOT DELETED: %s still exists (status: %d, body: %s)\n", key, resp.StatusCode, string(body))
			notDeleted++
		}
	}

	fmt.Printf("\n=== FINAL RESULTS ===\n")
	fmt.Printf("Phase 2  - Eventual Read:  Passed: %d, Failed: %d\n", passed, failed)
	fmt.Printf("Phase 2b - Strong Read:    Passed: %d, Failed: %d\n", strongPassed, strongFailed)
	fmt.Printf("Phase 3  - Delete:         Deleted: %d, Failed: %d\n", deleted, deleteFailed)
	fmt.Printf("Phase 4  - Verify Delete:  Confirmed: %d, Not Deleted: %d\n", confirmed, notDeleted)
}
