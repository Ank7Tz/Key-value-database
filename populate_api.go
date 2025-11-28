//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Payload struct {
	Value string `json:"value"`
}

func main() {
	baseURL := "http://localhost:8000/api/data"

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

	fmt.Println("\nDone! Populated 40 key-value pairs.")
}
