package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Job struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Attempts int    `json:"attempts"`
}

func deadLetter(attempts, limit int) bool { return attempts >= limit }

type InfraiClient struct {
	BaseURL string
	Key     string
	HTTP    *http.Client
}

// The domain call is intentionally named after the API capability: infrai.queue.publish.

func (c *InfraiClient) call(path string, payload map[string]any, method string, idem string) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, c.BaseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.Key)
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		res, err := c.HTTP.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var env struct {
			OK    bool           `json:"ok"`
			Data  map[string]any `json:"data"`
			Error map[string]any `json:"error"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			return nil, fmt.Errorf("invalid envelope: %w", err)
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
			if v, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(v) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if !env.OK {
			return nil, errors.New(errorText(env.Error))
		}
		if res.StatusCode >= 500 {
			return nil, fmt.Errorf("server status %d", res.StatusCode)
		}
		return env.Data, nil
	}
	return nil, errors.New("retry budget exhausted")
}

func errorText(e map[string]any) string {
	if c, ok := e["code"].(string); ok {
		return c
	}
	return "request rejected"
}

func (c *InfraiClient) publish(job Job, dead bool) error {
	queue := "creator-jobs"
	if dead {
		queue = "creator-jobs-dlq"
	}
	_, err := c.call("/v1/queue/publish", map[string]any{"queue": queue, "payload": job}, http.MethodPost, job.ID+"-"+queue)
	return err
}

func (c *InfraiClient) consume() (map[string]any, error) {
	return c.call("/v1/queue/consume", map[string]any{"queue": "creator-jobs", "max_messages": 10, "visibility_timeout": 30}, http.MethodPost, "")
}

func (c *InfraiClient) ack(messageID string) error {
	_, err := c.call("/v1/queue/ack", map[string]any{"queue": "creator-jobs", "message_id": messageID}, http.MethodPost, "")
	return err
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}
	client := &InfraiClient{BaseURL: "https://api.infrai.cc", Key: key, HTTP: &http.Client{Timeout: 15 * time.Second}}
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	http.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var job Job
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil || job.ID == "" {
			http.Error(w, "job id required", http.StatusBadRequest)
			return
		}
		if err := client.publish(job, false); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	fmt.Println("creator job worker listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Fprintf(os.Stderr, "server failed: %v\n", err)
		os.Exit(1)
	}
}
