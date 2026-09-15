package elastic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// newCountServer returns an ES client backed by a test server that returns
// successive count values from counts on each request. When counts is
// exhausted, subsequent requests return 0.
func newCountServer(t *testing.T, counts ...int64) *elasticsearch.Client {
	t.Helper()
	var mu sync.Mutex
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		i := call
		call++
		mu.Unlock()
		c := int64(0)
		if i < len(counts) {
			c = counts[i]
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprintf(w, `{"count":%d,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`, c)
	}))
	t.Cleanup(srv.Close)
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:  []string{srv.URL},
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return es
}

func newErrorServer(t *testing.T, status int) *elasticsearch.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"type":"server_error","reason":"test"}}`)
	}))
	t.Cleanup(srv.Close)
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:  []string{srv.URL},
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return es
}

func TestPollLogCount_ImmediateSuccess(t *testing.T) {
	es := newCountServer(t, 48000)
	n, err := PollLogCount(context.Background(), es, "logs-*", time.Now(), nil, 48000, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 48000 {
		t.Errorf("got %d, want 48000", n)
	}
}

func TestPollLogCount_EventualSuccess(t *testing.T) {
	es := newCountServer(t, 0, 10000, 48000)
	n, err := PollLogCount(context.Background(), es, "logs-*", time.Now(), nil, 48000, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 48000 {
		t.Errorf("got %d, want 48000", n)
	}
}

func TestPollLogCount_ActualExceedsExpected(t *testing.T) {
	es := newCountServer(t, 50000)
	n, err := PollLogCount(context.Background(), es, "logs-*", time.Now(), nil, 48000, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 50000 {
		t.Errorf("got %d, want 50000", n)
	}
}

func TestPollLogCount_Timeout(t *testing.T) {
	es := newCountServer(t) // always returns 0
	_, err := PollLogCount(context.Background(), es, "logs-*", time.Now(), nil, 48000, 50*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestPollLogCount_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	es := newCountServer(t, 0)
	_, err := PollLogCount(ctx, es, "logs-*", time.Now(), nil, 48000, 5*time.Second, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestPollLogCount_ESError(t *testing.T) {
	es := newErrorServer(t, http.StatusInternalServerError)
	_, err := PollLogCount(context.Background(), es, "logs-*", time.Now(), nil, 48000, 5*time.Second, time.Millisecond)
	if err == nil {
		t.Fatal("expected error for ES 500 response")
	}
}

// TestPollLogCount_IndexNotFoundThenSuccess covers a data stream that doesn't
// exist until its first document arrives (e.g. a dynamic-dataset Custom Logs
// integration): ES 404s instead of returning zero hits, and that must be
// treated as "no documents yet" rather than a fatal error.
func TestPollLogCount_IndexNotFoundThenSuccess(t *testing.T) {
	var mu sync.Mutex
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		i := call
		call++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		if i == 0 {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"type":"index_not_found_exception","reason":"no such index"},"status":404}`)
			return
		}
		fmt.Fprint(w, `{"count":48000,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
	}))
	t.Cleanup(srv.Close)
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})
	if err != nil {
		t.Fatal(err)
	}
	n, err := PollLogCount(context.Background(), es, "logs-flog-default", time.Now(), nil, 48000, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 48000 {
		t.Errorf("got %d, want 48000", n)
	}
}

func TestPollLogCount_QueryContainsTimestamp(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprintln(w, `{"count":100,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
	}))
	t.Cleanup(srv.Close)
	es, _ := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})

	since := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	PollLogCount(context.Background(), es, "logs-*", since, nil, 100, 5*time.Second, time.Millisecond) //nolint

	if !strings.Contains(string(captured), "2024-01-15T12:00:00Z") {
		t.Errorf("query body missing timestamp, got: %s", captured)
	}
}

func TestPollLogCount_WildcardFilter(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprintln(w, `{"count":100,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
	}))
	t.Cleanup(srv.Close)
	es, _ := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})

	filter := map[string]string{"log.file.path": "/var/log/containers/emitter-*"}
	PollLogCount(context.Background(), es, "logs-*", time.Now(), filter, 100, 5*time.Second, time.Millisecond) //nolint

	body := string(captured)
	if !strings.Contains(body, "wildcard") {
		t.Errorf("expected wildcard query, got: %s", body)
	}
	if strings.Contains(body, `"term"`) {
		t.Errorf("unexpected term query for wildcard value, got: %s", body)
	}
}

func TestPollLogCount_QueryContainsRangeFilter(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprintln(w, `{"count":100,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`)
	}))
	t.Cleanup(srv.Close)
	es, _ := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})

	PollLogCount(context.Background(), es, "logs-*", time.Now(), nil, 100, 5*time.Second, time.Millisecond) //nolint

	body := string(captured)
	for _, want := range []string{"range", "@timestamp", "gte"} {
		if !strings.Contains(body, want) {
			t.Errorf("query missing %q, got: %s", want, body)
		}
	}
}
