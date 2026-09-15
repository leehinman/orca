package elastic

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// spanBucket is one bucket returned by the test server for log span queries.
type spanBucket struct {
	key   string
	minMs float64
	maxMs float64
}

// newSpanServer returns an ES client backed by a test server that returns
// successive sets of span buckets on each request. When responses is
// exhausted, subsequent requests return an empty bucket list.
func newSpanServer(t *testing.T, responses ...[]spanBucket) *elasticsearch.Client {
	t.Helper()
	var mu sync.Mutex
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		i := call
		call++
		mu.Unlock()
		var buckets []spanBucket
		if i < len(responses) {
			buckets = responses[i]
		}
		bucketsJSON := ""
		for j, b := range buckets {
			if j > 0 {
				bucketsJSON += ","
			}
			bucketsJSON += fmt.Sprintf(
				`{"key":%q,"doc_count":1,"min_ts":{"value":%f},"max_ts":{"value":%f}}`,
				b.key, b.minMs, b.maxMs,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprintf(w, `{"aggregations":{"groups":{"buckets":[%s]}}}`, bucketsJSON)
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

func bucket(key string, minMs, maxMs float64) spanBucket {
	return spanBucket{key: key, minMs: minMs, maxMs: maxMs}
}

func TestPollGroupedLogSpan_ImmediateSuccess(t *testing.T) {
	// span = 60s, minDuration = 1m → ready immediately
	es := newSpanServer(t, []spanBucket{bucket("agent0", 0, 60_000)})
	spans, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 1, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spans["agent0"] < time.Minute {
		t.Errorf("got %v, want >= 1m", spans["agent0"])
	}
}

func TestPollGroupedLogSpan_EventualSuccess(t *testing.T) {
	// first response has 30s span, second has 60s span
	es := newSpanServer(t,
		[]spanBucket{bucket("agent0", 0, 30_000)},
		[]spanBucket{bucket("agent0", 0, 60_000)},
	)
	spans, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 1, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spans["agent0"] < time.Minute {
		t.Errorf("got %v, want >= 1m", spans["agent0"])
	}
}

func TestPollGroupedLogSpan_MultipleGroups(t *testing.T) {
	es := newSpanServer(t, []spanBucket{
		bucket("agent0", 0, 60_000),
		bucket("agent1", 0, 90_000),
	})
	spans, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 2, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(spans) != 2 {
		t.Errorf("got %d groups, want 2", len(spans))
	}
}

func TestPollGroupedLogSpan_NotEnoughGroups(t *testing.T) {
	// only 1 group ready, need 2 → timeout
	es := newSpanServer(t, []spanBucket{bucket("agent0", 0, 60_000)})
	_, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 2, 50*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestPollGroupedLogSpan_SpanTooShort(t *testing.T) {
	// span = 30s, need 1m → timeout
	es := newSpanServer(t, []spanBucket{bucket("agent0", 0, 30_000)})
	_, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 1, 50*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestPollGroupedLogSpan_Timeout(t *testing.T) {
	es := newSpanServer(t) // always returns empty buckets
	_, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 1, 50*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestPollGroupedLogSpan_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	es := newSpanServer(t)
	_, err := PollGroupedLogSpan(ctx, es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 1, 5*time.Second, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestPollGroupedLogSpan_ESError(t *testing.T) {
	es := newErrorServer(t, http.StatusInternalServerError)
	_, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 1, 5*time.Second, time.Millisecond)
	if err == nil {
		t.Fatal("expected error for ES 500 response")
	}
}

// TestPollGroupedLogSpan_IndexNotFoundThenSuccess covers a data stream that
// doesn't exist until its first document arrives (e.g. a dynamic-dataset
// Custom Logs integration): ES 404s instead of returning zero hits, and that
// must be treated as "no documents yet" rather than a fatal error.
func TestPollGroupedLogSpan_IndexNotFoundThenSuccess(t *testing.T) {
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
		fmt.Fprint(w, `{"aggregations":{"groups":{"buckets":[{"key":"agent0","doc_count":1,"min_ts":{"value":0},"max_ts":{"value":60000}}]}}}`)
	}))
	t.Cleanup(srv.Close)
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{srv.URL}, MaxRetries: 0})
	if err != nil {
		t.Fatal(err)
	}
	spans, err := PollGroupedLogSpan(context.Background(), es, "logs-flog-default", time.Now(), nil, "agent.name", time.Minute, 1, 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spans["agent0"] < time.Minute {
		t.Errorf("got %v, want >= 1m", spans["agent0"])
	}
}

func TestPollGroupedLogSpan_TimeoutDescribesSpans(t *testing.T) {
	responses := make([][]spanBucket, 500)
	for i := range responses {
		responses[i] = []spanBucket{bucket("agent0", 0, 30_000), bucket("agent1", 0, 61_000)}
	}
	es := newSpanServer(t, responses...)
	_, err := PollGroupedLogSpan(context.Background(), es, "logs-*", time.Now(), nil, "agent.name", time.Minute, 2, 50*time.Millisecond, time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout")
	}
	for _, want := range []string{"1 of 2 groups reached 1m0s", "agent0=30s", "agent1=1m1s"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err.Error(), want)
		}
	}
}
