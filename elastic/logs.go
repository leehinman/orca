package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// PollLogCount polls the ES _count API until the document count in index
// with @timestamp >= since reaches expected, or timeout elapses.
// filter maps ES field names to values; values containing * or ? use wildcard
// queries, all others use term queries.
// Returns the actual count when the condition is satisfied.
func PollLogCount(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since time.Time,
	filter map[string]string,
	expected int,
	timeout, interval time.Duration,
) (int64, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var actual int64
	err := waitUntil(pollCtx, interval, func() (bool, error) {
		n, err := countDocs(pollCtx, es, index, since, filter)
		if err != nil {
			return false, err
		}
		actual = n
		return n >= int64(expected), nil
	})
	return actual, err
}

func waitUntil(ctx context.Context, interval time.Duration, fn func() (bool, error)) error {
	for {
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// PollGroupedLogCount polls until at least expectedGroups distinct values of
// groupByField each have >= expectedPerGroup documents since the given time.
// Returns a map of group value -> doc count when the condition is satisfied.
func PollGroupedLogCount(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since time.Time,
	filter map[string]string,
	groupByField string,
	expectedPerGroup int,
	expectedGroups int,
	timeout, interval time.Duration,
) (map[string]int64, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var counts map[string]int64
	err := waitUntil(pollCtx, interval, func() (bool, error) {
		buckets, err := countDocsByGroup(pollCtx, es, index, since, filter, groupByField)
		if err != nil {
			return false, err
		}
		ready := 0
		for _, n := range buckets {
			if n >= int64(expectedPerGroup) {
				ready++
			}
		}
		counts = buckets
		return ready >= expectedGroups, nil
	})
	return counts, err
}

// PollGroupedLogSpan polls until at least expectedGroups distinct values of
// groupByField each have a log span (max @timestamp − min @timestamp) >=
// minDuration since the given time. Returns a map of group → span when the
// condition is satisfied.
func PollGroupedLogSpan(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since time.Time,
	filter map[string]string,
	groupByField string,
	minDuration time.Duration,
	expectedGroups int,
	timeout, interval time.Duration,
) (map[string]time.Duration, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var spans map[string]time.Duration
	ready := 0
	err := waitUntil(pollCtx, interval, func() (bool, error) {
		buckets, err := logSpanByGroup(pollCtx, es, index, since, filter, groupByField)
		if err != nil {
			return false, err
		}
		ready = 0
		for _, d := range buckets {
			if d >= minDuration {
				ready++
			}
		}
		spans = buckets
		return ready >= expectedGroups, nil
	})
	if err != nil {
		return spans, fmt.Errorf("%w (%d of %d groups reached %s; observed: %s)", err, ready, expectedGroups, minDuration, describeSpans(spans))
	}
	return spans, nil
}

func describeSpans(spans map[string]time.Duration) string {
	if len(spans) == 0 {
		return "no documents"
	}
	keys := make([]string, 0, len(spans))
	for k := range spans {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + spans[k].Round(time.Second).String()
	}
	return strings.Join(parts, ", ")
}

func logSpanByGroup(ctx context.Context, es *elasticsearch.Client, index string, since time.Time, filter map[string]string, groupByField string) (map[string]time.Duration, error) {
	q := map[string]interface{}{
		"size":  0,
		"query": map[string]interface{}{"bool": map[string]interface{}{"filter": buildBoolFilter(since, nil, filter, true)}},
		"aggs": map[string]interface{}{
			"groups": map[string]interface{}{
				"terms": map[string]interface{}{"field": groupByField, "size": 1000},
				"aggs": map[string]interface{}{
					"min_ts": map[string]interface{}{"min": map[string]interface{}{"field": "@timestamp"}},
					"max_ts": map[string]interface{}{"max": map[string]interface{}{"field": "@timestamp"}},
				},
			},
		},
	}
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	resp, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer resp.Body.Close()
	// A data stream that hasn't received its first document yet (e.g. a
	// dynamic-dataset Custom Logs integration before the agent ships
	// anything) doesn't exist, and ES 404s rather than returning zero hits;
	// treat that the same as "no documents yet" so pollers keep waiting
	// instead of failing immediately.
	if resp.StatusCode == 404 {
		return map[string]time.Duration{}, nil
	}
	if resp.IsError() {
		return nil, fmt.Errorf("search: %s", resp.Status())
	}
	var result struct {
		Aggregations struct {
			Groups struct {
				Buckets []struct {
					Key   string `json:"key"`
					MinTs struct {
						Value *float64 `json:"value"`
					} `json:"min_ts"`
					MaxTs struct {
						Value *float64 `json:"value"`
					} `json:"max_ts"`
				} `json:"buckets"`
			} `json:"groups"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}
	out := make(map[string]time.Duration, len(result.Aggregations.Groups.Buckets))
	for _, b := range result.Aggregations.Groups.Buckets {
		if b.MinTs.Value == nil || b.MaxTs.Value == nil {
			out[b.Key] = 0
			continue
		}
		spanMs := *b.MaxTs.Value - *b.MinTs.Value
		out[b.Key] = time.Duration(spanMs) * time.Millisecond
	}
	return out, nil
}

func countDocsByGroup(ctx context.Context, es *elasticsearch.Client, index string, since time.Time, filter map[string]string, groupByField string) (map[string]int64, error) {
	return countByGroup(ctx, es, index, since, nil, filter, groupByField)
}

func countByGroup(ctx context.Context, es *elasticsearch.Client, index string, since time.Time, until *time.Time, filter map[string]string, groupByField string) (map[string]int64, error) {
	q := map[string]interface{}{
		"size":  0,
		"query": map[string]interface{}{"bool": map[string]interface{}{"filter": buildBoolFilter(since, until, filter, true)}},
		"aggs": map[string]interface{}{
			"groups": map[string]interface{}{
				"terms": map[string]interface{}{"field": groupByField, "size": 1000},
			},
		},
	}
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	resp, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return map[string]int64{}, nil
	}
	if resp.IsError() {
		return nil, fmt.Errorf("search: %s", resp.Status())
	}
	var result struct {
		Aggregations struct {
			Groups struct {
				Buckets []struct {
					Key      string `json:"key"`
					DocCount int64  `json:"doc_count"`
				} `json:"buckets"`
			} `json:"groups"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}
	out := make(map[string]int64, len(result.Aggregations.Groups.Buckets))
	for _, b := range result.Aggregations.Groups.Buckets {
		out[b.Key] = b.DocCount
	}
	return out, nil
}

func countDocs(ctx context.Context, es *elasticsearch.Client, index string, since time.Time, filter map[string]string) (int64, error) {
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"filter": buildBoolFilter(since, nil, filter, true)},
		},
	}
	body, err := json.Marshal(q)
	if err != nil {
		return 0, err
	}
	resp, err := es.Count(
		es.Count.WithContext(ctx),
		es.Count.WithIndex(index),
		es.Count.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return 0, nil
	}
	if resp.IsError() {
		return 0, fmt.Errorf("count: %s", resp.Status())
	}
	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode count response: %w", err)
	}
	return result.Count, nil
}
