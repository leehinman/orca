package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// HistogramBucket is one fixed-interval bucket of a document count histogram.
type HistogramBucket struct {
	Time  time.Time
	Count int64
}

// DocCountHistogram returns document counts per fixed interval between since
// and until, including empty buckets.
func DocCountHistogram(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since, until time.Time,
	filter map[string]string,
	interval time.Duration,
) ([]HistogramBucket, error) {
	q := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"filter": buildBoolFilter(since, &until, filter, true)},
		},
		"aggs": map[string]interface{}{
			"hist": map[string]interface{}{
				"date_histogram": map[string]interface{}{
					"field":          "@timestamp",
					"fixed_interval": fmt.Sprintf("%ds", int(interval.Seconds())),
					"min_doc_count":  0,
					"extended_bounds": map[string]interface{}{
						"min": since.UTC().Format(time.RFC3339),
						"max": until.UTC().Format(time.RFC3339),
					},
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
	if resp.IsError() {
		return nil, fmt.Errorf("search: %s", resp.Status())
	}
	var result struct {
		Aggregations struct {
			Hist struct {
				Buckets []struct {
					Key      int64 `json:"key"`
					DocCount int64 `json:"doc_count"`
				} `json:"buckets"`
			} `json:"hist"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode histogram response: %w", err)
	}
	out := make([]HistogramBucket, 0, len(result.Aggregations.Hist.Buckets))
	for _, b := range result.Aggregations.Hist.Buckets {
		out = append(out, HistogramBucket{Time: time.UnixMilli(b.Key).UTC(), Count: b.DocCount})
	}
	return out, nil
}

// CountByGroup returns document counts between since and until grouped by the
// distinct values of field.
func CountByGroup(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since, until time.Time,
	filter map[string]string,
	field string,
) (map[string]int64, error) {
	return countByGroup(ctx, es, index, since, &until, filter, field)
}
