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

// DataStreamCount is the number of documents in one data stream.
type DataStreamCount struct {
	Name  string
	Count int64
}

// CountByDataStream returns document counts between since and until grouped
// by data stream (type-dataset-namespace), largest first.
func CountByDataStream(ctx context.Context, es *elasticsearch.Client, index string, since, until time.Time) ([]DataStreamCount, error) {
	q := map[string]interface{}{
		"size":  0,
		"query": map[string]interface{}{"bool": map[string]interface{}{"filter": buildBoolFilter(since, &until, nil, true)}},
		"aggs": map[string]interface{}{
			"ds": map[string]interface{}{
				"multi_terms": map[string]interface{}{
					"size": 1000,
					"terms": []interface{}{
						map[string]interface{}{"field": "data_stream.type"},
						map[string]interface{}{"field": "data_stream.dataset"},
						map[string]interface{}{"field": "data_stream.namespace"},
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
		es.Search.WithIgnoreUnavailable(true),
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
			DS struct {
				Buckets []struct {
					Key      []string `json:"key"`
					DocCount int64    `json:"doc_count"`
				} `json:"buckets"`
			} `json:"ds"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}
	out := make([]DataStreamCount, 0, len(result.Aggregations.DS.Buckets))
	for _, b := range result.Aggregations.DS.Buckets {
		out = append(out, DataStreamCount{Name: strings.Join(b.Key, "-"), Count: b.DocCount})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}
