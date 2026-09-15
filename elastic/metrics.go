package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// AggResult maps "<metric_name>_<agg>" to its float64 value.
// Keys match the step output names written to State.
type AggResult map[string]float64

// AggMetricSpec describes one field and the aggregations to compute over it.
type AggMetricSpec struct {
	Name  string   // output key prefix
	Field string   // ES document field
	Aggs  []string // "min", "max", "avg", "p95", "p99", etc.
}

// buildBoolFilter constructs the ES bool/filter clause list for a timestamp
// range plus optional term/wildcard filters. until is optional (nil = no upper
// bound). Wildcard values (containing * or ?) use wildcard queries only when
// allowWildcard is true; otherwise they fall through to term queries.
func buildBoolFilter(since time.Time, until *time.Time, filter map[string]string, allowWildcard bool) []interface{} {
	rangeClause := map[string]interface{}{
		"gte": since.UTC().Format(time.RFC3339),
	}
	if until != nil {
		rangeClause["lte"] = until.UTC().Format(time.RFC3339)
	}
	clauses := []interface{}{
		map[string]interface{}{
			"range": map[string]interface{}{"@timestamp": rangeClause},
		},
	}
	for field, value := range filter {
		if allowWildcard && strings.ContainsAny(value, "*?") {
			clauses = append(clauses, map[string]interface{}{
				"wildcard": map[string]interface{}{field: value},
			})
		} else {
			clauses = append(clauses, map[string]interface{}{
				"term": map[string]interface{}{field: value},
			})
		}
	}
	return clauses
}

// FetchDocs returns up to size documents matching the date-range + filter query,
// sorted by @timestamp ascending. Use size=0 for the default of 10 000.
func FetchDocs(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since, until time.Time,
	filter map[string]string,
	size int,
) ([]map[string]interface{}, error) {
	if size <= 0 {
		size = 10000
	}
	q := map[string]interface{}{
		"size": size,
		"sort": []interface{}{
			map[string]interface{}{"@timestamp": map[string]interface{}{"order": "asc"}},
		},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"filter": buildBoolFilter(since, &until, filter, true)},
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
		Hits struct {
			Hits []struct {
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	docs := make([]map[string]interface{}, len(result.Hits.Hits))
	for i, h := range result.Hits.Hits {
		docs[i] = h.Source
	}
	return docs, nil
}

// GetFieldValue resolves a dot-notation path against a document whose keys may
// be nested objects, dotted keys, or any mix of the two. Returns the value and
// true if found, or nil and false if no combination of segments matches.
func GetFieldValue(doc map[string]interface{}, path string) (interface{}, bool) {
	if v, ok := doc[path]; ok {
		return v, true
	}
	parts := strings.Split(path, ".")
	for i := 1; i < len(parts); i++ {
		nested, ok := doc[strings.Join(parts[:i], ".")].(map[string]interface{})
		if !ok {
			continue
		}
		if v, ok := GetFieldValue(nested, strings.Join(parts[i:], ".")); ok {
			return v, true
		}
	}
	return nil, false
}

// RunAgg executes a date-range + term-filter aggregation query and returns
// the results and the total document hit count. filter maps ES field names to
// exact match values (term queries). A hit count of 0 means no documents
// matched the query — aggregation values will all be absent from AggResult.
func RunAgg(
	ctx context.Context,
	es *elasticsearch.Client,
	index string,
	since, until time.Time,
	filter map[string]string,
	metrics []AggMetricSpec,
) (AggResult, int64, error) {
	body, err := buildAggQuery(since, until, filter, metrics)
	if err != nil {
		return nil, 0, fmt.Errorf("build agg query: %w", err)
	}
	resp, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(index),
		es.Search.WithBody(strings.NewReader(body)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("search: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return nil, 0, fmt.Errorf("search: %s", resp.Status())
	}
	return parseAggResult(resp.Body, metrics)
}

func buildAggQuery(since, until time.Time, filter map[string]string, metrics []AggMetricSpec) (string, error) {
	aggs := map[string]interface{}{}
	for _, m := range metrics {
		for _, agg := range m.Aggs {
			key := m.Name + "_" + agg
			switch agg {
			case "min":
				aggs[key] = map[string]interface{}{"min": map[string]interface{}{"field": m.Field}}
			case "max":
				aggs[key] = map[string]interface{}{"max": map[string]interface{}{"field": m.Field}}
			case "avg":
				aggs[key] = map[string]interface{}{"avg": map[string]interface{}{"field": m.Field}}
			default:
				if !strings.HasPrefix(agg, "p") {
					return "", fmt.Errorf("unsupported agg %q: expected min, max, avg, or pN (e.g. p95)", agg)
				}
				pct, err := strconv.Atoi(strings.TrimPrefix(agg, "p"))
				if err != nil {
					return "", fmt.Errorf("invalid agg %q: %w", agg, err)
				}
				aggs[key] = map[string]interface{}{
					"percentiles": map[string]interface{}{
						"field":    m.Field,
						"percents": []int{pct},
					},
				}
			}
		}
	}

	q := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"filter": buildBoolFilter(since, &until, filter, false)},
		},
		"aggs": aggs,
	}
	b, err := json.Marshal(q)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseAggResult(body io.Reader, metrics []AggMetricSpec) (AggResult, int64, error) {
	var resp struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
		} `json:"hits"`
		Aggregations map[string]json.RawMessage `json:"aggregations"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, 0, fmt.Errorf("decode agg response: %w", err)
	}

	result := AggResult{}
	for _, m := range metrics {
		for _, agg := range m.Aggs {
			key := m.Name + "_" + agg
			raw, ok := resp.Aggregations[key]
			if !ok {
				continue
			}
			if strings.HasPrefix(agg, "p") {
				var pResp struct {
					Values map[string]interface{} `json:"values"`
				}
				if err := json.Unmarshal(raw, &pResp); err != nil {
					continue
				}
				pctKey := strings.TrimPrefix(agg, "p") + ".0" // "95" → "95.0"
				if v, ok := pResp.Values[pctKey].(float64); ok {
					result[key] = v
				}
			} else {
				var vResp struct {
					Value *float64 `json:"value"`
				}
				if err := json.Unmarshal(raw, &vResp); err == nil && vResp.Value != nil {
					result[key] = *vResp.Value
				}
			}
		}
	}
	return result, resp.Hits.Total.Value, nil
}
