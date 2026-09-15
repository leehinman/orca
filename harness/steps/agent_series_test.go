package steps

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAlignSeries_ForwardFillAndStale(t *testing.T) {
	base := time.Date(2026, 9, 24, 18, 0, 0, 0, time.UTC)
	a := []tsPoint{{base.Add(10 * time.Second), 1}, {base.Add(70 * time.Second), 2}, {base.Add(130 * time.Second), 3}}
	b := []tsPoint{{base.Add(20 * time.Second), 10}, {base.Add(4*time.Minute + 20*time.Second), 40}}

	times, vals := alignSeries([][]tsPoint{a, b}, time.Minute)
	if len(times) != 5 {
		t.Fatalf("buckets: got %d, want 5", len(times))
	}
	if !times[0].Equal(base) {
		t.Errorf("first bucket: %v", times[0])
	}
	want := [][2]float64{{1, 10}, {2, 10}, {3, math.NaN()}, {3, math.NaN()}, {math.NaN(), 40}}
	for i, w := range want {
		for j := range w {
			got := vals[i][j]
			if math.IsNaN(w[j]) != math.IsNaN(got) || (!math.IsNaN(w[j]) && got != w[j]) {
				t.Errorf("bucket %d series %d: got %v, want %v", i, j, got, w[j])
			}
		}
	}
	totals := sumRows(vals)
	if totals[0] != 11 || totals[1] != 12 || totals[2] != 3 || totals[4] != 40 {
		t.Errorf("totals: %v", totals)
	}
}

func TestAlignSeries_Empty(t *testing.T) {
	times, vals := alignSeries(nil, time.Minute)
	if times != nil || vals != nil {
		t.Error("expected nil for no series")
	}
}

func TestCpuPctPoints(t *testing.T) {
	base := time.Date(2026, 9, 24, 18, 0, 0, 0, time.UTC)
	p := &procSeries{samples: []procSample{
		{t: base, cpuMS: 1000, cores: 4},
		{t: base.Add(time.Minute), cpuMS: 1000 + 12000, cores: 4},
	}}
	pts := cpuPctPoints(p)
	if len(pts) != 1 || pts[0].v != 5 {
		t.Errorf("12s of cpu over 60s on 4 cores should be 5%%, got %+v", pts)
	}
}

func TestMatchesIntegration(t *testing.T) {
	cases := []struct {
		id, integ string
		want      bool
	}{
		{"prometheus/metrics-default", "prometheus/metrics-default", true},
		{"prometheus/metrics-default/prometheus", "prometheus/metrics-default", true},
		{"prometheus/metrics-default-2", "prometheus/metrics-default", false},
		{"elasticsearch-default", "prometheus/metrics-default", false},
	}
	for _, c := range cases {
		if got := matchesIntegration(c.id, c.integ); got != c.want {
			t.Errorf("matchesIntegration(%q,%q)=%v want %v", c.id, c.integ, got, c.want)
		}
	}
}

func TestStatsComponentID_PrefersDottedKey(t *testing.T) {
	doc := map[string]interface{}{
		"component":    map[string]interface{}{"id": "elastic-otel-collector"},
		"component.id": "prometheus/metrics-default/prometheus",
	}
	if got := statsComponentID(doc); got != "prometheus/metrics-default/prometheus" {
		t.Errorf("got %q", got)
	}
	delete(doc, "component.id")
	if got := statsComponentID(doc); got != "elastic-otel-collector" {
		t.Errorf("fallback got %q", got)
	}
}

func TestReadTSV_LongLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wide.tsv")
	cols := make([]string, 20000)
	for i := range cols {
		cols[i] = fmt.Sprintf("column_%05d", i)
	}
	content := "timestamp\t" + strings.Join(cols, "\t") + "\n2026-01-01T00:00:00Z\t" + strings.Repeat("1\t", 19999) + "1\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	header, rows, err := readTSV(path)
	if err != nil {
		t.Fatalf("readTSV: %v", err)
	}
	if len(header) != 20001 || len(rows) != 1 || len(rows[0]) != 20001 {
		t.Errorf("header %d rows %d cols %d", len(header), len(rows), len(rows[0]))
	}
}

func inputDoc(t time.Time, parent, inputID string, published float64) map[string]interface{} {
	return map[string]interface{}{
		"@timestamp":        t.Format(time.RFC3339Nano),
		"data_stream":       map[string]interface{}{"dataset": "elastic_agent.filebeat_input"},
		"component":         map[string]interface{}{"id": "elastic-otel-collector"},
		"component.id":      parent + "/" + inputID,
		"filebeat_input.id": inputID,
		"filebeat_input.events_pipeline_published_total": published,
	}
}

func TestRollUpInputSeries_SumsPerFileRates(t *testing.T) {
	base := time.Date(2026, 9, 25, 2, 40, 0, 0, time.UTC)
	var docs []map[string]interface{}
	for f := 0; f < 3; f++ {
		id := fmt.Sprintf("kubernetes.container_logs-emitter-%d", f)
		for i := 0; i < 3; i++ {
			docs = append(docs, inputDoc(base.Add(time.Duration(i)*time.Minute+time.Duration(f)*time.Second), "filestream-default", id, float64(i*60*(f+1))))
		}
	}
	docs = append(docs, inputDoc(base, "filestream-monitoring", "filestream-monitoring-agent", 0), inputDoc(base.Add(time.Minute), "filestream-monitoring", "filestream-monitoring-agent", 600))

	stats := groupStatsDocs(docs)
	if len(stats) != 4 {
		t.Fatalf("raw series: got %d, want 4", len(stats))
	}
	rates := rollUpInputSeries(stats, time.Minute)
	if len(rates) != 2 {
		t.Fatalf("rolled up series: got %d, want 2: %+v", len(rates), rates)
	}
	def := rates[0]
	if def.id != "filestream-default" || def.members != 3 {
		t.Errorf("parent roll-up: %+v", def)
	}
	if len(def.points) != 2 || def.points[0].v != 6 || def.points[1].v != 6 {
		t.Errorf("summed rates should be 1+2+3 events/s per bucket, got %+v", def.points)
	}
	if mon := rates[1]; mon.id != "filestream-monitoring" || mon.members != 1 || len(mon.points) != 1 || mon.points[0].v != 10 {
		t.Errorf("single-member series should pass through: %+v", mon)
	}
}

func TestCapEventSeries_FoldsIntoOther(t *testing.T) {
	base := time.Date(2026, 9, 25, 2, 40, 0, 0, time.UTC)
	var series []*rateSeries
	for i := 0; i < 25; i++ {
		series = append(series, &rateSeries{dataset: "d", id: fmt.Sprintf("s%02d", i), members: 1,
			points: []tsPoint{{base, float64(i)}, {base.Add(time.Minute), float64(i)}}})
	}
	out := capEventSeries(series, 20, time.Minute)
	if len(out) != 21 {
		t.Fatalf("got %d series, want 21", len(out))
	}
	other := out[20]
	if other.id != otherSeriesID || other.members != 5 {
		t.Errorf("other: %+v", other)
	}
	if len(other.points) != 2 || other.points[0].v != 0+1+2+3+4 {
		t.Errorf("other should sum the five lowest series: %+v", other.points)
	}
	for _, s := range out[:20] {
		if s.id < "s05" {
			t.Errorf("low-rate series %s should have been folded", s.id)
		}
	}
	if got := capEventSeries(series[:5], 20, time.Minute); len(got) != 5 {
		t.Errorf("under the cap should be untouched, got %d", len(got))
	}
}

func TestDetectPeriod(t *testing.T) {
	base := time.Date(2026, 10, 5, 17, 0, 0, 0, time.UTC)
	mk := func(step time.Duration, n int, jitter time.Duration) []tsPoint {
		pts := make([]tsPoint, n)
		for i := range pts {
			j := time.Duration(0)
			if i%2 == 1 {
				j = jitter
			}
			pts[i] = tsPoint{base.Add(time.Duration(i)*step + j), float64(i)}
		}
		return pts
	}
	cases := []struct {
		name  string
		procs []*procSeries
		stats []*statsSeries
		want  time.Duration
	}{
		{"empty", nil, nil, time.Minute},
		{"15s stats", nil, []*statsSeries{{points: mk(15*time.Second, 8, 0)}}, 15 * time.Second},
		{"60s stats with jitter", nil, []*statsSeries{{points: mk(time.Minute, 6, 400*time.Millisecond)}}, time.Minute},
		{"single sample", nil, []*statsSeries{{points: mk(15*time.Second, 1, 0)}}, time.Minute},
		{"procs 10s", []*procSeries{{samples: []procSample{{t: base}, {t: base.Add(10 * time.Second)}, {t: base.Add(20 * time.Second)}}}}, nil, 10 * time.Second},
	}
	for _, c := range cases {
		if got := detectPeriod(c.procs, c.stats); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestRollUpInputSeries_15sPeriod(t *testing.T) {
	base := time.Date(2026, 10, 5, 17, 0, 0, 0, time.UTC)
	counter := func(perSec float64, n int) []tsPoint {
		pts := make([]tsPoint, n)
		for i := range pts {
			pts[i] = tsPoint{base.Add(time.Duration(i) * 15 * time.Second), perSec * 15 * float64(i)}
		}
		return pts
	}
	stats := []*statsSeries{
		{dataset: "elastic_agent.filebeat_input", id: "log-default/a", parent: "log-default", field: "events", points: counter(100, 5)},
		{dataset: "elastic_agent.filebeat_input", id: "log-default/b", parent: "log-default", field: "events", points: counter(50, 5)},
	}
	for _, period := range []time.Duration{15 * time.Second, time.Minute} {
		out := rollUpInputSeries(stats, period)
		if len(out) != 1 || out[0].members != 2 {
			t.Fatalf("period %s: got %d series, members=%d", period, len(out), out[0].members)
		}
		avg, max := avgMax(out[0].points)
		if period == 15*time.Second && (avg != 150 || max != 150) {
			t.Errorf("15s period: got avg=%v max=%v, want 150", avg, max)
		}
		if period == time.Minute && max <= 150 {
			t.Errorf("1m period should over-count 15s samples (max=%v), proving detection matters", max)
		}
	}
}
