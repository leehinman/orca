package steps_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

func fakeESInfo(t *testing.T, body string) harness.ESConfig {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return harness.ESConfig{URL: srv.URL}
}

func fleetAgentJSON(host, version string, snapshot bool) string {
	return fmt.Sprintf(`{"id":"id-%s","status":"online","local_metadata":{"host":{"hostname":%q},"elastic":{"agent":{"version":%q,"snapshot":%v,"build":{"original":"%s (build: abc at 2026-01-01)"}}}},"agent":{"version":%q}}`,
		host, host, version, snapshot, version, version)
}

func fakeKibanaFleet(t *testing.T, agents []string, perPage int) (harness.KibanaConfig, *[]string) {
	t.Helper()
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/status":
			fmt.Fprint(w, `{"version":{"number":"9.6.0","build_hash":"fedcba9876543210","build_snapshot":true}}`)
		case "/api/fleet/agents":
			page := 1
			fmt.Sscanf(r.URL.Query().Get("page"), "%d", &page) //nolint:errcheck
			start := (page - 1) * perPage
			end := start + perPage
			if start > len(agents) {
				start = len(agents)
			}
			if end > len(agents) {
				end = len(agents)
			}
			fmt.Fprintf(w, `{"items":[%s],"total":%d,"page":%d,"perPage":%d}`, strings.Join(agents[start:end], ","), len(agents), page, perPage)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return harness.KibanaConfig{URL: srv.URL, Username: "elastic", Password: "changeme"}, &paths
}

func runStackVersions(t *testing.T, es harness.ESConfig, kb harness.KibanaConfig) (harness.StackVersions, *harness.State) {
	t.Helper()
	exec, err := steps.NewStackVersionsFactory(es, kb)(harness.Step{ID: "versions", StackVersions: &harness.StackVersionsStep{}})
	if err != nil {
		t.Fatal(err)
	}
	state := harness.NewState()
	if err := exec.Run(context.Background(), state); err != nil {
		t.Fatalf("run: %v", err)
	}
	raw, err := state.Resolve("${steps.versions.summary}")
	if err != nil {
		t.Fatalf("summary not in state: %v", err)
	}
	var sv harness.StackVersions
	if err := json.Unmarshal([]byte(raw), &sv); err != nil {
		t.Fatal(err)
	}
	return sv, state
}

const esInfoSnapshot = `{"name":"es","version":{"number":"9.6.0-SNAPSHOT","build_hash":"0123456789abcdef","build_date":"2026-10-01T03:12:00Z","build_snapshot":true}}`

func TestStackVersions_GroupsAgentsByVersion(t *testing.T) {
	es := fakeESInfo(t, esInfoSnapshot)
	kb, _ := fakeKibanaFleet(t, []string{
		fleetAgentJSON("agent-1", "9.2.8", false),
		fleetAgentJSON("agent-0", "9.2.8", false),
		fleetAgentJSON("fleet-server", "9.6.0", true),
	}, 1000)

	sv, state := runStackVersions(t, es, kb)

	if sv.Elasticsearch.Version != "9.6.0-SNAPSHOT" || sv.Elasticsearch.BuildHash != "0123456789abcdef" || sv.Elasticsearch.BuildDate != "2026-10-01T03:12:00Z" {
		t.Errorf("elasticsearch: %+v", sv.Elasticsearch)
	}
	if sv.Kibana.Version != "9.6.0-SNAPSHOT" || sv.Kibana.BuildHash != "fedcba9876543210" {
		t.Errorf("kibana: %+v", sv.Kibana)
	}
	if len(sv.Agents) != 2 {
		t.Fatalf("agent groups: %+v", sv.Agents)
	}
	if sv.Agents[0].Version != "9.2.8" || sv.Agents[0].Count != 2 || strings.Join(sv.Agents[0].Hosts, ",") != "agent-0,agent-1" {
		t.Errorf("agents[0]: %+v", sv.Agents[0])
	}
	if sv.Agents[1].Version != "9.6.0-SNAPSHOT" || sv.Agents[1].Count != 1 || sv.Agents[1].Build == "" {
		t.Errorf("agents[1]: %+v", sv.Agents[1])
	}
	for key, want := range map[string]string{"elasticsearch": "9.6.0-SNAPSHOT", "kibana": "9.6.0-SNAPSHOT", "elastic_agent": "9.2.8,9.6.0-SNAPSHOT"} {
		if got, _ := state.Resolve("${steps.versions." + key + "}"); got != want {
			t.Errorf("steps.versions.%s: got %q, want %q", key, got, want)
		}
	}
}

func TestStackVersions_PaginatesFleetAgents(t *testing.T) {
	es := fakeESInfo(t, esInfoSnapshot)
	var agents []string
	for i := 0; i < 5; i++ {
		agents = append(agents, fleetAgentJSON(fmt.Sprintf("h%d", i), "9.2.8", false))
	}
	kb, paths := fakeKibanaFleet(t, agents, 2)

	sv, _ := runStackVersions(t, es, kb)

	if len(sv.Agents) != 1 || sv.Agents[0].Count != 5 {
		t.Errorf("agents: %+v", sv.Agents)
	}
	pages := 0
	for _, p := range *paths {
		if strings.HasPrefix(p, "/api/fleet/agents?") {
			pages++
		}
	}
	if pages != 3 {
		t.Errorf("fleet agent pages requested: got %d, want 3 (%v)", pages, *paths)
	}
}

func TestStackVersions_NoKibanaConfigured(t *testing.T) {
	es := fakeESInfo(t, `{"version":{"number":"9.2.8","build_hash":"abc","build_snapshot":false}}`)

	sv, _ := runStackVersions(t, es, harness.KibanaConfig{})

	if sv.Elasticsearch.Version != "9.2.8" {
		t.Errorf("elasticsearch: %+v", sv.Elasticsearch)
	}
	if sv.Kibana.Version != "" || len(sv.Agents) != 0 {
		t.Errorf("expected no kibana/agent info: %+v", sv)
	}
}

func TestStackVersions_ESErrorFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"unauthorized"}`)
	}))
	t.Cleanup(srv.Close)
	exec, _ := steps.NewStackVersionsFactory(harness.ESConfig{URL: srv.URL}, harness.KibanaConfig{})(harness.Step{ID: "v", StackVersions: &harness.StackVersionsStep{}})
	if err := exec.Run(context.Background(), harness.NewState()); err == nil || !strings.Contains(err.Error(), "elasticsearch") {
		t.Fatalf("expected elasticsearch error, got %v", err)
	}
}
