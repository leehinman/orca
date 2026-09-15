package steps_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/orca/harness"
	"github.com/elastic/orca/harness/steps"
)

type fakeFleet struct {
	mu        sync.Mutex
	hostnames map[string][]string
	statuses  map[string][]string
	polls     map[string]int
	requests  []string
	fileBody  string
	auth      string
}

func newFakeFleet() *fakeFleet {
	return &fakeFleet{
		hostnames: map[string][]string{},
		statuses:  map[string][]string{},
		polls:     map[string]int{},
		fileBody:  "PK\x03\x04fake-zip",
	}
}

func (f *fakeFleet) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("kbn-xsrf") == "" {
			t.Errorf("missing kbn-xsrf header on %s %s", r.Method, r.URL.Path)
		}
		if u, p, ok := r.BasicAuth(); ok {
			f.auth = u + ":" + p
		}
		path := r.URL.Path
		switch {
		case path == "/api/fleet/agents":
			kuery := r.URL.Query().Get("kuery")
			host := strings.TrimSuffix(strings.TrimPrefix(kuery, `local_metadata.host.hostname:"`), `"`)
			items := []map[string]string{}
			for _, id := range f.hostnames[host] {
				items = append(items, map[string]string{"id": id, "status": "online"})
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items}) //nolint:errcheck
		case strings.HasSuffix(path, "/request_diagnostics"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/fleet/agents/"), "/request_diagnostics")
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body) //nolint:errcheck
			f.requests = append(f.requests, id)
			if _, ok := f.statuses[id]; !ok {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"message":"agent not found"}`)) //nolint:errcheck
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"actionId": "action-" + id}) //nolint:errcheck
		case strings.HasSuffix(path, "/uploads"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/fleet/agents/"), "/uploads")
			seq := f.statuses[id]
			n := f.polls[id]
			f.polls[id]++
			if n >= len(seq) {
				n = len(seq) - 1
			}
			items := []map[string]string{{
				"id": "file-old", "name": "old.zip", "actionId": "action-stale", "status": "READY",
			}}
			if n >= 0 && seq[n] != "" {
				items = append(items, map[string]string{
					"id": "file-" + id, "name": id + ".zip", "actionId": "action-" + id,
					"status": seq[n], "error": map[string]string{"FAILED": "boom"}[seq[n]],
				})
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items}) //nolint:errcheck
		case strings.HasPrefix(path, "/api/fleet/agents/files/"):
			w.Header().Set("Content-Type", "application/zip")
			w.Write([]byte(f.fileBody)) //nolint:errcheck
		default:
			http.NotFound(w, r)
		}
	})
}

func runFleetDiag(t *testing.T, srv *httptest.Server, step harness.Step, state *harness.State) error {
	t.Helper()
	exec, err := steps.NewFleetDiagFactory(harness.KibanaConfig{
		URL: srv.URL, Username: "elastic", Password: "changeme",
	})(step)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	return exec.Run(context.Background(), state)
}

func diagStep(agents ...harness.FleetDiagAgent) harness.Step {
	return harness.Step{ID: "diag", FleetDiag: &harness.FleetDiagStep{
		OutputDir:    "${results}",
		Timeout:      harness.Duration{Duration: 5 * time.Second},
		PollInterval: harness.Duration{Duration: 10 * time.Millisecond},
		Agents:       agents,
	}}
}

func TestFleetDiag_DownloadsAllBundles(t *testing.T) {
	f := newFakeFleet()
	f.hostnames["bench-1"] = []string{"id-1"}
	f.statuses["id-0"] = []string{"", "AWAITING_UPLOAD", "IN_PROGRESS", "READY"}
	f.statuses["id-1"] = []string{"READY"}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()

	results := t.TempDir()
	state := harness.NewState()
	state.SetResults(results)
	state.SetStepOutput("provision", "agent0", "id-0")

	err := runFleetDiag(t, srv, diagStep(
		harness.FleetDiagAgent{Name: "agent0", ID: "${steps.provision.agent0}"},
		harness.FleetDiagAgent{Name: "agent1", Hostname: "bench-1"},
	), state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range []string{"agent0", "agent1"} {
		want := filepath.Join(results, name+"-diagnostics.zip")
		got, err := state.Resolve("${steps.diag." + name + "}")
		if err != nil || got != want {
			t.Errorf("%s output: got %q (%v), want %q", name, got, err, want)
		}
		data, err := os.ReadFile(want)
		if err != nil || string(data) != f.fileBody {
			t.Errorf("%s: bundle content mismatch: %q (%v)", name, data, err)
		}
	}
	if f.auth != "elastic:changeme" {
		t.Errorf("basic auth: got %q", f.auth)
	}
	if len(f.requests) != 2 {
		t.Errorf("expected 2 diagnostics requests, got %v", f.requests)
	}
}

func TestFleetDiag_FailedUploadDoesNotFailStep(t *testing.T) {
	f := newFakeFleet()
	f.statuses["id-0"] = []string{"READY"}
	f.statuses["id-1"] = []string{"IN_PROGRESS", "FAILED"}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()

	results := t.TempDir()
	state := harness.NewState()
	state.SetResults(results)

	err := runFleetDiag(t, srv, diagStep(
		harness.FleetDiagAgent{Name: "agent0", ID: "id-0"},
		harness.FleetDiagAgent{Name: "agent1", ID: "id-1"},
	), state)
	if err != nil {
		t.Fatalf("agent failure should not fail the step, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(results, "agent1-diagnostics.zip")); err == nil {
		t.Errorf("agent1 bundle should not exist")
	}
	if _, err := os.Stat(filepath.Join(results, "agent0-diagnostics.zip")); err != nil {
		t.Errorf("agent0 bundle should still be downloaded: %v", err)
	}
}

func TestFleetDiag_TimeoutWhenNeverReady(t *testing.T) {
	f := newFakeFleet()
	f.statuses["id-0"] = []string{"AWAITING_UPLOAD"}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()

	state := harness.NewState()
	state.SetResults(t.TempDir())
	step := diagStep(harness.FleetDiagAgent{Name: "agent0", ID: "id-0"})
	step.FleetDiag.Timeout = harness.Duration{Duration: 100 * time.Millisecond}

	err := runFleetDiag(t, srv, step, state)
	if err != nil {
		t.Fatalf("timeout should not fail the step, got %v", err)
	}
}

func TestFleetDiag_HostnameNotFound(t *testing.T) {
	f := newFakeFleet()
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()

	state := harness.NewState()
	state.SetResults(t.TempDir())
	err := runFleetDiag(t, srv, diagStep(harness.FleetDiagAgent{Name: "agent0", Hostname: "nope"}), state)
	if err != nil {
		t.Fatalf("lookup failure should not fail the step, got %v", err)
	}
	if len(f.requests) != 0 {
		t.Errorf("no diagnostics should be requested, got %v", f.requests)
	}
}

func TestFleetDiag_HostnameAmbiguous(t *testing.T) {
	f := newFakeFleet()
	f.hostnames["dup"] = []string{"id-a", "id-b"}
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()

	state := harness.NewState()
	state.SetResults(t.TempDir())
	err := runFleetDiag(t, srv, diagStep(harness.FleetDiagAgent{Name: "agent0", Hostname: "dup"}), state)
	if err != nil {
		t.Fatalf("ambiguity should not fail the step, got %v", err)
	}
}

func TestFleetDiag_RequestRejected(t *testing.T) {
	f := newFakeFleet()
	srv := httptest.NewServer(f.handler(t))
	defer srv.Close()

	state := harness.NewState()
	state.SetResults(t.TempDir())
	err := runFleetDiag(t, srv, diagStep(harness.FleetDiagAgent{Name: "agent0", ID: "unknown"}), state)
	if err != nil {
		t.Fatalf("rejected request should not fail the step, got %v", err)
	}
}

func TestFleetDiag_FactoryValidation(t *testing.T) {
	factory := steps.NewFleetDiagFactory(harness.KibanaConfig{URL: "https://kibana"})
	cases := map[string]harness.Step{
		"no agents":      {FleetDiag: &harness.FleetDiagStep{OutputDir: "x"}},
		"missing name":   {FleetDiag: &harness.FleetDiagStep{Agents: []harness.FleetDiagAgent{{ID: "id"}}}},
		"no id/hostname": {FleetDiag: &harness.FleetDiagStep{Agents: []harness.FleetDiagAgent{{Name: "a"}}}},
	}
	for name, step := range cases {
		if _, err := factory(step); err == nil {
			t.Errorf("%s: expected factory error", name)
		}
	}
}

func TestFleetDiag_NoKibanaURL(t *testing.T) {
	exec, err := steps.NewFleetDiagFactory(harness.KibanaConfig{})(diagStep(harness.FleetDiagAgent{Name: "a", ID: "id"}))
	if err != nil {
		t.Fatal(err)
	}
	state := harness.NewState()
	state.SetResults(t.TempDir())
	if err := exec.Run(context.Background(), state); err == nil || !strings.Contains(err.Error(), "kibana.url") {
		t.Fatalf("expected kibana.url error, got %v", err)
	}
}
