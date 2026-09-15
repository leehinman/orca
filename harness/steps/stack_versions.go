package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/orca/harness"
)

const fleetAgentsPageSize = 1000

func NewStackVersionsFactory(esCfg harness.ESConfig, kbCfg harness.KibanaConfig) harness.ExecutorFactory {
	return func(step harness.Step) (harness.Executor, error) {
		return harness.ExecutorFunc(func(ctx context.Context, state *harness.State) error {
			var summary harness.StackVersions

			es, err := buildESClient(state, esCfg)
			if err != nil {
				return fmt.Errorf("stack-versions: %w", err)
			}
			if summary.Elasticsearch, err = esBuildInfo(ctx, es); err != nil {
				return fmt.Errorf("stack-versions: elasticsearch: %w", err)
			}

			if kbCfg.URL == "" {
				fmt.Fprintln(os.Stderr, "[stack-versions] kibana.url not configured; skipping kibana and elastic-agent versions")
			} else {
				kb, err := newKibanaClient(state, kbCfg)
				if err != nil {
					return fmt.Errorf("stack-versions: %w", err)
				}
				if summary.Kibana, err = kb.buildInfo(ctx); err != nil {
					return fmt.Errorf("stack-versions: kibana: %w", err)
				}
				agents, err := kb.listAgents(ctx)
				if err != nil {
					return fmt.Errorf("stack-versions: fleet agents: %w", err)
				}
				summary.Agents = groupAgentVersions(agents)
			}

			fmt.Fprintf(os.Stderr, "[stack-versions] elasticsearch %s, kibana %s, elastic-agent %s\n",
				summary.Elasticsearch.Version, orDash(summary.Kibana.Version), orDash(agentVersionList(summary.Agents)))

			if step.ID != "" {
				b, err := json.Marshal(summary)
				if err != nil {
					return fmt.Errorf("encode summary: %w", err)
				}
				state.SetStepOutput(step.ID, "summary", string(b))
				state.SetStepOutput(step.ID, "elasticsearch", summary.Elasticsearch.Version)
				state.SetStepOutput(step.ID, "kibana", summary.Kibana.Version)
				state.SetStepOutput(step.ID, "elastic_agent", agentVersionList(summary.Agents))
			}
			return nil
		}), nil
	}
}

func esBuildInfo(ctx context.Context, es *elasticsearch.Client) (harness.ComponentVersion, error) {
	res, err := es.Info(es.Info.WithContext(ctx))
	if err != nil {
		return harness.ComponentVersion{}, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return harness.ComponentVersion{}, fmt.Errorf("GET /: %s", res.String())
	}
	var out struct {
		Version struct {
			Number    string `json:"number"`
			BuildHash string `json:"build_hash"`
			BuildDate string `json:"build_date"`
			Snapshot  bool   `json:"build_snapshot"`
		} `json:"version"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return harness.ComponentVersion{}, fmt.Errorf("decode GET /: %w", err)
	}
	if out.Version.Number == "" {
		return harness.ComponentVersion{}, fmt.Errorf("GET / returned no version")
	}
	return harness.ComponentVersion{
		Version:   snapshotVersion(out.Version.Number, out.Version.Snapshot),
		BuildHash: out.Version.BuildHash,
		BuildDate: out.Version.BuildDate,
	}, nil
}

type fleetAgent struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	PolicyID      string `json:"policy_id"`
	LocalMetadata struct {
		Host struct {
			Hostname string `json:"hostname"`
		} `json:"host"`
		Elastic struct {
			Agent struct {
				Version  string `json:"version"`
				Snapshot bool   `json:"snapshot"`
				Build    struct {
					Original string `json:"original"`
				} `json:"build"`
			} `json:"agent"`
		} `json:"elastic"`
	} `json:"local_metadata"`
	Agent struct {
		Version string `json:"version"`
	} `json:"agent"`
}

func (c *kibanaClient) buildInfo(ctx context.Context) (harness.ComponentVersion, error) {
	var out struct {
		Version struct {
			Number    string `json:"number"`
			BuildHash string `json:"build_hash"`
			BuildDate string `json:"build_date"`
			Snapshot  bool   `json:"build_snapshot"`
		} `json:"version"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/api/status", nil, &out); err != nil {
		return harness.ComponentVersion{}, err
	}
	if out.Version.Number == "" {
		return harness.ComponentVersion{}, fmt.Errorf("kibana /api/status returned no version")
	}
	return harness.ComponentVersion{
		Version:   snapshotVersion(out.Version.Number, out.Version.Snapshot),
		BuildHash: out.Version.BuildHash,
		BuildDate: out.Version.BuildDate,
	}, nil
}

func (c *kibanaClient) listAgents(ctx context.Context) ([]fleetAgent, error) {
	var all []fleetAgent
	for page := 1; ; page++ {
		q := url.Values{"perPage": {strconv.Itoa(fleetAgentsPageSize)}, "page": {strconv.Itoa(page)}}
		var out struct {
			Items []fleetAgent `json:"items"`
			Total int          `json:"total"`
		}
		if err := c.doJSON(ctx, http.MethodGet, "/api/fleet/agents?"+q.Encode(), nil, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Items...)
		if len(out.Items) == 0 || len(all) >= out.Total {
			return all, nil
		}
	}
}

func groupAgentVersions(agents []fleetAgent) []harness.AgentVersion {
	byVersion := map[string]*harness.AgentVersion{}
	for _, a := range agents {
		meta := a.LocalMetadata.Elastic.Agent
		v := meta.Version
		if v == "" {
			v = a.Agent.Version
		}
		v = snapshotVersion(v, meta.Snapshot)
		if v == "" {
			v = "unknown"
		}
		g, ok := byVersion[v]
		if !ok {
			g = &harness.AgentVersion{Version: v, Build: meta.Build.Original}
			byVersion[v] = g
		}
		g.Count++
		host := a.LocalMetadata.Host.Hostname
		if host == "" {
			host = a.ID
		}
		g.Hosts = append(g.Hosts, host)
	}
	out := make([]harness.AgentVersion, 0, len(byVersion))
	for _, g := range byVersion {
		sort.Strings(g.Hosts)
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Version < out[j].Version
	})
	return out
}

func agentVersionList(groups []harness.AgentVersion) string {
	vs := make([]string, len(groups))
	for i, g := range groups {
		vs[i] = g.Version
	}
	return strings.Join(vs, ",")
}

func snapshotVersion(number string, snapshot bool) string {
	if number == "" || !snapshot || strings.HasSuffix(number, "-SNAPSHOT") {
		return number
	}
	return number + "-SNAPSHOT"
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
