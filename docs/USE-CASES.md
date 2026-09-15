# Use Cases

## 1. Kubernetes log ingestion benchmark

**Goal:** Measure how fast elastic-agent ingests a known volume of Kubernetes container logs end-to-end.

### What it does

1. Creates a local [kind](https://kind.sigs.k8s.io/) cluster.
2. Deploys a Fleet-enrolled elastic-agent via a user-supplied manifest and waits for it to become Ready.
3. Runs an emitter Job that generates a fixed number of log lines across many short-lived containers.
4. Watches the Job until all completions finish, recording the start time.
5. Polls Elasticsearch until the expected log count appears, recording the stop time.
6. Queries the agent metrics index for CPU and memory usage over the run window.
7. Destroys the kind cluster (always, even on failure).
8. Prints a result table (or JSON) with throughput, latency, and resource usage.

### Inputs

| Flag | Env var | Description |
|------|---------|-------------|
| `--es-url` | `ES_URL` | Elasticsearch endpoint |
| `--es-user` | `ES_USER` | ES username |
| `--es-password` | `ES_PASSWORD` | ES password (env preferred) |
| `--agent-manifest` | — | Path to Fleet-enrolled elastic-agent manifest |
| `--emitter-manifest` | — | Path to emitter Job manifest (default: `emitter.yaml`) |
| `--expected-logs` | — | Number of log lines to wait for in ES |
| `--cluster-name` | — | kind cluster name (default: `bench`) |
| `--agent-ready-timeout` | — | How long to wait for elastic-agent pods to be Ready |
| `--ingest-timeout` | — | How long to poll ES for log ingestion before giving up |
| `--log-index` | — | ES index pattern for container logs (default: `logs-kubernetes.container_logs-*`) |
| `--metrics-index` | — | ES index pattern for agent metrics (default: `metrics-elastic_agent.*`) |
| `--json` | — | Emit JSON to stdout instead of a human-readable table |

### Outputs

| Field | Description |
|-------|-------------|
| `logs_expected` | Target log count |
| `logs_received` | Actual log count confirmed in ES |
| `ingest_duration` | Time from emitter Job start until expected log count reached in ES |
| `throughput_eps` | `logs_expected / ingest_duration` in events/sec |
| `cpu_avg_pct` | Average agent CPU % over the run window |
| `cpu_p95_pct` | p95 agent CPU % over the run window |
| `mem_avg_bytes` | Average agent RSS over the run window |
| `mem_max_bytes` | Peak agent RSS over the run window |

### Phase mapping

| Phase | Implementation |
|-------|---------------|
| Setup | Create kind cluster; write kubeconfig to temp file |
| Configure | Apply agent manifest; wait for pods Ready |
| Seed | *(no-op)* |
| Warmup | *(no-op)* |
| Execute | Apply emitter manifest; watch Job until all completions finish |
| Collect | Poll ES log count; query agent metrics index |
| Teardown | Delete kind cluster |
| Report | Print result table or JSON |

### Prerequisites

- `kind` and `docker` on `PATH`
- A Fleet-enrolled elastic-agent manifest (generated from Kibana Fleet UI)
- An Elasticsearch endpoint reachable from the machine running the benchmark

---

## 2. VM fleet log ingestion benchmark

**Goal:** Measure elastic-agent log ingestion throughput and resource usage across a fleet of VMs, with both per-agent and aggregate results.

### What it does

1. Verifies SSH connectivity to each VM in the target list.
2. Installs elastic-agent on every VM and runs the user-supplied Fleet enrollment command.
3. Copies the log generator binary to each VM.
4. Waits for all agents to check in to Fleet.
5. Generates test log files on each VM (**pre-generated mode**: written to disk before ingestion starts; **real-time mode**: written continuously during ingestion).
6. Triggers ingestion: in pre-generated mode, moves log files to the watched path; in real-time mode, starts the generator and the agent tails it simultaneously.
7. Polls Elasticsearch until the expected total log count (logs-per-agent × number of agents) is confirmed.
8. Queries the agent metrics index to compute per-agent CPU and memory usage over the run window.
9. Unenrolls and uninstalls elastic-agent from every VM (always runs, even on failure).
10. Prints per-agent and aggregate results as tables and time-series graphs.

### Modes

| Mode | Seed phase | Execute phase |
|------|-----------|---------------|
| `pregenerated` | Generator writes all log files to disk | Log files moved to watched path; agent begins reading |
| `realtime` | *(no-op)* | Generator writes lines continuously; agent tails in real time |

### Inputs

| Flag | Env var | Description |
|------|---------|-------------|
| `--hosts` | — | Comma-separated list of VM hostnames or IPs |
| `--hosts-file` | — | Path to a file with one host per line (alternative to `--hosts`) |
| `--ssh-user` | `SSH_USER` | SSH username |
| `--ssh-key` | `SSH_KEY` | Path to SSH private key |
| `--enrollment-command` | — | Full `elastic-agent enroll` command from Fleet UI |
| `--log-generator` | — | Local path to log generator binary (SCP'd to each VM) |
| `--log-generator-args` | — | Arguments passed to the generator on each VM |
| `--logs-per-agent` | — | Number of log lines each agent should ingest |
| `--log-path` | — | Path on each VM where logs are written and watched |
| `--mode` | — | `pregenerated` or `realtime` (default: `realtime`) |
| `--es-url` | `ES_URL` | Elasticsearch endpoint |
| `--es-user` | `ES_USER` | ES username |
| `--es-password` | `ES_PASSWORD` | ES password (env preferred) |
| `--log-index` | — | ES index pattern for ingested logs (default: `logs-*`) |
| `--metrics-index` | — | ES index pattern for agent metrics (default: `metrics-elastic_agent.*`) |
| `--agent-checkin-timeout` | — | How long to wait for all agents to appear in Fleet after enrollment |
| `--ingest-timeout` | — | How long to poll ES for log ingestion before giving up |
| `--json` | — | Emit JSON to stdout instead of tables and graphs |

### Outputs

**Per-agent table**

| Field | Description |
|-------|-------------|
| `host` | VM hostname or IP |
| `agent_id` | Fleet agent ID |
| `logs_expected` | Target log count for this agent |
| `logs_received` | Actual log count confirmed in ES |
| `ingest_duration` | Time from ingestion start until expected count reached |
| `eps` | `logs_expected / ingest_duration` for this agent |
| `cpu_avg_pct` | Average agent CPU % over the run window |
| `cpu_p95_pct` | p95 agent CPU % over the run window |
| `mem_avg_bytes` | Average agent RSS over the run window |
| `mem_max_bytes` | Peak agent RSS over the run window |

**Aggregate summary**

| Field | Description |
|-------|-------------|
| `total_logs_expected` | `logs_per_agent × num_agents` |
| `total_logs_received` | Sum of confirmed log counts across all agents |
| `total_ingest_duration` | Time from first agent start until all logs confirmed in ES |
| `aggregate_eps` | `total_logs_expected / total_ingest_duration` |

**Graphs** (time-series over the run window, one series per agent plus an aggregate)

- EPS over time
- CPU % over time
- RSS over time

### Phase mapping

| Phase | Implementation |
|-------|---------------|
| Setup | Verify SSH connectivity to all VMs in parallel |
| Configure | Install + enroll elastic-agent on each VM; copy log generator binary; wait for all agents to check in to Fleet |
| Seed | *(pre-generated mode only)* Run generator on each VM to write log files to disk |
| Warmup | *(no-op)* |
| Execute | *(pre-generated)* Move log files to watched path on each VM; *(real-time)* start generator on each VM |
| Collect | Poll ES until total expected log count is confirmed; query agent metrics index for all agent IDs |
| Teardown | Unenroll + uninstall elastic-agent on every VM (runs even on failure) |
| Report | Print per-agent table, aggregate summary, and time-series graphs |

### Prerequisites

- SSH key access to all target VMs
- Log generator binary compiled for the VM OS/arch
- An Elasticsearch endpoint reachable from the machine running the benchmark
- Fleet enrollment command from Kibana Fleet UI (includes enrollment token and Fleet Server URL)
