# CLAUDE.md - Orca Guidelines

## Commands

### Build & Run
- Build binary: `go build -o bin/orca ./cmd/orca`
- Run local binary: `./bin/orca --help`
- Install to $GOPATH/bin: `go install ./cmd/orca`

### Testing
- Run all tests: `go test -race ./...`
- Run package tests: `go test -race ./harness/...`
- Run single test: `go test -race -run TestRunnerXxx ./harness`

### Quality & Linting
- Lint code: `golangci-lint run ./...`
- Tidy dependencies: `go mod tidy`

---

## Architecture & Layout

- `cmd/orca/` — Entry point; cobra command tree (`run`, `teardown`, `validate`)
- `harness/` — Core: config loading/validation, runner, state store, step interfaces
- `harness/steps/` — Step executor implementations (`kind`, `kubectl`, `shell`, `opentofu`, `elastic-agent-metrics`, `elasticsearch-metrics`, `fleet-agent-diagnostics`, …)
- `elastic/` — Elasticsearch query helpers (metrics, doc counts, aggregations)
- `samples/` — captured elastic-agent self-monitoring documents for process and otel mode; fixture data for the `elastic-agent-metrics` step tests, each with a README describing the document shapes

---

## CLI & Go Guidelines

### Exit Codes & Signal Handling
- Use `os.Exit()` only in `cmd/orca/main.go`. All other code returns errors.
- `signal.NotifyContext` is wired in `runner.Run`; `SIGINT`/`SIGTERM` cancel the main-phase context and trigger automatic teardown.

### Output Stream Separation
- Write results and data payloads (table, JSON) to **stdout**.
- Write phase logs, step progress, and error messages to **stderr**.
  This allows `orca run | jq` to work cleanly.

### Error Handling & Context
- Wrap errors with the operation that failed: `fmt.Errorf("create scratch dir: %w", err)`
- Pass `ctx context.Context` through all command and step calls to support cancellation.
- Surface clean messages to the user on stderr; do not print raw stack traces.

### State & Environment Variables
- Env vars are resolved at step execution time via `state.Resolve("${VAR}")`, not at startup.
- Structural keys (`scratch`, `results`, `steps.*`, `phase.*`) never fall back to env vars — they must be set explicitly by the runner or a step.
- `wait: {duration: 15m}` pauses for a fixed wall-clock duration, logging remaining time each minute; use it for an execute phase that must run for a set time regardless of ingestion lag.
- `${NAME:-default}` supplies a default when `NAME` is unset; validation accepts an unset env var only when a default is given, and structural keys are still validated by name.
- `${scratch}` is a per-run temp directory inside `run-<timestamp>/`; steps should use it for ephemeral files (kubeconfigs, inventory). It is removed after teardown completes.
- The `ansible` step sets `ANSIBLE_CONFIG` to an `ansible.cfg` found next to the playbook unless the variable is already set; ansible otherwise only reads `ansible.cfg` from the process cwd, which is the benchmark directory, not the playbook directory.
- An `ansible` step with an `id` may declare `outputs: {from: file, path: <json file>}`; after the playbook runs, each top-level key of that JSON object becomes `${steps.<id>.<key>}`. Step outputs persist in `orca-state.json`, so values captured during `up` are available to every `bench`.
- `elastic-agent-metrics` detects the agent self-monitoring sample period from the data (median spacing, default 1m when there are no samples) and uses it for the output grid and for summing per-file input rates; assuming 60s would over-count rates when the policy sets `agent.monitoring.metrics_period` to 15s. When more than one agent is configured, the summary also includes an `all_agents` section averaging each agent's own total memory/cpu (its `"total"` process row) across agents; with a single agent this section is omitted (`AgentMetricsSummary.AllAgents` is nil), not zero-filled.
- `elasticsearch-metrics` consolidates node stats collection (TSV + indexing/CPU/heap charts from `elasticsearch.stack_monitoring.node_stats`) with a per-data-stream document count over `since`..`until`, reporting observed events/sec; its summary is stored as JSON under `steps.<id>.summary` like `elastic-agent-metrics`. It also parses, from the same already-fetched node_stats docs (no extra ES query), write-path saturation (`thread_pool.write.{active,queue,rejected}`, reported as avg/max per node; `rejected` is a cumulative counter so it's reported as an avg/max rejected-per-second rate like `indexing_rate`, not a raw count), realized `_bulk` request shape (`indices.bulk.{avg_size,avg_time,operations.total}`), per-document indexing cost (`index_time` ÷ `index_total` delta), merge/segment growth, and indexing-pressure memory/rejections — written as `es-node[-slug]-threadpool-timeseries.tsv` and `es-node[-slug]-bulk-timeseries.tsv`, plus extra columns on the existing node timeseries TSV. The legacy `thread_pool.bulk` pool (retired in Elasticsearch 9.6.0, replaced by `write`) and `indexing_pressure` are each optional: their columns/charts/summary sections are only emitted when actually observed in the fetched docs (`EsMetricsSummary.BulkPoolObserved` / `.IndexingPressureObserved`), not zero-filled, so a 9.6+ cluster's summary doesn't show a dead legacy pool.
- `filebeat-wait` and `filebeat-metrics` read Filebeat's legacy `monitoring.elasticsearch` documents (`.monitoring-beats-*`, `type: beats_stats`; the cluster needs `xpack.monitoring.collection.enabled: true` or ES silently drops them). Queries use the `timestamp` field with millisecond-precision dates, because the index rejects RFC3339 without `.000`. Samples from a Filebeat process that started before `since` (`timestamp - uptime`) are ignored, so a restart at warmup cannot satisfy the gate with the previous run's counters. `filebeat-wait` succeeds at the first sample with `libbeat.output.events.acked >= max(expected, 1)` and outputs `reachedAt`; passing that as `filebeat-metrics`' `until` (exclusive) drops the partial final interval so the remaining rates are constant. Filebeat must set `monitoring.elasticsearch.metrics.period` (5s matches the poll interval).
- `stack-versions` queries Elasticsearch (`GET /`), Kibana (`/api/status`), and the Fleet agents API at run time and stores the build versions as JSON under `steps.<id>.summary`; the summary prints them in a `versions:` section. Versions are queried rather than taken from config because components can be upgraded between `bench` runs. Kibana and agent versions are skipped when `kibana:` is not configured.
- The top-level `variables:` map declares env var names with default values. `LoadConfig` exports them with `os.Setenv` before validation; precedence, highest first: the process environment, `--env-file` files in order (later wins), `orca.env` next to the config file, then the config defaults. Env files are `KEY=VALUE` lines (`#` comments, optional `export `, matching quotes stripped) and may set undeclared names such as `TF_VAR_*`. The summary prints every declared variable with its effective value under `variables:` and in JSON as `variables`; values are resolved per command, so changing `orca.env` between `bench` runs changes the next bench. Keep provider and `TF_VAR_*` entries stable across `up`, `bench`, and `down`. Do not declare secrets in `variables:` (they would be printed in `summary.txt`); set them only in `orca.env`, where undeclared names are allowed and still fail validation fast when missing.
- Dashboard PNGs in `elastic-agent-metrics` are best-effort: a render or download failure is logged, recorded as `dashboard_error` on that agent (printed as `dashboard: failed:`), and the step continues so the metrics and TSVs are never lost. `waitReport` retries transport errors until the dashboard timeout because Kibana may be restarting after Chromium was OOM-killed; the Reporting job survives the restart.
- The top-level `kibana:` block (`url`, `username`, `password`, `ca_path`) configures the Kibana client used by `fleet-agent-diagnostics` and by the `dashboard:` option of `elastic-agent-metrics`, which renders a per-agent PNG of a Kibana dashboard via the Reporting API. Reporting needs a trial or higher license, a Kibana with working headless Chromium (sandbox disabled on Ubuntu 24.04+), and `xpack.reporting.encryptionKey` set. Job status lives at `/internal/reporting/jobs/info/<id>`; the public download path returns 503 until the job completes.

### Lifecycle Commands
- Phases run in this order: `setup`, `configure`, `seed`, `warmup`, `execute`, `collect`, `report`, `teardown`.
- `orca up` runs `setup`, `configure`, `seed` and creates a new run directory.
- `orca bench` runs `warmup`, `execute`, `collect`, `report` against the current run; it can be repeated.
- `orca down` runs `teardown` against the current run, then removes `scratch/`.
- `orca run` is all three in one process; teardown runs automatically on failure unless `--skip-teardown` is set.
- `bench` and `down` default to the run recorded in `.orca/current`; pass `--run <dir>` to target another.

### Run Directory
- `orca up` and `orca run` create `run-<timestamp>/` in the working directory containing `scratch/` and `orca-state.json`.
- Each `bench` iteration creates `run-<timestamp>/bench-<n>/`; that directory is `${results}` for the bench phases and receives `summary.txt`. At the start of a bench the runner deletes `steps.<id>.*` for every step declared in the bench phases, so a step that fails or is skipped leaves no section in the summary instead of repeating the previous bench's results; a failed bench prints `error:` under the benchmark name and `error` in JSON.
- `${run}` resolves to the run directory itself.

---

## Testing

- Use table-driven tests for step executor logic and config validation.
- Inject mock `io.Writer` instances (e.g. `bytes.Buffer`) rather than referencing `os.Stdout`/`os.Stderr` directly — `Runner` accepts a `stderr` field for this purpose.
- Use `t.Setenv()` for env var overrides to ensure test isolation.
- Call `cdTemp(t)` in any test that invokes `runner.Run` to avoid creating `run-*/` directories in the source tree.
- The `harness` package has no dependency on cobra or terminal output; test it directly without CLI overhead.
