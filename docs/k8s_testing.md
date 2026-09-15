# Implementation Plan: elastic-agent benchmark harness (Go)

---

## Goal

Build a general-purpose benchmark harness framework, then implement the elastic-agent Kubernetes benchmark as its first concrete use case.

---

## General framework: 8-phase lifecycle

Every benchmark run follows these phases. Not all phases are required for every use case — unused phases are no-ops.

| # | Phase | Purpose |
|---|-------|---------|
| 1 | **Setup** | Allocate physical or virtual infrastructure for the SUT and load generators |
| 2 | **Configure** | Install dependencies, set OS parameters, deploy application code |
| 3 | **Seed** | Populate databases or file systems with initial state |
| 4 | **Warmup** | Run preliminary, unrecorded load to prime caches, connection pools, JIT |
| 5 | **Execute** | The actual test phase |
| 6 | **Collect** | Gather result data |
| 7 | **Teardown** | Destroy the SUT and free provisioned resources (always runs, even on error) |
| 8 | **Report** | Generate artifacts (JSON, human-readable table, graphs) |

---

## Framework design

### Config-file driven

Benchmarks are defined entirely in a YAML config file. Users write no Go code. The CLI takes `--config <file>` plus env var overrides for secrets. Each phase is a list of steps; each step is a map with exactly one key (the step type) containing the step's configuration.

```yaml
benchmark:
  name: k8s-log-ingestion

elasticsearch:
  url: ${ES_URL}
  username: ${ES_USER}
  password: ${ES_PASSWORD}

phases:
  setup:
    - id: cluster
      kind:
        name: bench
        outputs:
          kubeconfigPath: { from: file, path: "${scratch}/kubeconfig" }

  configure:
    - kubectl:
        kubeconfig: ${steps.cluster.kubeconfigPath}
        manifest: ./agent.yaml
    - wait-pods:
        kubeconfig: ${steps.cluster.kubeconfigPath}
        namespace: default
        selector: app=elastic-agent
        timeout: 5m

  execute:
    - kubectl:
        kubeconfig: ${steps.cluster.kubeconfigPath}
        manifest: ./emitter.yaml
    - watch-job:
        kubeconfig: ${steps.cluster.kubeconfigPath}
        namespace: default
        job: emitter

  collect:
    - es-log-count:
        index: logs-kubernetes.container_logs-*
        since: ${phase.execute.start}
        expected: 48000
        timeout: 10m
    - es-agent-metrics:
        index: metrics-elastic_agent.*
        since: ${phase.execute.start}
        until: ${phase.collect.start}

  teardown:
    - kind-delete:
        name: bench
```

### `noop` step type

When infrastructure already exists, replace the real step with `noop`. Its `state:` block pre-populates the value store with whatever the real step would have discovered. Downstream steps are unaffected — they reference the same keys either way.

```yaml
phases:
  setup:
    - id: cluster
      noop:
        state:
          kubeconfigPath: /home/user/.kube/bench-config
```

### State / value store

A `map[string]any` inside the `Runner` is the shared state bus. Steps declare their outputs; the harness captures them after the step completes and makes them available to all subsequent steps via template expressions. The harness also auto-populates reserved keys:

| Key | Value |
|-----|-------|
| `${phase.<name>.start}` | RFC3339 timestamp when phase began |
| `${phase.<name>.end}` | RFC3339 timestamp when phase ended |
| `${phase.<name>.duration}` | Duration string, e.g. `3m42s` |
| `${scratch}` | Path to the per-run temp directory (cleaned up after Teardown) |

**Reference syntax: qualified by step ID** — `${steps.<id>.<key>}`. Steps that produce outputs referenceable by other steps must declare an `id:` field. Harness-owned reserved keys use `${phase.<name>.start}` and `${scratch}` (no step prefix).

### `Runner`

`Runner.Run(ctx, config)` owns orchestration:
- Reads and validates the config file at startup; fails fast if template references name undeclared keys
- Runs phases in order, recording timestamps into the value store
- Within each phase, runs steps in list order, resolving templates before each step executes
- Stops on the first error (except Teardown, which always runs)
- Registers `SIGINT`/`SIGTERM` handlers that cancel the root context — triggers deferred Teardown

### Framework-level decisions

- **Single Go module** — `harness/` and all benchmark implementations live in one module.
- **`Runner` logs phase transitions to stderr by default** — `[setup] starting`, `[setup] done in 3.2s`.
- **Reference syntax** — qualified by step ID: `${steps.<id>.<key>}` (see DATA-FLOW.md).

---

## Package structure

```
harness/                        ← framework (no external deps beyond stdlib)
├── config.go                   # BenchmarkConfig struct; YAML parsing and validation
├── runner.go                   # Runner.Run() — sequencing, timing, teardown guarantee
├── state.go                    # value store: map[string]any, template resolution
└── steps/                      # built-in step type implementations
    ├── noop.go                 # noop step: populates state from inline declaration
    ├── kind.go                 # kind step: create cluster, write kubeconfig to scratch
    ├── kind_delete.go          # kind-delete step: delete cluster
    ├── kubectl.go              # kubectl step: apply manifest
    ├── wait_pods.go            # wait-pods step: poll until pods Ready
    ├── watch_job.go            # watch-job step: watch batch/v1 Job to completion
    ├── es_log_count.go         # es-log-count step: poll ES _count until expected reached
    ├── es_agent_metrics.go     # es-agent-metrics step: date-histogram + stats aggregation
    ├── shell.go                # shell step: run arbitrary command, capture output
    ├── opentofu.go             # opentofu step: run tofu apply/destroy, parse JSON output
    └── ansible.go              # ansible step: run ansible-playbook subprocess

elastic/
├── logs.go                     # PollLogCount(ctx, es, index, startTime, expectedCount, timeout)
└── metrics.go                  # CollectAgentMetrics(ctx, es, index, startTime, stopTime) → MetricsResult

result/
└── result.go                   # BenchmarkResult struct, Print()/JSON() methods

internal/
└── poll/
    └── poll.go                 # WaitUntil(ctx, interval, fn func() (bool, error))

cmd/orca/
└── main.go                     # --config flag → load config → Runner.Run()
```

---

## k8s benchmark: phase mapping

| Phase | k8s implementation |
|-------|--------------------|
| Setup | `cluster.Create` — creates kind cluster, writes kubeconfig to temp file |
| Configure | `ApplyManifest(agentManifest)` + `WaitPodsReady` |
| Seed | *(no-op)* |
| Warmup | *(no-op)* |
| Execute | `ApplyManifest(emitterManifest)` + `WatchJobComplete` |
| Collect | `PollLogCount` + `CollectAgentMetrics` |
| Teardown | `cluster.Delete` |
| Report | `result.Print` |

`RunState.PhaseStartTimes["execute"]` provides the ES query start time without framework special-casing.

---

## k8s benchmark: inputs

The CLI takes two flags only:

| Flag | Description |
|------|-------------|
| `--config` | Path to the YAML benchmark config file |
| `--json` | Override output format to JSON (can also be set in config) |

All other inputs live in the config file. Secrets (`ES_URL`, `ES_USER`, `ES_PASSWORD`) are referenced as `${ENV_VAR}` inside the config and resolved from the environment at startup.

---

## k8s benchmark: key design decisions

**Job completion (Execute phase):** Use `client-go` Watch on the `batch/v1` Job, not polling. Check `job.Status.Succeeded >= *job.Spec.Completions` on each `MODIFIED` event. This avoids thundering-herd polling during the 1600-completion run.

**Log count wait (Collect phase):** ES `_count` API filtering `@timestamp >= PhaseStartTimes["execute"]`. Poll every 5 seconds via `poll.WaitUntil`. Using `execute` start time as a lower bound prevents re-runs from counting prior data.

**Agent metrics (Collect phase):** elastic-agent publishes internal beat metrics to `metrics-elastic_agent.*`. Target fields: `elastic_agent.beat.cpu.total.pct` and `elastic_agent.beat.memstats.rss`. Use a date-histogram aggregation over `[execute_start, collect_end]` plus `min`/`max`/`avg` and a percentiles aggregation for p95. Field names must be confirmed on first run.

**Kubeconfig:** `cluster.Create` writes the kubeconfig to a temp file stored in `RunState.KV["kubeconfigPath"]`; all subsequent k8s operations read from there. Avoids polluting `~/.kube/config`.

---

## Testing

### Unit tests (`go test ./...`)

| Package | What is tested | How |
|---------|---------------|-----|
| `harness/` | `Runner` phase sequencing; teardown runs on error; `RunState` timestamps populated | Pure logic tests |
| `elastic/logs.go` | `PollLogCount`: returns when count reached, times out, propagates ES errors | `httptest.NewServer` with canned `_count` responses |
| `elastic/metrics.go` | Query construction (correct time range, aggregation structure); result parsing | `httptest.NewServer`; assert request body matches expected DSL |
| `result/result.go` | JSON round-trips; table formatting | Pure function tests |
| `internal/poll/poll.go` | Returns on first `true`; context cancellation; error propagation | Counter function |
| `k8s/job.go` | Stops when `Succeeded >= Completions`; waits on intermediate events | Fake `watch.Interface` with pre-populated event channel |

### Integration tests (`go test -tags integration ./...`)

Require `kind` and `docker` on `PATH` and a real ES endpoint (set via env vars).

| Test | What it validates |
|------|------------------|
| `TestKindCreateDelete` | `cluster.Create` + `cluster.Delete` round-trip; kubeconfig valid after create |
| `TestApplyAndWatchJob` | Apply a minimal short-running Job (10 completions); `WatchJobComplete` returns within timeout |
| `TestPollLogCount` | Apply real emitter (small scale), wait for count; validates index pattern and query |
| `TestCollectAgentMetrics` | With agent running, query metrics index; validates field names and aggregation return non-zero values |

### Manual validation checklist (first run)

- [ ] Confirm elastic-agent pods reach Ready state before Execute phase starts.
- [ ] Confirm the log index pattern matches where Fleet actually routes container logs.
- [ ] Confirm `elastic_agent.beat.cpu.total.pct` and `elastic_agent.beat.memstats.rss` exist in the metrics index; if not, identify correct field names via Kibana Discover.
- [ ] Verify `kind delete` runs when the binary receives Ctrl-C mid-run.

---

## Open questions (k8s-specific)

1. **Log index pattern**: Fleet routes Kubernetes container logs differently depending on the integration in the policy. Confirm with the actual agent manifest.
2. **Agent metrics field names**: Depends on elastic-agent version. Verify on first run before hardcoding defaults.
3. **Expected log count**: `emitter.yaml` runs 1600 completions × 30 messages = **48,000 logs**. Is this the `--expected-logs` default, or should the flag have no default and always be required?
4. **`emitter.yaml`**: The emitter job manifest is referenced but not yet defined. Needs to be created alongside the benchmark binary.
