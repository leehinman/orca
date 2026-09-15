# Orca Design

## Purpose

Orca is a YAML-driven benchmark harness for elastic-agent ingestion. A config file describes the infrastructure to build, the workload to run, and the metrics to collect from Elasticsearch, Kibana and the agents. Orca owns phase sequencing, timing, teardown guarantees, signal handling, metric collection and result reporting. External tools (kind, kubectl, OpenTofu, Ansible, shell) run as typed steps. Users write no Go.

Related documents: [USE-CASES.md](USE-CASES.md) (target scenarios), [ANALYSIS.md](ANALYSIS.md) (why a custom harness rather than Ansible/Earthly/etc.), [DATA-FLOW.md](DATA-FLOW.md) (inter-step data passing survey), [k8s_testing.md](k8s_testing.md).

## Goals and non-goals

Goals:

- Repeatable, comparable runs: one setup can be benchmarked many times with different parameters.
- Guaranteed teardown, even on failure or SIGINT/SIGTERM.
- Direct measurement of agent resource use and Elasticsearch saturation rather than inference.
- Machine-readable results (`--json`) alongside a human-readable `summary.txt`.

Non-goals:

- Orca is not a general workflow engine: there is no DAG, parallelism or conditionals. Steps in a phase run sequentially.
- Orca does not manage secrets beyond reading them from the environment or `orca.env`.

## Architecture

```
cmd/orca/            cobra CLI: run, up, bench, down, validate; registers executors
harness/             config loading, runner, state, env handling, result model
harness/steps/       one executor per step type
elastic/             Elasticsearch query helpers (doc counts, histograms, aggs, log polling)
samples/             captured agent self-monitoring documents (test fixtures)
```

`harness` has no dependency on cobra or terminal output. `cmd/orca` is the only place that calls `os.Exit` and the only place that wires step factories to the runner, injecting `cfg.Elasticsearch` / `cfg.Kibana` into the factories that need them.

## Core concepts

### Config

`harness.LoadConfig` parses YAML into `Config`: `benchmark`, `variables`, `elasticsearch`, `kibana`, `phases`. Before decoding, `!repeat` directives (`harness/repeat.go`) are expanded: a `!repeat {count, as, template}` node becomes `count` copies of the template with `${as}` replaced by the index. Step executors therefore only see fully materialized lists. `count` resolves from variables and the process environment only, since step outputs do not exist at load time.

Validation runs at load time and fails fast: unknown fields, steps with zero or multiple types, unresolvable `${...}` references (unset env vars are accepted only with a `:-default`), and missing required settings. `orca validate` runs this alone.

### Phases

Fixed order: `setup`, `configure`, `seed`, `warmup`, `execute`, `collect`, `report`, `teardown`. Each phase is an ordered list of steps. The runner records start, end and duration of each phase in state (`phase.<name>.*`), which steps use for query windows.

Phases are grouped by lifecycle command:

| Group | Phases | Command |
|---|---|---|
| up | setup, configure, seed | `orca up` |
| bench | warmup, execute, collect, report | `orca bench` (repeatable) |
| down | teardown | `orca down` |

`orca run` runs all three in one process. Teardown runs automatically after a failure unless `--skip-teardown` is set.

### Steps

A `Step` is a struct with an optional `id`, an `optional` flag, and exactly one non-nil typed field (`shell`, `kind`, `opentofu`, ...). `typeName()` picks the type; the runner looks up a registered `ExecutorFactory`, builds an `Executor`, and calls `Run(ctx, state)`.

```go
type Executor interface { Run(ctx context.Context, state *State) error }
type ExecutorFactory func(Step) (Executor, error)
```

A failing step aborts its phase unless `optional: true`, in which case it is logged and skipped. A phase that has already started still records its end time and duration on failure.

Step types, by role:

| Role | Types |
|---|---|
| Infrastructure | `kind`, `kind-delete`, `kind-load`, `kubectl`, `wait-pods`, `watch-job`, `opentofu`, `ansible` |
| Generic | `shell`, `wait`, `noop`, `write-ini` |
| Elasticsearch queries | `es-doc-count`, `es-log-span`, `es-agg`, `es-tsv` |
| Metrics and reporting | `elastic-agent-metrics`, `elasticsearch-metrics`, `stack-versions`, `fleet-agent-diagnostics`, `tsv-derive`, `chart` |

Adding a step type requires: a config struct, a field on `Step`, a `typeName()` case, validation, an executor in `harness/steps/`, and a `Register` call in `main.go`.

### State

`State` is a string-keyed store shared by every step in a run and persisted to `orca-state.json` in the run directory. It holds:

- structural keys set by the runner: `scratch`, `results`, `run`, `phase.<name>.{start,end,duration}`
- step outputs: `steps.<id>.<key>`, including JSON `summary` blobs consumed by the report

`State.Resolve` expands `${NAME}`, `${NAME:-default}` and `${steps.<id>.<key>}` at step execution time, not at startup. Structural keys never fall back to environment variables. Persisting state means values captured during `up` (VM IPs, agent IDs, kubeconfig paths) are available to every later `bench`, and `down` can run in a separate process.

Step outputs come from three sources: the step's own logic (e.g. `kind` sets the kubeconfig path), `outputs:` on `opentofu`/`ansible` steps (each top-level key of a JSON file becomes `steps.<id>.<key>`), and summaries written by the metrics steps.

### Variables and environment

Top-level `variables:` declares names with defaults. `LoadConfig` exports them with `os.Setenv` before validation. Precedence, highest first:

1. process environment
2. `--env-file` files, in order (later wins)
3. `orca.env` next to the config
4. config defaults

Values resolve per command, so editing `orca.env` between benches changes the next bench. Declared variables are printed in the summary, so secrets belong only in `orca.env`, never in `variables:`. Provider and `TF_VAR_*` values must stay stable across `up`, `bench` and `down`.

### Run directory

`orca up` / `orca run` create `run-<timestamp>/` containing `scratch/` and `orca-state.json`, and write its path to `.orca/current`. Each bench creates `run-<timestamp>/bench-<n>/`, which is `${results}` for that bench and receives TSVs, charts and `summary.txt`. `${scratch}` holds ephemeral files (kubeconfigs, inventory) and is deleted when `down` succeeds. `bench` and `down` default to the run in `.orca/current`; `--run <dir>` overrides.

At the start of a bench the runner deletes `steps.<id>.*` for every step declared in the bench phases. A step that fails or is skipped therefore leaves no section in the summary rather than repeating the previous bench's numbers.

## Execution flow

```
up:     newRun -> setup -> configure -> seed -> save state
bench:  load state -> mkdir bench-n -> clear step outputs -> warmup -> execute -> collect -> report -> save state
down:   load state -> teardown -> rm scratch -> clear .orca/current
run:    up + bench + down (down always, unless --skip-teardown)
```

`signal.NotifyContext` is installed in `Run`, `Up` and `Bench`. SIGINT/SIGTERM cancel the context, the current step returns, remaining steps are skipped, and `run` proceeds to teardown using a fresh `context.Background()` so teardown is not itself cancelled.

## Metrics and reporting

Collection steps query Elasticsearch (and Kibana) over the `execute` window, using phase timestamps from state.

- **`elastic-agent-metrics`**: per-agent CPU and memory by process from agent self-monitoring data, plus per-file input rates. The sample period is detected from the data (median spacing, default 1m) rather than assumed, because `agent.monitoring.metrics_period` can be changed. With several agents it adds an `all_agents` average. Optionally renders a Kibana dashboard PNG per agent through the Reporting API; failures are logged and recorded as `dashboard_error` without failing the step.
- **`elasticsearch-metrics`**: node stats TSV and charts from `elasticsearch.stack_monitoring.node_stats`, per-data-stream document counts and events/sec, write thread-pool saturation, `_bulk` request shape, per-document indexing cost, merge/segment growth and indexing pressure. All derived from the same fetched documents with no extra query. Legacy `thread_pool.bulk` and `indexing_pressure` sections appear only when observed.
- **`stack-versions`**: queries ES, Kibana and the Fleet agents API at run time for build versions, since components may be upgraded between benches.
- **`fleet-agent-diagnostics`**: downloads agent diagnostic bundles through Kibana. Fetch failures are non-fatal.
- **`es-*`, `tsv-derive`, `chart`**: generic building blocks for custom series: count/aggregate/poll, derive columns from TSVs, render charts.

Each metrics step stores a JSON summary under `steps.<id>.summary`. `BuildResult` (`harness/result.go`) assembles these into `BenchmarkResult`, which `Print` renders as text to `summary.txt` and `JSON` emits as structured output.

Kibana Reporting requires a trial or higher license, working headless Chromium and `xpack.reporting.encryptionKey`. `waitReport` retries transport errors until the dashboard timeout because Kibana may restart after Chromium is OOM-killed; the report job survives.

## Conventions

- Results (tables, JSON) go to stdout; phase logs, progress and errors go to stderr, so `orca run --json | jq` works.
- Errors are wrapped with the failing operation (`fmt.Errorf("create scratch dir: %w", err)`); no stack traces reach the user.
- `context.Context` is threaded through every command and step.
- The runner takes a `stderr io.Writer` so tests capture output; tests that call `Run` use `cdTemp(t)` to keep `run-*/` out of the source tree.
- Table-driven tests for step executors and config validation; `t.Setenv` for env overrides.

## Key design decisions

| Decision | Rationale |
|---|---|
| Typed steps in YAML, not a scripting language | Config is declarative and validated up front; unusual needs fall back to `shell`/`ansible`. |
| Split `up` / `bench` / `down` | Provisioning is slow; one environment supports many parameter sweeps. |
| Persistent JSON state | Lets lifecycle commands run in separate processes and carry outputs across them. |
| Late `${...}` resolution | Step outputs and phase timestamps do not exist at startup. |
| `!repeat` expanded before decode | Scales agent fleets without every executor handling multiplicity. |
| Query-time versions and sample periods | Avoids stale or wrong assumptions baked into config. |
| Best-effort artifacts (dashboards, diagnostics) | A flaky render must not lose the core metrics. |
| Bench clears prior step outputs | Prevents silently reporting stale numbers. |

## Known limitations

- Strictly sequential steps; no parallel execution within a phase.
- No step-level retry or conditional logic beyond `optional`.
- `!repeat` counts cannot depend on step outputs.
- The Beats `elasticsearch` `node_stats` metricset does not parse the `write_coordination` and `merge` thread pools (see `METRICS_PLAN.md`), so those are not available until upstream is fixed.
