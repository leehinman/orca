<p align="center">
  <img src="orca.jpeg" alt="Orca logo" width="400">
</p>

# Orca

Orca orchestrates repeatable benchmarks from a YAML config. It provisions infrastructure, runs a workload, collects Elasticsearch and elastic-agent metrics, and prints a summary. Its first use case is benchmarking elastic-agent on Kubernetes.

## Build

```sh
go build -o bin/orca ./cmd/orca
```

## Usage

```sh
orca validate --config config.yaml
orca run --config config.yaml
```

| Command | Phases |
|---|---|
| `orca up` | `setup`, `configure`, `seed`; creates `run-<timestamp>/` |
| `orca bench` | `warmup`, `execute`, `collect`, `report`; repeatable |
| `orca down` | `teardown`, then removes `scratch/` |
| `orca run` | `up`, `bench`, `down` in one process |
| `orca validate` | checks a config file |

`bench` and `down` target the run in `.orca/current`; pass `--run <dir>` to pick another.

Common flags: `--config`, `--env-file` (repeatable), `--json`, and `--skip-teardown` (`run` only).

Results go to stdout and logs to stderr, so `orca run --json | jq` works.

## Steps

Phases are lists of steps. Available executors include `kind`, `kubectl`, `ansible`, `opentofu`, `shell`, `wait`, `elastic-agent-metrics`, `elasticsearch-metrics`, `fleet-agent-diagnostics`, and `stack-versions`. Each bench writes a `summary.txt` into `run-<timestamp>/bench-<n>/`.

## Docs

- [DESIGN.md](docs/DESIGN.md)
- [DATA-FLOW.md](docs/DATA-FLOW.md)
- [USE-CASES.md](docs/USE-CASES.md)
- [ANALYSIS.md](docs/ANALYSIS.md)
- [k8s_testing.md](docs/k8s_testing.md)
- [CLAUDE.md](CLAUDE.md): conventions and config details

## Development

```sh
go test -race ./...
golangci-lint run ./...
```
