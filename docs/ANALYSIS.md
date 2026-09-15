# Pipeline runner analysis

## Context

We are building a YAML-driven benchmark harness framework in Go. The harness owns phase sequencing, timing, teardown guarantees, signal handling, Elasticsearch querying, and result reporting. External tools (OpenTofu, Ansible, shell commands) are invoked as typed steps within each phase. The YAML config file defines what runs in each phase; Go code is not required from the user.

A `NoopHarness` allows phases to be skipped when infrastructure has already been set up by hand — the user supplies the outputs those phases would have produced (e.g., an existing kubeconfig or VM IP list) directly in the config.

---

## Tools evaluated

### Ansible
**Fit for k8s benchmark:** Poor as the overall orchestrator. `block/rescue/always` gives a teardown guarantee but it's clunky. Watching a Kubernetes Job to completion, polling ES with precise timestamps, and computing EPS don't fit the Ansible model at all.

**Fit for VM fleet benchmark:** Excellent for the install/enroll/copy steps. Wrong tool for timing the ingestion window or querying ES.

**Data flow:** Facts and registered vars work within a playbook, but passing structured data (e.g., provisioned VM IPs) *out* to the next tool requires writing files.

**Verdict:** Right as a *step type* inside the harness. Wrong as the harness itself.

---

### Earthly
**Fit for k8s benchmark:** Everything runs inside containers. Running `kind` inside Docker-in-Docker is painful and non-standard.

**Fit for VM fleet benchmark:** Cannot SSH to remote VMs at all. Fundamentally wrong model.

**Verdict:** Poor fit for both use cases. Designed for reproducible builds, not infrastructure orchestration.

---

### Dagger
**Fit for k8s benchmark:** Same container-centric problem as Earthly. Strong Go SDK, but the build/pipeline model doesn't match a benchmark harness.

**Fit for VM fleet benchmark:** No remote SSH capability. Ruled out for the same reason as Earthly.

**Verdict:** Poor fit. Worth watching for fully containerized future use cases, but not today.

---

### Temporal
**Fit for k8s benchmark:** Excellent conceptual fit. Workflow Activities map directly to phases. Durable execution means a crash mid-benchmark can resume. Saga/compensation pattern handles the teardown-always-runs requirement natively.

**Fit for VM fleet benchmark:** Same strengths. Long-running benchmarks (potentially hours) benefit from Temporal's durability. Activity return values flow naturally between phases, solving the data-flow problem cleanly.

**Data flow:** Best of the six. Activity outputs are first-class workflow state.

**The catch:** Requires a running Temporal server (or Temporal Cloud). That's real operational overhead for a tool engineers run ad-hoc. Also requires all orchestration logic to be written in the Temporal Go SDK — no config file, phases are code.

**Verdict:** Architecturally the closest match to what we're building. Too heavy for a benchmark CLI tool.

---

### Taskfile
**Fit for k8s benchmark:** Can chain `kind`, `kubectl`, and shell commands. Timing is just wall-clock shell execution — no built-in capture or phase timestamps.

**Fit for VM fleet benchmark:** Local-only. Cannot SSH to remote hosts from the task definition. Would still need Ansible for everything remote.

**Data flow:** Environment variables and files only. No structured passing of outputs between tasks.

**Verdict:** Too simple. Loses the precise timing, programmatic ES querying, and structured result collection that make this tool useful.

---

### Mage
**Fit for k8s benchmark:** Strong. Tasks are Go functions — you get `client-go`, the ES client, `time.Now()`, and `defer` for teardown, all natively. This is essentially the harness we were already designing, with Mage providing the task runner scaffolding.

**Fit for VM fleet benchmark:** Can shell out to Ansible/OpenTofu and capture their output. Data flow is just Go variables between function calls.

**Data flow:** Best of the lightweight options — it's Go, so anything goes.

**The catch:** Users write Go, not YAML. The config-file flexibility we discussed goes away. Extensibility requires editing Go source.

**Verdict:** Closest to what we're building. The question is: do we want users to configure runs via a YAML file, or write Go? Answer: YAML.

---

## Summary table

| Tool | k8s fit | VM fleet fit | Data flow | Timing precision | Operational cost |
|------|---------|-------------|-----------|-----------------|-----------------|
| Ansible | Poor | Good (steps only) | File-based | None | Low |
| Earthly | Poor | Poor | N/A | N/A | Low |
| Dagger | Poor | Poor | N/A | N/A | Low |
| Temporal | Good | Good | Excellent | Excellent | High (needs server) |
| Taskfile | Partial | Poor | Weak | None | Low |
| Mage | Good | Good | Excellent | Excellent | Low |

---

## Conclusions

None of the evaluated tools replace the custom harness. The combination of external tool orchestration, precise timing, ES querying, and structured benchmark reporting is not handled end-to-end by any of them.

**Design decisions:**

- The harness is YAML-config-driven. Users write no Go code.
- Ansible and OpenTofu are first-class step types the harness invokes as subprocesses.
- `NoopHarness` (or per-phase skip) allows users to supply pre-existing infrastructure state directly in the config, bypassing the steps that would have produced it.
- Data passing between steps is a critical open design question — see data flow survey (DATA-FLOW.md).
- Temporal's model (named, typed activity outputs as first-class workflow state) is the strongest design reference for our inter-step data flow.
