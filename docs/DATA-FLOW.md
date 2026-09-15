# Inter-step data flow: survey and design

## Problem

Each benchmark phase is a sequence of steps invoking external tools (OpenTofu, Ansible, shell). Outputs from one step must be reachable as inputs to later steps, possibly in a later phase. Concrete examples:

- OpenTofu provisions VMs → their IPs reach the Ansible inventory in Configure
- `kind create cluster` → kubeconfig path reaches all subsequent k8s steps
- Ansible enrollment → Fleet agent IDs reach the ES metrics query in Collect
- Harness-captured phase start timestamps → EPS calculation in Report

---

## Survey of existing tools

### GitHub Actions
Two buses within a job. Writing `key=value` to `$GITHUB_OUTPUT` makes that value addressable as `${{ steps.<id>.outputs.key }}` by any later step. Writing to `$GITHUB_ENV` injects a variable into every subsequent step's environment. Across jobs, a job-level `outputs:` block promotes step outputs up one level; downstream jobs pull via `${{ needs.<job>.outputs.key }}`. Files move between jobs only through artifact upload/download (external store). All values are strings. Job output cap: 1 MB.

### Tekton
Two complementary channels. Results are strings (≤4 KB each) written by a Task to a well-known file path; the Pipeline wires them to downstream parameters via `$(tasks.<name>.results.<name>)` in YAML. For anything larger, a Workspace — a shared PVC or emptyDir volume — is declared once in the Pipeline and mounted into whichever Tasks need it; producers write files, consumers read them. Both channels require explicit producer declarations and consumer wiring. The 4 KB result limit is the sharpest constraint of any tool surveyed.

### Argo Workflows
Dual-channel with richer infrastructure support. Parameters are strings passed between steps via `{{steps.<name>.outputs.parameters.<name>}}` in YAML; the producer writes a value to a declared file path. Artifacts are files or directories the controller uploads to a configured object store (S3, GCS, Minio) after the producing step finishes, then downloads into the consuming step before it starts. The object-store backend means no practical size limit. Both channels require explicit producer and consumer declarations.

### Pulumi
Outputs are typed values (`Output<T>` promises) that remain opaque during a program run and are materialized only after apply, stored in the state backend. Within a stack, outputs flow automatically through the dependency graph — referencing one resource's output in another resource's argument creates the dependency implicitly. Across stacks, a `StackReference` reads exported outputs from a named stack's state file. Strongly typed (string, number, list, map, secret). No practical size limit; secrets encrypted at rest.

### OpenTofu / Terraform
Outputs declared in HCL `output` blocks are stored in state. Within a root module, child-module outputs bubble up only if explicitly re-exported. Across root modules, `terraform_remote_state` reads another state file and exposes its declared outputs (pull-based). Strongly typed. Security caveat: reading remote state exposes the full state snapshot, not just declared outputs.

### Ansible
Three in-process mechanisms. `register:` captures a task's full JSON result under a named variable accessible to all subsequent tasks on the same host. `set_fact:` promotes a computed value to a persistent host fact, available across plays. `hostvars[<hostname>].<varname>` lets any task on any host read facts or registered variables from any other host — but only after that host has been reached. Effectively untyped (Python dicts/lists/strings). No documented size limit.

### Packer
No inter-step data flow at build time. The `manifest` post-processor writes a JSON file after all builds complete, recording artifact IDs, file lists, timestamps, and custom metadata. Downstream tooling reads this file; nothing inside Packer consumes it. One-shot sink, not a channel.

---

## The three patterns

### Pattern 1: Named key-value bus (string channel)
*GitHub Actions, Tekton results, Argo parameters*

Producers write a named string to a well-known location (env file, declared file path). Consumers reference that name via template syntax in YAML. The orchestrator resolves names at runtime before launching the consuming step.

**Tradeoffs:** Simple to implement, universally compatible with external tools, zero infrastructure dependencies. Limited to small strings, no type safety, consumer must know producer names up front.

### Pattern 2: Shared filesystem / workspace
*Tekton workspaces, Argo artifacts, Ansible host facts*

Producers write files; consumers read files. In distributed systems the filesystem is mediated by an external store (PVC, S3). For a single-machine harness, a scratch directory owned by the orchestrator is the natural implementation.

**Tradeoffs:** No size constraint, naturally handles structured data and binary content, decouples format from the pipeline engine. Requires either a shared mount or an external store; paths must be communicated somehow.

### Pattern 3: Typed graph / state snapshot
*Pulumi, OpenTofu/Terraform*

Outputs are strongly typed values stored in persistent state; consumers pull from state explicitly. Typing catches mismatches early; secrets can be handled distinctly; state is queryable after the run.

**Tradeoffs:** Requires a state backend, adds a serialization layer, tightly coupled to the tool's own type system. Poor fit for a harness invoking heterogeneous external tools.

---

## Design for this harness

For a single-machine CLI harness invoking OpenTofu, Ansible, and shell commands, the right design is a **hybrid of patterns 1 and 2**, implemented entirely in-process.

### Phases are lists of steps

Each phase in the YAML config is a list. Each list item is a map with exactly one key — the step type — whose value is the step's configuration. This keeps the format uniform and makes it easy to mix step types within a phase.

```yaml
phases:
  setup:
    - id: provision
      opentofu:
        dir: ./infra
        action: apply
        outputs:
          vmIPs: { from: json_stdout, path: "$.vm_ips" }
    - ansible:
        playbook: ./playbooks/verify-ssh.yml
        vars:
          hosts: ${steps.provision.vmIPs}
```

### Named value store (pattern 1)

A `map[string]any` inside the harness acts as the shared state bus. Each step declares an `outputs:` block specifying what it contributes to the store — by parsing stdout as JSON, reading a specific file, or extracting a regex match. Later steps reference stored values via template expressions resolved by the harness before the step runs.

**Reference syntax: qualified by step ID** — `${steps.<id>.<key>}`. This prevents collisions when two steps produce keys with the same name and makes the data flow explicit and self-documenting. Steps that need their outputs referenced by later steps must declare an `id:` field.

### Per-run scratch directory (pattern 2)

A temp directory created at harness startup holds structured or large outputs: kubeconfig files, inventory files, manifest JSON. The harness exposes its path as a reserved key (`${scratch}`) so steps can construct paths into it. The harness deletes the directory at the end of the run (after Teardown).

### Harness-owned reserved keys

Phase-level metadata is captured automatically and injected into the value store:

| Key | Value |
|-----|-------|
| `${phase.<name>.start}` | RFC3339 timestamp when phase began |
| `${phase.<name>.end}` | RFC3339 timestamp when phase ended |
| `${phase.<name>.duration}` | Duration string, e.g. `3m42s` |
| `${scratch}` | Path to the per-run temp directory |

These make EPS and latency calculations in Report trivial without any step declaring them.

### The `noop` step type

`noop` is a first-class step type, not a special skip flag. It runs no external tool; its only effect is contributing a declared `state:` block to the value store. This is used when infrastructure was set up by hand — the `noop` step supplies what a real step would have discovered, so all downstream steps see the same keys regardless of how the infrastructure was created.

```yaml
phases:
  setup:
    - id: cluster
      noop:
        state:
          kubeconfigPath: /home/user/.kube/bench-config
          hosts:
            - 10.0.1.10
            - 10.0.1.11
```

The `id:` field is required on `noop` steps so that downstream steps can reference their state via `${steps.cluster.kubeconfigPath}`. A phase can mix `noop` steps with real steps if only part of the infrastructure was set up manually.

### Producer declaration is required

Following the Tekton/GitHub Actions convention: if a step doesn't declare an output (and isn't a `noop`), the harness doesn't capture anything from it. This keeps the YAML self-documenting and catches wiring errors at parse time, before any external tool runs.

---

## Key references

- [Passing data between steps and jobs in GitHub Actions](https://runs-on.com/github-actions/passing-data-between-jobs/)
- [How Tekton Pipelines provide seamless information flow](https://developers.redhat.com/articles/2023/11/02/how-tekton-pipelines-provide-seamless-information-flow)
- [Argo Workflows: Workflow Inputs](https://argo-workflows.readthedocs.io/en/latest/workflow-inputs/)
- [Pulumi: Stack References](https://www.pulumi.com/blog/iac-recommended-practices-using-stack-references/)
- [OpenTofu: terraform_remote_state](https://opentofu.org/docs/v1.7/language/state/remote-state-data/)
- [Ansible: Facts and magic variables](https://docs.ansible.com/projects/ansible/latest/playbook_guide/playbooks_vars_facts.html)
- [Packer: manifest post-processor](https://developer.hashicorp.com/packer/docs/post-processors/manifest)
