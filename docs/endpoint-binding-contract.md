# Endpoint Binding Contract

> Status: current. This document describes only the deterministic runtime
> binding engine. User-facing source CRUD, account subscriptions, instance
> probes, and confirmation cards are retired.

## Boundary

`tikhubcatalog` is the lookup catalog for one-off public research.
`capabilitycatalog` is the allowlist for recurring acquisition capabilities.
They are intentionally different:

- lookup results are untrusted text returned to the model and never enter
  `content_items`;
- recurring acquisition uses only reviewed `capabilitycatalog` entries;
- an approved task manual is compiled through `fetchspec`;
- the compiler materializes internal `fetch_targets` and the exact
  `task_fetch_targets` projection;
- users never bind, list, enable, disable, or delete target instances.

## Binding model

A binding is trusted code that maps one reviewed provider endpoint into the
canonical `ContentItem` shape:

```go
type Binding struct {
    Endpoint       string
    Params         map[string]string
    ItemsPath      string
    Fields         FieldMap
    Kind           types.Kind
    Enrich         *EnrichSpec
    Unwrap         string
    TimeoutSeconds int
}
```

The mapping language is deliberately sub-Turing: dotted field paths, constant
templates, a fixed time-format enum, bounded unwrap/enrich primitives, and
declared parameter substitution. It does not admit regex, XPath, JavaScript,
arbitrary predicates, or runtime-authored code.

Provider endpoint names and transport details remain in trusted binding
templates. `fetch_targets.config` contains only capability parameters from the
task plan; provider-specific routing is never model-authored or user-authored.

## Admission

A recurring capability may enter `capabilitycatalog` only after:

1. fixture tests cover a real response shape;
2. required identity fields are present;
3. declared timestamps parse correctly;
4. capability-specific order promises are verified;
5. parameter validation rejects unknown or missing fields;
6. paid enrichment is protected by the shared cost gate.

There is no per-user or per-target trial run. Exact acquisition identity is
`{platform, capability, url, config}`; title is display-only. Materialization
may reuse an existing target only when that full identity matches.

## Runtime

Every run:

1. loads the task’s frozen approved definition;
2. verifies exact equality with its frozen target identities;
3. resolves the trusted binding template;
4. validates parameters against the current reviewed capability;
5. stops before network or billing on identity drift;
6. calls the provider with bounded timeout and response size;
7. maps items through the declared binding;
8. rejects empty identities and response-shape drift;
9. records upstream attribution, cost, and failure health;
10. writes canonical content and provenance.

Provider failure is explicit. In particular, X capabilities use only the
reviewed TikHub route; they never fall back to an official X API, syndication,
or another third-party X data service.

## Failure and health

Network, authentication, rate-limit, endpoint-removal, parameter-drift, and
response-shape failures increment internal target health. A successful fetch
clears consecutive failure state. Task editing or re-approval resets automatic
suspension only for that task’s approved targets.

An empty or corrupt approved task plan fails closed before external calls,
database business writes, model scoring, or delivery.

## Current references

- `capabilitycatalog/`
- `fetchspec/`
- `fetcher/binding.go`
- `fetcher/identity.go`
- `docs/task-playbook-fetch-target-cutover.md`
