# Endpoint Binding Contract

> Status: current. This document describes only the deterministic runtime
> binding engine. Account-wide source CRUD, account subscriptions, instance
> probes, and confirmation cards are retired. Task-owned Sources remain.

## Boundary

`tikhubcatalog` contains one-off public-research endpoints. Scheduled
acquisition is exposed as versioned Agent Tools. They are intentionally
different:

- lookup results are untrusted text returned to the model and never enter
  `content_items`;
- scheduled acquisition uses only reviewed Tool definitions;
- the Agent selects Tools directly from the task manual;
- exact Tool calls are sealed in task-owned Sources, the approved task head
  and run snapshot;
- users manage Sources by natural-language task edits, never by internal ID.

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
templates. Canonical Tool arguments contain only public capability parameters;
provider-specific routing is never model-authored or user-authored.

## Admission

A scheduled acquisition Tool may become available only after:

1. fixture tests cover a real response shape;
2. required identity fields are present;
3. declared timestamps parse correctly;
4. capability-specific order promises are verified;
5. parameter validation rejects unknown or missing fields;
6. paid enrichment is protected by the shared cost gate.

There is no confirmation-card trial run. Exact invocation identity is the
versioned Tool name plus canonical arguments, scoped to one tenant/user/task
Source; display labels are not identity.

## Runtime

Every run:

1. loads the task’s frozen approved definition;
2. verifies the exact frozen Tool invocations;
3. resolves the trusted binding template;
4. validates parameters against the retained Tool version;
5. stops before network or billing on route drift;
6. calls the provider with bounded timeout and response size;
7. maps items through the declared binding;
8. rejects empty identities and response-shape drift;
9. records upstream attribution, cost, and the task Source/Tool invocation
   outcome;
10. writes canonical content plus task-isolated appearance provenance.

Provider failure is explicit. In particular, X capabilities use only the
reviewed TikHub route; they never fall back to an official X API, syndication,
or another third-party X data service.

## Failure and health

Network, authentication, rate-limit, endpoint-removal, parameter-drift, and
response-shape failures are recorded on the run's Tool invocation. They never
mutate a shared global acquisition object.

An empty or corrupt approved task plan fails closed before external calls,
database business writes, model scoring, or delivery.

## Current references

- `acquisitiontool/`
- `fetcher/binding.go`
- `fetcher/identity.go`
- `docs/task-manual-tool-runtime.md`
