# Structured Event Evidence Contract

## Status

- Phase: 2-B0 (dark Store foundation)
- Risk: S (event identity, first-writer replay, future immutable Brief payload)
- Production call points: zero
- Migration: none
- Rollout: none in 2-B0; the existing Phase 2-A exact canary is unchanged

Phase 2-A has engineering and quiet-path production proof, but no natural
non-empty structured Brief yet. This batch therefore does not advance that
canary or change CardGen, Temporal, Brief, API, Feishu, Web, LLM, quota, push,
observation admission, or recovery behavior.

## Purpose

The next structured-analysis step needs one durable identity for the qualified
event that owns an Insight. `task_observed_events` already enforces event
deduplication and binds a qualified event to one exact delivery, but its current
reservation API returns only a boolean. A later versioned runtime would
otherwise have to rediscover or guess the event row when freezing multi-source
evidence into a Brief.

2-B0 adds a zero-call-point Store primitive that returns a sealed
`ObservedEventProvenanceV1` while reusing the exact existing reservation,
replay, stale takeover, push-effect and tenant/user/task/run fences.

## Provenance

`ObservedEventProvenanceV1` contains:

- observed-event row ID;
- schema version;
- policy digest and event key;
- event type and subject;
- canonical UTC/microsecond occurrence time;
- a digest of the canonical `evidence_json` value.

The Store obtains these fields from the authoritative
`task_observed_events` row inside the reservation transaction and exposes them
only after commit. It never seals caller bytes after an exact replay: if a
response-lost caller retries with different event fields, the original
first-writer row remains authoritative and the returned provenance still
matches that stored row.

The evidence digest canonicalizes JSON object key order and insignificant
whitespace. It changes when the semantic evidence value changes. Raw evidence
JSON and content bodies are not copied into the provenance object.

## Invariants

1. `ReserveObservedEventV1` behavior and signature do not change.
2. The new primitive has zero non-test callers in 2-B0.
3. First reservation, exact replay and frozen push-effect replay return the
   same observed-event row identity.
4. Cross-run duplicates remain rejected unless the existing bounded stale
   takeover rules admit the new exact run.
5. The returned provenance is loaded under exact tenant, user, task, snapshot
   and Temporal RunID scope.
6. Driver error chains remain behind the existing controlled database error.
7. No Brief schema, API DTO, renderer or Temporal payload changes in 2-B0.

## Required proof

- pure type tests for canonical JSON, time normalization and tamper rejection;
- PostgreSQL 18 reservation/replay test proving caller drift cannot replace
  the stored first writer;
- existing observed-event concurrency, stale takeover and push-effect suites;
- mutation proof: sealing caller evidence instead of the committed row must
  fail the first-writer replay test;
- AST guard proving zero production callers;
- affected race, full Store race, full repository race, vet and build.

## Later wiring boundary

A separate 2-B1 batch may introduce a new durable runtime label and CardGen
Activity for multi-source event evidence. It must keep one paid CardGen call,
load only content IDs admitted by the frozen qualified event and run snapshot,
freeze inventory-owned source metadata, and preserve the Phase 2-A exact
canary until a natural non-empty run proves claims/evidence end to end.

2-B0 does not authorize 2-B1, batch summaries, extra LLM calls, search,
bookmarks, trends, reports, default experience changes or a wider user scope.
