# Structured Insight Contract

## Status

- Phase: 2-A
- Risk: S (model protocol, immutable Brief payload, Temporal compatibility)
- Rollout: default off; exact compiled-task canary before any wider authority
- User-visible scope: none until the exact-task renderer is enabled

This batch upgrades the existing CardGen call. It does not add an LLM call,
generate a batch summary, merge events, or add search, bookmarks, trends,
daily/weekly/monthly reports.

## Invariants

1. One selected item still causes at most one CardGen LLM call.
2. Existing Temporal histories and frozen runtime policies retain
   `cardgen.render/v1` and the existing command/payload sequence.
3. A structured result is frozen once in the canonical Brief. Web and Feishu
   only project that frozen value; neither channel reparses Markdown or calls a
   model.
4. Every structured factual claim must cite source material supplied in the
   same CardGen request. Code validates references before persistence.
5. Invalid optional structured fields, claims, refs, or excerpts fail closed
   to the validated `body_md` carried by the same response. If the JSON
   envelope, schema, or `body_md` itself is invalid, the item follows the
   existing CardGen failure/partial-processing path. Neither case triggers a
   second LLM call.
6. Original title, source URL, source publication time and discovery time remain
   inventory-owned fields. The model cannot overwrite them.
7. Rollout disablement stops new v2 production but readers/renderers continue
   serving already-frozen structured Briefs.

## Wire schema

The new renderer returns exactly one JSON object:

```json
{
  "schema_version": "vane.cardgen-insight/v1",
  "body_md": "兼容 Phase 1 的完整、安全 Markdown 正文",
  "what_changed": "发生了什么",
  "why_it_matters": "为什么与当前任务有关",
  "importance_reason": "重要性判断依据",
  "claims": [
    {
      "text": "可展示的事实主张",
      "excerpt": "本次请求中可逐字定位的原文片段",
      "source_refs": ["source-1"]
    }
  ]
}
```

Rules:

- unknown or duplicate JSON fields are rejected;
- all strings are trimmed, valid UTF-8 and bounded;
- `body_md` is required and remains the only safe fallback presentation;
- `what_changed`, `why_it_matters` and `importance_reason` are either all
  valid or the structured projection is absent;
- claims are optional in the first rollout, but every present claim has at
  least one reference and one source excerpt;
- references must match request-owned opaque IDs; raw URLs, database IDs and
  model-invented IDs are rejected;
- each cited excerpt must occur in the normalized source input represented by
  every opaque ID listed by that claim;
- the title remains visible to CardGen but is not part of the excerpt evidence
  corpus; claims must be supported by sanitized source body bytes;
- no suggested action or importance tier is introduced in 2-A.

## Persistence

Structured fields are an optional, versioned extension of an immutable Insight,
not mutable delivery columns. CardGen adds an internal `evidence_digest` over
the exact opaque-ref/body corpus supplied to the model. The Store receives that
same bounded corpus, independently verifies every excerpt/ref and the digest,
then freezes only the digest with the structured payload. The Brief digest
covers the complete extension. Legacy `vane.brief/v1` rows remain byte-for-byte
valid and readable.

The Store must reject:

- structured payloads without a compatible schema version;
- a response-lost retry whose structured claim differs from the sealed Brief;
- cross-tenant/user/task/run references;
- claims whose references are absent from the exact frozen CardGen input;
- any update of an already-sealed Brief.

## Temporal and rollout

Use a new durable runtime label and `GetVersion` boundary before changing the
CardGen activity/result wire shape. Pre-2-A histories keep the old activity name
and payload. The new path uses an outcome-aware activity variant and preserves
the existing processing-completeness rules.

Rollout controls are independent:

- structured CardGen generation exact-task canary;
- structured Brief reader/renderer exact-task canary;
- explicit allow-all, which remains false during initial production UAT.

Generation is the intersection of its own scope and the reader/renderer scope.
The structured renderer canary may only target the exact task already inside
the compiled runtime, RunOutcome, canonical Brief writer and canonical renderer
scopes. Disabling generation stops new structured Briefs; readers continue to
serve previously frozen structured payloads from their durable runtime/schema
identity.

## Required proof

- strict parser table tests: malformed JSON, duplicate/unknown fields, limits,
  invalid UTF-8, missing/forged refs and excerpt mismatch;
- mutation: bypassing reference validation makes the claim test fail;
- Store: first seal, response-lost replay, conflicting retry, immutable digest,
  tenant/user/task denial and old Brief compatibility on PostgreSQL 18;
- Temporal replay: pre-2-A legacy, compiled, RunOutcome, Brief and renderer
  histories plus new v2 success/partial/failure histories;
- channel contract: same Brief ID, Insight ID, order, structured text, sources
  and timestamps in Web and Feishu;
- one real exact-task production run with no additional LLM span and no pending
  outcome/stage rows.

Natural `quiet` runs prove compatibility but do not prove structured content.
The exact canary remains enabled until a normal content run supplies that proof;
tests must not weaken observation rules or manufacture production evidence.
