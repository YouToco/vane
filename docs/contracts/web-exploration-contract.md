# Web exploration contract

`探索视野` is a small Web-only companion to the ordinary personalized Brief.
It is not a second push feed and it has no Feishu renderer.

## Product boundary

- Ordinary Brief ranking remains authoritative. Exploration cannot change,
  re-rank, or settle a canonical Brief.
- A candidate must already pass relevance and evidence validation. Exploration
  does not make weak or unrelated content acceptable.
- A candidate already selected into the ordinary Brief, or recently surfaced
  in exploration, is excluded.
- Selection requires a complete canonical/recent/muted-direction snapshot.
  Missing any set fails closed; an omitted snapshot is never treated as empty.
  The snapshot carries an exact tenant/user/task identity plus durable receipt
  ID and digest; an independent verifier re-proves both completeness and scope.
- The Web projection contains at most three items and favors one item per
  boundary reason before filling remaining room by quality.
- The only reasons are `challenges_judgment`, `adjacent_opportunity`, and
  `new_source`. User-facing clients localize those enums; model-authored
  free-form categories are not accepted.
- The only feedback values are `inspiring`, `off_target`, and
  `mute_direction`. They are separate from canonical Brief feedback. Every
  candidate carries a stable opaque direction key, and muted directions are
  excluded by the next complete snapshot.

## Security and channel boundary

- The public projection contains public source metadata and verified evidence
  references only. It excludes raw source bodies, profile text, prompts,
  digests, provider details, and internal provenance.
- A candidate must bind a durable evidence receipt ID and digest, and an
  independent verifier must re-prove that receipt before selection. A plausible
  URL or caller-supplied boolean is not evidence.
- Source and evidence links must be absolute HTTP(S) URLs without credentials.
- Hostless, localhost, loopback, private, link-local, and unspecified literal
  addresses fail closed; any future server-side fetch must re-check the
  resolved network target against the same public-network boundary.
- V1 rejects URL query strings and fragments entirely; it also rejects
  duplicate evidence URLs even when they use different refs. Default ports are
  normalized and encoded raw paths fail closed.
- The projection envelope is fixed to `channel=web`. The package imports no
  delivery, Feishu, workflow, LLM, or store implementation.
- Exploration never creates task actions. A future “monitor this direction”
  action must enter the existing proposal-and-confirm control plane.

## Rollout sequence

1. Land the pure selector and schema (`exploration.SelectV1`) with no production
   call point. An AST import guard keeps that boundary at zero.
2. Add a bounded producer and durable receipt after the task runtime cutover
   freezes its new snapshot contract.
3. Add a read-only task-scoped API and Web panel. Keep Feishu and bot renderers
   at zero call points.
4. Canary one task, verify no duplicate/canonical item leakage, then widen only
   the Web reader.
