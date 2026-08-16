# Telegram Bot channel contract (routed ingress v4)

Status: implemented, default-off, production UAT pending.

This contract adds Telegram as an authenticated, routed entrance to the same
Vane user Agent used by Web and Feishu. One authenticated user may authorize a
private chat, groups, supergroups, and exact forum topics. Telegram IDs remain
routing data, never Vane user IDs.

## Scope

Included in v4:

- startup `getMe`, exact HTTPS `setWebhook`, `getWebhookInfo` verification;
- explicit `allowed_updates=["message","callback_query","my_chat_member"]`, `max_connections=1` and
  `drop_pending_updates=false`;
- constant-time webhook-secret verification and a 1 MiB body limit;
- private text plus explicitly installed group/supergroup/forum-topic routes;
- group admission only for the bound user, and only for commands, an exact
  `@bot_username` mention, or a reply to a Bot message;
- administrator-verified, ten-minute, one-time group/topic installation links;
- Bot command menu (`help`, `status`, `tasks`, `new`, `connect`) and command
  menu button installed during startup;
- HMAC-authenticated inline callbacks admitted through the same durable inbox;
- exact `message_thread_id` preservation and provider-neutral
  `(provider,app,chat,thread,message)` mappings;
- Agent history isolated by the authenticated internal route ID: private chat,
  each group, and each forum topic have distinct TTL sessions, and raw Telegram
  actor/chat/thread IDs never become Agent session keys;
- provider-authenticated group-to-supergroup migration retargets only
  pre-provider route/effect state while preserving the internal route/session
  identity; Bot removal/block and topic close/hide revoke affected authority
  before normal message admission;
- a provider-neutral durable outbound effect boundary with stable caller UUID,
  frozen route/payload, exact replay, and terminal sent/failed/ambiguous
  settlement. Connection tests use it; future product notifications can reuse
  it without bypassing Telegram's no-blind-retry rule;
- explicit media recognition with a safe text-only capability reply; a
  provider-neutral `vane.channel-message/v1` metadata envelope is durably
  frozen with the ingress receipt, while media bytes are not downloaded or
  silently passed to the model;
- Web-session-issued, ten-minute, one-time `/start` pairing links;
- hash-only pairing-token storage and exact actor/chat/bot binding;
- durable `(provider, bot identity, update_id)` inbox receipts;
- deterministic UUID Agent turns and completed-turn replay before model/Tool;
- plain-text replies split at 4096 runes in order;
- provider ambiguity that blocks resend instead of duplicating a maybe-sent
  message;
- authenticated status/link/test/unlink Dashboard APIs and Settings UI.

Not included in v4:

- channel-post ingestion, inline queries, Telegram Login, reactions, edits, or
  arbitrary group-member Agent access;
- media download/transcription/vision, feedback/deep-dive domain actions, and
  destructive callbacks. Their transport extension points exist but remain
  fail-closed until their own authority contracts are implemented;
- Telegram as the primary scheduled Brief/periodic-report delivery policy.

The scheduled-delivery exclusion is deliberate. The transport/outbound effect
foundation exists, but current approved task definitions still say
`owner_feishu`, Research V3 target resolution is Feishu-specific, and legacy
delivery/feedback receipts retain `feishu_message_id`. Telegram `sendMessage`
has neither the provider UUID nor the arbitrary history lookup used by the
Feishu recovery contract. Scheduled Telegram delivery therefore requires a
separate S-level task-policy and provider-receipt migration. Until that is
implemented and UATed, product feature 4.9 remains `开发中`.

## Identity and authorization

The only Telegram principal derivation is:

`(provider=telegram, getMe bot id, from.id, private chat.id)`

→ active `channel_identities` row

→ active `memberships(tenant_id,user_id)` and active tenant

→ existing Vane `(tenant_id,user_id)` principal.

Username and display name are never identity. For private chat,
`message.chat.id` must equal `message.from.id`. An unbound actor is acknowledged
without any Agent, Tool, Temporal or business-data write. Web pairing APIs take
tenant/user only from `auth.PrincipalFromContext`; request parameters cannot
choose the target principal.

A `channel_identity` authenticates the human; a `channel_route` independently
authorizes one destination. Private pairing creates both atomically. A group or
topic install requires the same bound actor and Telegram `getChatMember` must
prove `creator` or `administrator`. Forum routes freeze the exact numeric
`message_thread_id`; another topic cannot fall back to that route. Identity and
route status are both rechecked under row locks before inbox admission and send
claim.

A bot-generation change is bound to the immutable numeric bot ID returned by
`getMe`. Successful pairing to a new bot ID revokes the same Vane user's older
active Telegram bot binding in the same transaction. Old webhook secrets and
old bot identities cannot resolve the new principal.

## Ingress and Agent replay

`channel_ingress_receipts` has a database primary key on
`(provider,app_identity,provider_update_id)` and a unique stable Agent turn UUID.
An exact duplicate is accepted as replay; the same update identity with changed
payload or principal conflicts.

The worker may reclaim only `pending`, or `processing` after its pre-send lease
expires. It calls `agent.Loop.HandleChannelMessage` with the deterministic turn
UUID. A completed `agent_turn_records` row returns before any model/Tool call;
an interrupted retry keeps the same Tool idempotency namespace.

Ingress v3 also fixes both provider and process concurrency to one connection
and one worker. The Store orders numeric Telegram update IDs and keeps one
`pending|processing|reply_ready` head per channel identity. A provider-crossed
`sending` row is terminal for FIFO purposes: it remains an operator-visible
blocked audit but cannot permanently stall future messages after a crash.

State transitions:

`pending → processing → reply_ready → sending → completed`

Agent failure becomes a durable, content-free user reply that asks the owner to
check Web state before retrying; it does not guess whether a Tool committed.
Once state crosses to `sending`, no automatic recovery path may send again. A
timeout, network disconnect, HTTP 5xx, redirect,
malformed success or partial multi-chunk result ends at `ambiguous`. A crash in
`sending` remains visibly blocked. This prefers a missing reply over a duplicate
external message because Telegram Bot API has no caller idempotency key.
An explicit non-429 4xx before the first chunk is recorded as definite
`failed`, not ambiguous. A 429 with a valid provider `retry_after` is the one
safe retry case: before any chunk is accepted it returns to `reply_ready` (or
outbound `prepared`) with a durable `next_send_at`, survives restart, preserves
FIFO, and has a bounded five-deferral budget. Missing/invalid `retry_after` or
an exhausted budget becomes operator-visible terminal `failed`; it is never
treated as ambiguous. Blocked `sending|ambiguous`
or terminal `failed` counts and oldest timestamps are exposed in Telegram
status and the Settings warning surface, scoped to the authenticated
tenant/user; bot-global counts remain server-side operational telemetry.
Runtime Bot API 401 proves the Bot credential is rejected and makes the adapter
unready until credentials are repaired and the process is restarted. A 403 is
recipient/chat scoped (for example, the user blocked the Bot): only that
delivery becomes terminal `failed`, while the adapter remains ready.

## Media evolution boundary

Media support is split into a transport-safe foundation and a separately
activated understanding pipeline. The current release implements only the
foundation and continues to return the explicit `telegram:media-help` reply.
It must not claim that an image, voice note, audio file or video was understood.

At webhook admission, supported Telegram message shapes (`photo`, `animation`,
`audio`, `voice`, `video`, `video_note`, `document`, and `sticker`) are reduced
to one versioned provider-neutral envelope. For Telegram animations, the
compatibility `document` duplicate is collapsed. For photos, only the largest
available variant is retained. The envelope freezes:

- provider opaque `file_id` and non-downloadable `file_unique_id`;
- semantic kind, caption, media-group identifier, filename, MIME hint, byte
  size, dimensions and duration when supplied by Telegram;
- no Bot token, `file_path`, provider download URL, local path or media bytes.

The envelope is part of the semantic update digest and exact replay comparison.
A duplicate update with changed media authority conflicts before Agent work.
Group and topic admission still occurs before persistence: an ambient media
message is ignored, while an exact mention/reply stores a caption with the Bot
mention removed. Caption text is user input, never a system instruction.

A future multimodal activation must be a separate A/S-level change and must
implement all of these gates before the first download or paid model call:

1. Recheck active tenant membership, channel identity, exact route/topic and a
   dedicated media-processing policy under a durable claim/fence. Revocation
   after webhook receipt must prevent a new download claim.
2. Select processing from an explicit model capability manifest (`image`,
   `audio`, `video`, document/PDF), not from model-name guesses. The selected
   capability, provider/model generation, preprocessing policy and request
   digest must be frozen for replay and cost attribution.
3. Resolve `file_id` with the then-active encrypted Bot credential. Telegram's
   hosted Bot API currently limits `getFile` downloads to 20 MB and returns a
   temporary path; Vane must apply a lower product quota when appropriate,
   stream with a hard byte limit and deadline, and never persist or log the
   token-bearing URL. MIME and filename metadata are advisory only.
4. Verify magic bytes and decoded shape before model submission. Enforce image
   pixel/frame limits, audio/video duration and sampling budgets, decompression
   limits, supported codecs and tenant cost quota. Generic documents require a
   separate allowlist/scanner/parser boundary; they cannot inherit image trust.
5. Materialize a typed model request: direct multimodal content only when the
   selected model/provider contract supports that modality; otherwise use an
   explicit, separately metered transcription/description stage. Never silently
   drop an attachment or pretend a text-only model inspected it.
6. Store only content-addressed, tenant-scoped temporary objects with bounded
   retention and deletion/audit receipts. Conversation history stores the
   typed reference and derived text/digest, not base64 media. Raw bytes must not
   enter Agent logs, errors, Temporal history or general database JSON.
7. Treat Telegram albums (`media_group_id`) as multiple independently durable
   updates until a bounded album-assembly protocol exists. Do not assume all
   album members arrive together or block unrelated conversation FIFO forever.
8. Make user-visible outcomes honest and durable: processed modality and any
   sampling/transcription fallback are stated; rejected/oversized/unsupported
   media gets an actionable reply; provider/model failures cannot become silent
   loss or an unbounded automatic paid retry.

This boundary allows future image/audio/video models to plug in after the
durable inbox without changing Telegram identity, route, topic, update replay
or outbound-send semantics. It intentionally does not pre-authorize downloads
or broaden the current model input protocol.

## Database role boundary

Migrations 133-139 install exact-user RLS policies and revoke the future
`vane_app` role, but the current primary Store intentionally still runs through
the repository's schema-owner compatibility DSN. PostgreSQL owners bypass
non-FORCE RLS, so this branch does **not** count those policies as current
tenant-isolation evidence. Current authority is the exact membership foreign
key plus the explicit tenant/user/bot/actor/chat predicates and row locks in
every Store method. Moving the primary server to `vane_app` remains blocked
until a later migration grants a narrow channel capability and production-role
tests prove `row_security_active`; it must not be enabled by config alone.

## Secret and transport rules

- Bot token and webhook secret are absent from repository values. Migrations
  137-138 store user-owned encrypted generations. Every authenticated member
  may manage only their exact `(tenant_id,user_id)` scope; AES-GCM AAD binds that
  scope. The manager fleet routes `/telegram/webhook/{verified_bot_id}` and hot
  rotates the exact user's Manager. The legacy environment Manager remains a
  compatibility route only and does not become a platform-owned Bot.
- Bot API redirects are never followed because the token is embedded in the
  request path.
- Transport errors are sanitized and never wrap the token-bearing URL.
- Replies use plain text; no Markdown/HTML parser or escaping ambiguity exists.
- Callback values contain only a bounded action and HMAC. Tenant, user, route,
  Tool arguments, and business-object IDs are never trusted from callback data.
- Route status APIs expose only internal route ID, kind, chat type, and bind
  time; raw actor/chat/thread IDs and group titles are not returned to Web.
- Token, secret, raw update body, actor ID, chat ID and message text are not
  logged or exposed by the status API.

## Rollout and UAT

The legacy adapter is dark unless `VANE_TELEGRAM_ENABLED=true`. Database-backed
user Bots require the credential vault key and are discovered at startup even
when the legacy adapter is disabled. A bad stored generation fails startup;
runtime failure is exposed to the exact user without taking unrelated users
offline. `/readyz` proves the fleet router, not every user's provider account.

Before changing feature 4.9 from `开发中`, production must prove:

1. exact `getMe` bot ID/username and exact HTTPS webhook state;
2. wrong secret, unknown actor, ambient group messages, foreign actors and
   wrong-topic messages cause zero Agent/business work;
3. logged-in owner pairs once, installs a group and forum topic, uses the menu
   and signed callbacks, queries tasks and creates one removable
   test task through the existing Agent lifecycle;
4. exact and concurrent update replay produce one Agent turn and one task effect;
5. restart after inbox ACK recovers with the same turn identity;
6. 4096-boundary Chinese/emoji replies preserve content and order, and topic
   replies retain the exact `message_thread_id`;
7. unlink and bot/secret rotation revoke old authority;
8. group migration preserves the internal route, topic close revokes only that
   topic, and `my_chat_member` removal revokes all routes in that chat;
9. a real 429 honors durable `retry_after` across restart while timeout/5xx
   remains ambiguous and cannot resend;
10. Feishu owner chat and one existing Feishu delivery still pass regression;
11. logs/traces contain no bot token, webhook secret or raw private message.

## Historical decisions used

- `memory/vane-owned-agent-harness.md`: channels are adapters; Vane owns Agent
  state and business facts.
- `memory/vane-agent-architecture-references.md`: the second real channel
  triggers a light channel/capability boundary, not a heavyweight universal IR.
- `wiki/postmortems/2026-05-26--openclaw-context-overflow-silent-reply-loss.md`:
  user-visible delivery cannot fail silently.
- `wiki/invariants/unsafe-by-default.md`: Telegram update admission must be an
  explicit allowlist.
- `wiki/invariants/verified-not-done.md`: webhook/API success is not final
  business delivery; durable terminal settlement is separate.

No Telegram-specific prior decision existed when this contract was written.
