# Provider credential vault contract

Status: encrypted storage and administration are implemented in migrations
145-146. Telegram has a per-user runtime manager fleet. Shared LLM and
information-provider credentials are selected from the database at safe process
start; hot reload and the per-user Feishu runtime fleet remain separate
fail-closed rollouts.

## Authority model

- Telegram and Feishu bots are user-owned delivery channels. Any exact active
  membership may verify, rotate, inspect redacted status, or revoke only its
  own `(tenant_id,user_id)` credential. A tenant owner or platform administrator
  cannot read or replace another user's Bot credential.
- LLM routing is shared platform infrastructure. Only an exact active `owner`
  membership in the platform administration tenant may change it.
- Exa and TikHub acquisition keys are shared platform infrastructure and rotate
  atomically as one `fetch/shared_runtime` generation. This prevents a process
  restart from combining half of a new provider configuration with half of an
  old one.
- External provider actor, chat, bot, app, or webhook identifiers are never
  Vane principals. Existing channel bindings continue to resolve an exact
  tenant and user before any Agent, Tool, Temporal, or business write.

## Storage and encryption

- Provider secret bytes are JSON encoded and AES-256-GCM encrypted in the
  application before SQL insertion. PostgreSQL stores envelope version, key ID,
  random nonce, authenticated ciphertext, keyed fingerprint, non-secret
  metadata, generation, actor, and lifecycle timestamps only.
- GCM additional authenticated data binds scope kind, tenant ID, user ID for
  user credentials, provider, purpose, generation, and key ID. Moving
  ciphertext to another user, tenant, provider, purpose, or generation fails.
- The deployment key-encryption key (KEK) is never stored in PostgreSQL and is
  never accepted or returned by a Web API. Production supplies it through a
  systemd credential or sensitive environment value. Database backup theft
  alone therefore does not disclose provider tokens.
- Fingerprints use HMAC-SHA-256 rather than a raw token hash. GET endpoints
  expose only redacted metadata and the keyed fingerprint; they never decrypt.
- Secret plaintext must not enter logs, errors, metrics, Temporal histories,
  task snapshots, delivery rows, or API responses. Runtime resolution occurs
  only inside a narrow callback and clears its temporary byte slice afterward.

## Versioning and rotation

- Each exact `(scope, tenant, user, provider, purpose)` has monotonically
  increasing generations and at most one active generation. A verified Bot/App
  identity may be active for only one user across the whole database.
- Rotation takes an exact-scope advisory transaction lock, re-proves user/owner
  authority, retires the prior active generation, encrypts with the active KEK,
  and inserts the new generation in one transaction.
- Retired provider credential generations remain decryptable for pinned
  workflow/recovery references. Revoked generations are not runtime-readable.
- A scope with no credential history may use its documented environment
  compatibility route. Once any generation exists, an explicit revoke is a
  durable tombstone: startup must fail closed and must not resurrect an older
  environment token.
- KEK rotation is independent: new writes use the active key ID while old key
  IDs remain decrypt-only until every retained provider generation has expired.
- Downgrade of migration 145 refuses while any credential history exists.

## Web and runtime rollout gates

The database/API foundation does not by itself authorize claiming that a
provider is dynamically reconfigured. Each provider cutover must also prove:

1. startup resolves the active database generation before accepting work;
2. a successful Web rotation atomically installs or reloads that exact
   generation, or reports an explicit non-active state;
3. in-flight work keeps its pinned generation and missing retained keys fail
   closed;
4. Telegram webhook routing is bot-specific and user-safe; Feishu connection
   ownership is user-specific; LLM clients switch as one platform generation;
5. restart, concurrent rotation/revoke, provider verification failure, response
   loss, log redaction, and cross-tenant authorization have real integration
   coverage.

Until a provider passes those gates, existing environment/settings runtime is
the compatibility path and the Web credential endpoint must not describe the
new generation as runtime-active.

Telegram is the first hot channel cutover: every user can save a Bot token in
Web, the server verifies `getMe`, creates a random webhook secret, and starts or
rotates only that user's Manager at `/telegram/webhook/{verified_bot_id}`. The
same Bot ID cannot be active for two users. The legacy environment Manager is
only a compatibility route for users without a database-backed Bot.

The shared LLM cutover lets only the platform super-administrator create or
rotate an encrypted platform generation in Web. The next safe process start
makes it authoritative; the write response therefore says
`restart_required` and does not claim hot reload. Once platform credential
history exists, an unreadable, tampered, or explicitly revoked generation
blocks startup instead of falling back to a VPS model key.

The compiled pipeline and Research V3 currently require the official DeepSeek
endpoint. Agent chat can inherit that DeepSeek route or use a separately
encrypted Agent key for the official DeepSeek or Kimi/Moonshot endpoint. The
provider name, base URL and model are non-secret metadata; both primary and
dedicated Agent API keys remain encrypted secret material. Selecting Kimi for
Agent does not imply that compiled research or scheduled report generation has
switched to Kimi.

The shared information-provider cutover follows the same safe-start boundary.
Only the platform super-administrator can create, rotate, inspect redacted
status, or revoke the encrypted Exa/TikHub bundle. A process with no database
history may use its deployment compatibility keys. Once history exists, the
active database generation supplies both keys and both compiled credential
generation numbers; an explicit revoke, incomplete envelope, or decryption
failure blocks startup instead of falling back to VPS environment values.

Media capability is also fail-closed. No provider/model is treated as image,
audio or video capable from its name alone. Until a versioned native-modality
manifest and typed request path are implemented, Telegram media remains a
durable inert reference with an explicit unsupported reply; Vane does not add
OCR, transcription, frame extraction or other cross-modal conversion as a
fallback.
