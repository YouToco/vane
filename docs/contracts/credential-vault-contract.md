# Provider credential vault contract

Status: foundation implemented in migration 137; provider runtime cutover is a
separate, fail-closed rollout.

## Authority model

- Telegram and Feishu bots are tenant-owned delivery channels. An exact active
  `owner` membership may verify, rotate, inspect redacted status, or revoke its
  own tenant credential. A platform administrator does not silently turn one
  tenant bot into a shared platform bot.
- LLM routing is shared platform infrastructure. Only an exact active `owner`
  membership in the platform administration tenant may change it.
- External provider actor, chat, bot, app, or webhook identifiers are never
  Vane principals. Existing channel bindings continue to resolve an exact
  tenant and user before any Agent, Tool, Temporal, or business write.

## Storage and encryption

- Provider secret bytes are JSON encoded and AES-256-GCM encrypted in the
  application before SQL insertion. PostgreSQL stores envelope version, key ID,
  random nonce, authenticated ciphertext, keyed fingerprint, non-secret
  metadata, generation, actor, and lifecycle timestamps only.
- GCM additional authenticated data binds scope kind, tenant ID, provider,
  purpose, credential generation, and key ID. Moving ciphertext to another
  tenant, provider, purpose, or generation must fail authentication.
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

- Each exact `(scope, tenant, provider, purpose)` has monotonically increasing
  generations and at most one active generation.
- Rotation takes an exact-scope advisory transaction lock, re-proves owner
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
- Downgrade of migration 137 refuses while any credential history exists.

## Web and runtime rollout gates

The database/API foundation does not by itself authorize claiming that a
provider is dynamically reconfigured. Each provider cutover must also prove:

1. startup resolves the active database generation before accepting work;
2. a successful Web rotation atomically installs or reloads that exact
   generation, or reports an explicit non-active state;
3. in-flight work keeps its pinned generation and missing retained keys fail
   closed;
4. Telegram webhook routing is bot-specific and tenant-safe; Feishu connection
   ownership is tenant-specific; LLM clients switch as one platform generation;
5. restart, concurrent rotation/revoke, provider verification failure, response
   loss, log redaction, and cross-tenant authorization have real integration
   coverage.

Until a provider passes those gates, existing environment/settings runtime is
the compatibility path and the Web credential endpoint must not describe the
new generation as runtime-active.

The shared LLM is the first partial cutover: the super-administrator Web page
creates an encrypted platform generation, and the next safe process start makes
that generation authoritative for pipeline, Agent, and research clients. The
write response therefore says `restart_required`; it does not claim hot reload.
An unreadable, tampered, or explicitly revoked database generation blocks
startup instead of falling back. Hot generation switching remains a later gate.
