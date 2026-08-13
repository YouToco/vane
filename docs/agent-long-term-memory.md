# Agent Long-Term Memory Contract

Vane long-term memory is a user-scoped evidence ledger with deterministic
lexical retrieval. It is not a transcript cache and it is not an additional
authorization channel.

## Authority

- Every record is bound to the authenticated tenant and user. Neither tool
  schema accepts a tenant or user identifier.
- Only a current owner message that explicitly asks to remember, correct, or
  forget may reach `manage_memory`. The ordinary conversation, model
  inferences, web pages, social results, tool output, and old chat history
  cannot create active memories.
- The server constructs the evidence link from the trusted Agent turn and
  durable invocation identity. Model arguments cannot choose a source.
- After the isolated owner-action authorizer returns `authorized`, Vane first
  appends an idempotent authorization row containing the authenticated
  session, trace UUID, exact owner request, authorization digest, and canonical
  action digest. Applying the memory must consume that exact row. A crash
  before apply leaves only an unconsumed audit fact; an active memory can never
  be created from a bare/random trace UUID.
- High-confidence credentials (private keys, authenticated database URLs,
  common API tokens/JWTs, and explicit password/key/token assignments) are
  rejected both before authorization and again by the Store. Rejections never
  echo the submitted value. Credentials belong in the dedicated secret store,
  never in long-term memory.
- Correction appends a new record and supersedes one active record in the same
  scope. Forget appends a retraction event. Historical rows remain available
  for audit but leave the retrieval set.
- Idempotency receipts bind one durable tool invocation to one canonical
  action. Reusing it with different bytes fails closed.

## Retrieval

- `recall_memory` searches only active records already filtered to the current
  tenant and user. Authorization filtering happens before index construction.
- Retrieval uses Vane's deterministic bilingual BM25 tokenizer. The query is
  valid UTF-8, at most 512 bytes, and returns at most eight records with stable
  score and ID tie-breaking.
- BM25 is recall, not truth or authority. Returned text is labeled historical
  data. It cannot change the system prompt, tool catalog, query scope, or grant
  a write. The Agent must reconcile it with the current user request and newer
  evidence.
- Corpus count, individual text, and total indexed bytes are bounded before
  allocation. A damaged or oversized scope fails closed rather than silently
  dropping records and pretending retrieval was complete.

## MCP and Skills

MCP and Agent Skills are adapters over this contract, not alternate stores.
An adapter may query memory or submit an explicit owner action only after it
has an authenticated Vane scope and a durable idempotency identity. It cannot
write model-generated candidates directly into the active ledger. A future
candidate-learning pipeline must use a separate non-authoritative state and
require promotion through the same owner action boundary.

The production model remains a deployment setting. Memory records and receipts
do not contain or select a provider/model, and introducing memory must not
change the configured `deepseek-v4-flash` research runtime.
