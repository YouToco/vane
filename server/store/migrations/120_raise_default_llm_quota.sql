-- 120: keep the internal LLM token bucket from blocking Agent-first task
-- development and real shadow acceptance. This is a Vane tenant guard, not a
-- Kimi/DeepSeek account balance. Provider billing receipts, per-call limits,
-- idempotency, and immutable spend ledgers remain unchanged.

-- +goose Up

UPDATE tenant_quota
   SET tokens=1000000000.0,
       rate=1000000000.0/86400.0,
       burst=1000000000.0,
       updated_at=now()
 WHERE bucket='llm_tokens'
   AND burst=2000000.0
   AND abs(rate-2000000.0/86400.0)<0.000001;

-- +goose Down

UPDATE tenant_quota
   SET tokens=LEAST(tokens,2000000.0),
       rate=2000000.0/86400.0,
       burst=2000000.0,
       updated_at=now()
 WHERE bucket='llm_tokens'
   AND burst=1000000000.0
   AND abs(rate-1000000000.0/86400.0)<0.000001;
