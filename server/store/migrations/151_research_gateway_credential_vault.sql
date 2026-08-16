-- 151: let the isolated Research LLM gateway read only encrypted, retained
-- platform LLM credential envelopes.
--
-- The gateway receives neither table SELECT nor provider plaintext. PostgreSQL
-- returns authenticated ciphertext for the exact platform/llm/shared_runtime
-- authority; the deployment KEK remains outside PostgreSQL and is supplied to
-- the gateway by systemd LoadCredential.

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION list_research_gateway_llm_credentials_v1()
RETURNS TABLE(
    generation       BIGINT,
    envelope_version TEXT,
    key_id            TEXT,
    nonce             BYTEA,
    ciphertext        BYTEA,
    fingerprint       TEXT,
    metadata          JSONB,
    status            TEXT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog,public,pg_temp
AS $$
    SELECT c.generation,c.envelope_version,c.key_id,c.nonce,c.ciphertext,
           c.fingerprint,c.metadata,c.status
      FROM credential_vault_entries c
     WHERE c.scope_kind='platform' AND c.tenant_id IS NULL AND c.user_id IS NULL
       AND c.provider='llm' AND c.purpose='shared_runtime'
       AND c.status IN ('active','retired')
     ORDER BY c.generation DESC
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION list_research_gateway_llm_credentials_v1()
    FROM PUBLIC,vane_app,vane_research_v3_executor;
GRANT EXECUTE ON FUNCTION list_research_gateway_llm_credentials_v1()
    TO vane_research_llm_gateway;

-- +goose Down

REVOKE ALL ON FUNCTION list_research_gateway_llm_credentials_v1()
    FROM vane_research_llm_gateway;
DROP FUNCTION list_research_gateway_llm_credentials_v1();
