-- 133: bind every interactive model call to its exact non-secret policy.
--
-- The manifest is canonical BYTEA instead of JSONB so its SHA-256 remains a
-- byte identity across PostgreSQL versions. It contains only policy/module/
-- model/tool digests; prompt bodies, user content and credentials stay out.

-- +goose Up

SELECT pg_advisory_xact_lock(6215335020355474287);
LOCK TABLE llm_calls IN ACCESS EXCLUSIVE MODE;

ALTER TABLE llm_calls
    ADD COLUMN policy_manifest_payload BYTEA,
    ADD COLUMN policy_manifest_digest TEXT,
    ADD CONSTRAINT llm_calls_policy_manifest_exact_v133 CHECK (
        (policy_manifest_payload IS NULL) = (policy_manifest_digest IS NULL)
        AND (
            policy_manifest_payload IS NULL
            OR (
                octet_length(policy_manifest_payload) BETWEEN 1 AND 16384
                AND policy_manifest_digest ~ '^[0-9a-f]{64}$'
                AND policy_manifest_digest =
                    encode(sha256(policy_manifest_payload), 'hex')
            )
        )
    );

-- +goose Down

SELECT pg_advisory_xact_lock(6215335020355474287);
LOCK TABLE llm_calls IN ACCESS EXCLUSIVE MODE;

ALTER TABLE llm_calls
    DROP CONSTRAINT llm_calls_policy_manifest_exact_v133,
    DROP COLUMN policy_manifest_digest,
    DROP COLUMN policy_manifest_payload;
