-- 047: atomically close the provider effect and its delivery receipts.
--
-- Numbers 040-046 belong to the agentic ledger/workflow/feedback train already
-- present on main. Push delivery receipts therefore begin at 047.

-- +goose Up

-- 047 is the live-protocol admission boundary. Migrations 039-046 install the
-- dark store, but a runtime identity may enter its restricted roles only while
-- the atomic delivery-receipt contract is present.
GRANT vane_push_effect_coordinator, vane_push_effect_receipt,
    vane_push_effect_operator TO CURRENT_USER;

GRANT SELECT (
    batch_id, delivery_ids, card_payload
) ON push_effects TO vane_push_effect_receipt;

GRANT SELECT (
    id, tenant_id, user_id, batch_id, status, feishu_message_id,
    card_json, sent_at
) ON deliveries TO vane_push_effect_receipt;
GRANT UPDATE (
    status, feishu_message_id, card_json, sent_at
) ON deliveries TO vane_push_effect_receipt;

-- +goose Down

-- Stable ASCII "VANEPUSH". Every post-047 effect writer takes the matching
-- shared transaction lock before any role, tenant, or table lock. Admission is
-- therefore drained before this migration starts its table lock graph.
SELECT pg_advisory_xact_lock(6215335020355474248);

LOCK TABLE push_effects, deliveries IN ACCESS EXCLUSIVE MODE
    /* migration 047 downgrade fence */;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM push_effects) THEN
        RAISE EXCEPTION
            '047: refusing downgrade while durable push effects exist';
    END IF;
END $$;
-- +goose StatementEnd

REVOKE UPDATE (
    status, feishu_message_id, card_json, sent_at
) ON deliveries FROM vane_push_effect_receipt;
REVOKE SELECT (
    id, tenant_id, user_id, batch_id, status, feishu_message_id,
    card_json, sent_at
) ON deliveries FROM vane_push_effect_receipt;
REVOKE SELECT (
    batch_id, delivery_ids, card_payload
) ON push_effects FROM vane_push_effect_receipt;

-- Revoke runtime admission last: a failed non-empty downgrade above preserves
-- every role membership and capability. A successful empty downgrade makes a
-- writer that was waiting on the schema fence fail at SET ROLE after wakeup.
REVOKE vane_push_effect_coordinator, vane_push_effect_receipt,
    vane_push_effect_operator FROM CURRENT_USER;
