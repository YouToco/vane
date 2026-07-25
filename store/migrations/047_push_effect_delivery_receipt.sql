-- 047: atomically close the provider effect and its delivery receipts.
--
-- Numbers 040-046 belong to the agentic ledger/workflow/feedback train already
-- present on main. Push delivery receipts therefore begin at 047.

-- +goose Up

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
