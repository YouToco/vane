-- 113: retire the last expired V1 creation admission without deleting history.
--
-- New V1/V2 admission is closed in the production composition root by
-- LegacyAdmissionFencedStore. The retained tables, decoders and mutations are
-- intentionally left in place for exact Temporal replay and recovery. This
-- migration terminally resolves only already-expired pristine V1 operations
-- and records an audit-only suppressed receipt, so no stale user delivery is
-- created during deployment.

-- +goose Up

LOCK TABLE task_creation_operations IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE task_creation_receipts IN SHARE ROW EXCLUSIVE MODE;

WITH expired AS (
    UPDATE task_creation_operations
       SET status='expired', phase='expired',
           lease_owner='', lease_until=NULL, takeover_not_before=NULL,
           result=NULL, error_code='', error_message='', executed_at=NULL,
           tombstoned_at=clock_timestamp(), updated_at=clock_timestamp()
     WHERE execution_version=1 AND tool_name='create_schedule'
       AND status='pending' AND phase='' AND tombstoned_at IS NULL
       AND lease_owner='' AND lease_until IS NULL
       AND takeover_not_before IS NULL
       AND fence=0 AND attempt=0
       AND normalized_command IS NULL AND compiled_definition IS NULL
       AND compiled_digest='' AND prepared_schedule IS NULL
       AND ensure_receipt IS NULL AND task_id=''
       AND result IS NULL AND executed_at IS NULL
       AND error_code='' AND error_message=''
       AND ((receipt_provider='' AND receipt_target='') OR
            (receipt_provider='agent_auto/v1' AND receipt_target=id))
       AND NOT EXISTS (
           SELECT 1 FROM task_creation_receipts existing_receipt
            WHERE existing_receipt.operation_id=task_creation_operations.id
       )
       AND expires_at<=clock_timestamp()
    RETURNING id,tenant_id,user_id,session_id,receipt_provider,receipt_target
)
INSERT INTO task_creation_receipts (
    operation_id,tenant_id,user_id,session_id,provider,target,provider_key,
    status,next_attempt_at,provider_message_id,failure_class,sent_at
)
SELECT id,tenant_id,user_id,session_id,receipt_provider,receipt_target,
       md5('vane/task-creation-receipt/v1:'||id)::uuid,
       'suppressed',clock_timestamp(),'legacy-suppressed',
       'legacy_admission_fence_expired',clock_timestamp()
  FROM expired
ON CONFLICT (operation_id) DO NOTHING;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM task_creation_operations operation
         WHERE operation.execution_version=1
           AND operation.tool_name='create_schedule'
           AND operation.status='pending'
           AND operation.phase=''
           AND operation.lease_owner='' AND operation.lease_until IS NULL
           AND operation.takeover_not_before IS NULL
           AND operation.fence=0 AND operation.attempt=0
           AND operation.normalized_command IS NULL
           AND operation.compiled_definition IS NULL
           AND operation.compiled_digest=''
           AND operation.prepared_schedule IS NULL
           AND operation.ensure_receipt IS NULL AND operation.task_id=''
           AND operation.result IS NULL AND operation.executed_at IS NULL
           AND operation.error_code='' AND operation.error_message=''
           AND ((operation.receipt_provider='' AND operation.receipt_target='') OR
                (operation.receipt_provider='agent_auto/v1' AND
                 operation.receipt_target=operation.id))
           AND NOT EXISTS (
               SELECT 1 FROM task_creation_receipts existing_receipt
                WHERE existing_receipt.operation_id=operation.id
           )
           AND operation.expires_at<=clock_timestamp()
           AND operation.tombstoned_at IS NULL
    ) THEN
        RAISE EXCEPTION '113: expired V1 creation admission remains live';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM task_creation_operations operation
          LEFT JOIN task_creation_receipts receipt
            ON receipt.operation_id=operation.id
         WHERE operation.execution_version=1
           AND operation.tool_name='create_schedule'
           AND operation.status='expired'
           AND operation.phase='expired'
           AND operation.tombstoned_at IS NOT NULL
           AND operation.updated_at>=transaction_timestamp()
           AND (receipt.operation_id IS NULL OR receipt.status<>'suppressed')
    ) THEN
        RAISE EXCEPTION '113: expired V1 creation tombstone lacks audit receipt';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down

-- A terminal operation and its receipt are historical business records. A
-- downgrade may remove no schema because Up added none, but it must never
-- resurrect or delete those records. The application-level admission fence is
-- versioned with the binary and remains the authority after rollback.
SELECT 1;
