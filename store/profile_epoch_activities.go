package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/YouToco/vane/types"
)

const aggregateQuestionActivityKind = "aggregate_question"
const aggregateQuestionWrappedContextRunes = 32768

// LookupAggregateQuestionActivity returns an exact durable aggregate-question
// replay without consulting mutable delivery/message mappings. The
// SECURITY DEFINER reader is intentionally scoped to the authenticated
// vane_app user/app/inbound identity and returns at most one lifetime receipt.
func (s *Store) LookupAggregateQuestionActivity(
	ctx context.Context,
	userID int64,
	appIdentity string,
	inboundKey string,
	requestDigest string,
) (string, bool, error) {
	if userID <= 0 ||
		appIdentity == "" || len(appIdentity) > 255 ||
		inboundKey == "" || len(inboundKey) > 512 ||
		!validProfileEpochActivityDigest(requestDigest) {
		return "", false, types.NewAppError(
			types.CodeValidation,
			"聚合追问活动重放身份无效",
			types.ErrValidation,
		)
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", false, profileClaimDBError(
			"begin aggregate question activity lookup", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var available bool
	if err := tx.QueryRow(ctx, `
		SELECT to_regprocedure(
		  'public.lookup_profile_epoch_activity_receipt_v1(bigint,text,text)'
		) IS NOT NULL`,
	).Scan(&available); err != nil {
		return "", false, profileClaimDBError(
			"check aggregate question activity lookup capability", err)
	}
	if !available {
		return "", false, nil
	}
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.user_id',$1,true),
		       set_config('app.app_identity',$2,true)`,
		strconv.FormatInt(userID, 10), appIdentity,
	); err != nil {
		return "", false, profileClaimDBError(
			"set aggregate question activity lookup scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return "", false, profileClaimDBError(
			"set aggregate question activity lookup role", err)
	}

	var storedDigest, wrappedContext string
	err = tx.QueryRow(ctx, `
		SELECT request_digest,wrapped_context
		  FROM public.lookup_profile_epoch_activity_receipt_v1($1,$2,$3)`,
		userID, appIdentity, inboundKey,
	).Scan(&storedDigest, &wrappedContext)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, profileClaimDBError(
			"lookup aggregate question activity receipt", err)
	}
	if storedDigest != requestDigest {
		return "", false, aggregateQuestionActivityConflict(inboundKey)
	}
	if wrappedContext == "" ||
		utf8.RuneCountInString(wrappedContext) >
			aggregateQuestionWrappedContextRunes {
		return "", false, types.NewAppError(
			types.CodeConflict,
			"聚合追问活动回执正文无效",
			types.ErrConflict,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, profileClaimDBError(
			"commit aggregate question activity lookup", err)
	}
	return wrappedContext, true, nil
}

// RecordAggregateQuestionActivity appends one non-learning restore barrier for
// an inbound reply to an aggregate push message. The inbound message id is a
// lifetime-scoped idempotency key for the tenant/user; exact retries replay,
// while reusing the key with another canonical request digest conflicts.
//
// The database trigger re-proves that sourceMessageID belongs to at least two
// deliveries for this exact subject and assigns the current profile epoch under
// the same fence as feedback writers. No feedback row or cursor is touched.
func (s *Store) RecordAggregateQuestionActivity(
	ctx context.Context,
	userID int64,
	appIdentity string,
	inboundKey string,
	sourceMessageID string,
	requestDigest string,
	expectedDeliveryIDs []int64,
	wrappedContext string,
) (string, error) {
	if userID <= 0 ||
		appIdentity == "" || len(appIdentity) > 255 ||
		inboundKey == "" || len(inboundKey) > 512 ||
		sourceMessageID == "" || len(sourceMessageID) > 512 ||
		!validProfileEpochActivityDigest(requestDigest) ||
		!validAggregateQuestionDeliveryIDs(expectedDeliveryIDs) ||
		wrappedContext == "" ||
		utf8.RuneCountInString(wrappedContext) >
			aggregateQuestionWrappedContextRunes {
		return "", types.NewAppError(
			types.CodeValidation,
			"聚合追问活动范围或幂等凭据无效",
			types.ErrValidation,
		)
	}

	tx, err := s.beginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return "", profileClaimDBError(
			"begin aggregate question activity transaction", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	// Keep the total order shared with feedback and reset:
	// global admission -> tenant -> membership -> epoch fence -> profile/state.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock($1,$2)
		   /* aggregate question activity admission */`,
		agentSessionFactAdmissionClass,
		agentSessionFactAdmissionKey,
	); err != nil {
		return "", profileClaimDBError(
			"lock aggregate question activity admission", err)
	}

	var (
		storedTenantID int64
		storedDigest   string
		storedContext  string
	)
	if _, err := tx.Exec(ctx, `
		SELECT set_config('app.user_id',$1,true),
		       set_config('app.app_identity',$2,true)`,
		strconv.FormatInt(userID, 10), appIdentity,
	); err != nil {
		return "", profileClaimDBError(
			"set aggregate question activity replay scope", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE vane_app`); err != nil {
		return "", profileClaimDBError(
			"set aggregate question activity replay role", err)
	}
	replayErr := tx.QueryRow(ctx, `
		SELECT tenant_id,request_digest,wrapped_context
		  FROM public.lookup_profile_epoch_activity_receipt_v1($1,$2,$3)`,
		userID, appIdentity, inboundKey,
	).Scan(&storedTenantID, &storedDigest, &storedContext)
	if _, err := tx.Exec(ctx, `RESET ROLE`); err != nil {
		return "", profileClaimDBError(
			"reset aggregate question activity replay role", err)
	}
	if replayErr == nil {
		if storedDigest != requestDigest {
			return "", aggregateQuestionActivityConflict(inboundKey)
		}
		// The lifetime receipt is independent of later message/delivery repair.
		// If the supplied source now resolves unambiguously to another tenant,
		// however, this is cross-tenant key reuse and must never replay.
		var mappedTenantID, mappedTenantCount, mappedDeliveryCount int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(min(tenant_id),0),count(DISTINCT tenant_id),count(*)
			  FROM deliveries
			 WHERE user_id=$1 AND feishu_message_id=$2
			   AND feishu_message_id<>''`,
			userID, sourceMessageID,
		).Scan(
			&mappedTenantID, &mappedTenantCount, &mappedDeliveryCount,
		); err != nil {
			return "", profileClaimDBError(
				"revalidate aggregate question activity replay scope", err)
		}
		if mappedTenantCount > 1 ||
			(mappedDeliveryCount > 0 && mappedTenantID != storedTenantID) {
			return "", aggregateQuestionActivityConflict(inboundKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", profileClaimDBError(
				"commit aggregate question activity replay", err)
		}
		return storedContext, nil
	}
	if !errors.Is(replayErr, pgx.ErrNoRows) {
		return "", profileClaimDBError(
			"load aggregate question activity lifetime receipt", replayErr)
	}

	var tenantID, deliveryCount, tenantCount int64
	var deliveryIDs []int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(min(tenant_id),0),count(*),
		        count(DISTINCT tenant_id),
		        COALESCE(array_agg(id ORDER BY id),'{}'::bigint[])
		   FROM deliveries
		  WHERE user_id=$1 AND feishu_message_id=$2
		    AND feishu_message_id<>''`,
		userID, sourceMessageID,
	).Scan(
		&tenantID, &deliveryCount, &tenantCount, &deliveryIDs,
	); err != nil {
		return "", profileClaimDBError(
			"resolve aggregate question activity tenant", err)
	}
	if tenantID <= 0 || tenantCount != 1 {
		return "", types.NewAppError(
			types.CodeConflict,
			"聚合推送投递集合已变化，请重试",
			types.ErrConflict,
		)
	}
	if err := setFeedbackRuntimeContext(ctx, tx, tenantID, userID); err != nil {
		return "", err
	}
	expectedSetDigest := aggregateQuestionDeliverySetDigest(expectedDeliveryIDs)
	if deliveryCount < 2 ||
		aggregateQuestionDeliverySetDigest(deliveryIDs) != expectedSetDigest {
		return "", types.NewAppError(
			types.CodeConflict,
			"聚合推送投递集合已变化，请重试",
			types.ErrConflict,
		)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO profile_epoch_activities (
		     tenant_id,user_id,profile_epoch,activity_kind,app_identity,
		     inbound_key,request_digest,source_message_id,delivery_set_digest,
		     wrapped_context
		 )
		 VALUES ($1,$2,0,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT (user_id,app_identity,inbound_key) DO NOTHING`,
		tenantID, userID, aggregateQuestionActivityKind, appIdentity,
		inboundKey, requestDigest, sourceMessageID, expectedSetDigest,
		wrappedContext,
	); err != nil {
		return "", profileClaimDBError(
			"insert aggregate question activity", err)
	}

	var (
		storedKind, storedApp, storedSource string
		storedDeliverySetDigest             string
	)
	if err := tx.QueryRow(ctx,
		`SELECT activity_kind,app_identity,request_digest,
		        source_message_id,delivery_set_digest,wrapped_context
		   FROM profile_epoch_activities
		  WHERE tenant_id=$1 AND user_id=$2
		    AND app_identity=$3 AND inbound_key=$4`,
		tenantID, userID, appIdentity, inboundKey,
	).Scan(
		&storedKind, &storedApp, &storedDigest,
		&storedSource, &storedDeliverySetDigest, &storedContext,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", types.NewAppError(
				types.CodeConflict,
				"聚合追问活动幂等回执不可见",
				types.ErrConflict,
			)
		}
		return "", profileClaimDBError(
			"load aggregate question activity replay", err)
	}
	if storedKind != aggregateQuestionActivityKind ||
		storedApp != appIdentity ||
		storedDigest != requestDigest {
		return "", aggregateQuestionActivityConflict(inboundKey)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", profileClaimDBError(
			"commit aggregate question activity", err)
	}
	return storedContext, nil
}

func aggregateQuestionActivityConflict(inboundKey string) error {
	return types.NewAppError(
		types.CodeConflict,
		fmt.Sprintf(
			"聚合追问活动幂等键 %q 已被其他请求使用",
			inboundKey,
		),
		types.ErrConflict,
	)
}

func validAggregateQuestionDeliveryIDs(deliveryIDs []int64) bool {
	if len(deliveryIDs) < 2 {
		return false
	}
	for i, deliveryID := range deliveryIDs {
		if deliveryID <= 0 || (i > 0 && deliveryIDs[i-1] >= deliveryID) {
			return false
		}
	}
	return true
}

func aggregateQuestionDeliverySetDigest(deliveryIDs []int64) string {
	var canonical strings.Builder
	canonical.WriteString("vane.aggregate-question-delivery-set/v1|")
	for i, deliveryID := range deliveryIDs {
		if i > 0 {
			canonical.WriteByte(',')
		}
		canonical.WriteString(strconv.FormatInt(deliveryID, 10))
	}
	sum := sha256.Sum256([]byte(canonical.String()))
	return hex.EncodeToString(sum[:])
}

func validProfileEpochActivityDigest(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32
}
