package types

type PushBatchDeliveryAuthority string

const (
	PushBatchDeliveryAuthorityLegacy PushBatchDeliveryAuthority = "legacy"
	PushBatchDeliveryAuthorityEffect PushBatchDeliveryAuthority = "effect"
)

func (a PushBatchDeliveryAuthority) Valid() bool {
	return a == PushBatchDeliveryAuthorityLegacy ||
		a == PushBatchDeliveryAuthorityEffect
}

type PushBatchScope struct {
	TenantID int64
	UserID   int64
	BatchID  int64
}
