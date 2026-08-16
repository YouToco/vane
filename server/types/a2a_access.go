package types

import "time"

// A2AScope is a closed, Vane-owned authorization value. Remote A2A metadata
// never creates scopes and private Skill/MCP execution is intentionally absent.
type A2AScope string

const (
	A2AScopeAssistantChat A2AScope = "assistant.chat"
	A2AScopeContentQuery  A2AScope = "content.query"
)

func (s A2AScope) Valid() bool {
	return s == A2AScopeAssistantChat || s == A2AScopeContentQuery
}

// A2AAccessToken is the safe management projection. TokenHash is retained only
// for Store authentication and never serialized; the raw bearer is returned
// once by the issuing HTTP response and is not stored.
type A2AAccessToken struct {
	ID                  string     `json:"id"`
	TokenHash           []byte     `json:"-"`
	TenantID            int64      `json:"tenant_id"`
	PrincipalUserID     int64      `json:"principal_user_id"`
	ActorType           ActorType  `json:"actor_type"`
	ServiceAccountLabel string     `json:"service_account_label,omitempty"`
	Scopes              []A2AScope `json:"scopes"`
	IssuedBy            int64      `json:"issued_by"`
	ExpiresAt           time.Time  `json:"expires_at"`
	RevokedAt           *time.Time `json:"revoked_at,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	RawTokenOnce        string     `json:"token,omitempty"`
}

type IssueA2AAccessToken struct {
	TenantID            int64
	ActorUserID         int64
	PrincipalUserID     int64
	ActorType           ActorType
	ServiceAccountLabel string
	Scopes              []A2AScope
	TokenHash           []byte
	SessionTokenHash    []byte
	ReauthProofHash     []byte
	ExpiresAt           time.Time
}

// A2AAuthenticatedPrincipal is the authentication result consumed by the
// future A2A middleware cutover. Role is resolved from the live membership,
// never frozen into the bearer token.
type A2AAuthenticatedPrincipal struct {
	TokenID   string
	TenantID  int64
	UserID    int64
	Role      MembershipRole
	ActorType ActorType
	Scopes    []A2AScope
}

// A2AExecutionScope is the immutable authority captured after a bearer has
// been authenticated. Task and content stores accept this value explicitly so
// no caller can infer a workspace from user_id or silently fall back to the
// historical global A2A task namespace.
type A2AExecutionScope struct {
	TokenID   string
	TenantID  int64
	UserID    int64
	Role      MembershipRole
	ActorType ActorType
	Scopes    []A2AScope
}
