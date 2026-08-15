package feedback

import (
	"github.com/YouToco/vane/server/auth"
	"github.com/YouToco/vane/server/types"
)

func testPrincipal(userID int64) auth.Principal {
	return auth.Principal{
		TenantID: 1, UserID: userID,
		Role: types.MembershipRoleOwner, ActorType: types.ActorTypeUser,
	}
}
