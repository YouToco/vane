package periodicbrief

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/YouToco/vane/feishu"
	"github.com/YouToco/vane/pusheffect"
	"github.com/YouToco/vane/store"
	"github.com/YouToco/vane/types"
)

type DeliverySender interface {
	OwnerOpenID() string
	OwnerChatID() string
	AppIdentity() string
	SendCardWithUUIDResult(
		context.Context, string, string, string, string,
	) (pusheffect.ProviderObservation, error)
}

type DeliveryStore interface {
	GetBriefReportSettingsV1(
		context.Context, int64, int64, string,
	) (store.BriefReportSettingsV1, error)
	LoadGroundedBriefContextV1(
		context.Context, int64, int64, string,
		store.GroundedBriefKindV1, int64,
	) (store.GroundedBriefContextV1, error)
	PreparePeriodicReportDeliveryV1(
		context.Context, types.PeriodicBriefReportV1,
		store.BriefReportDeliveryV1, []byte, string, string, string, string,
		bool,
	) (store.PeriodicReportDeliveryV1, error)
	ClaimPeriodicReportDeliveryV1(
		context.Context, int64, int64, int64,
	) (store.PeriodicReportDeliveryV1, bool, error)
	FinalizePeriodicReportDeliveryV1(
		context.Context, int64, int64, int64,
		store.PeriodicReportDeliveryStatusV1, string,
	) error
}

type DeliverInputV1 struct {
	Report types.PeriodicBriefReportV1 `json:"report"`
}

func periodicReportWebURLV1(
	origin string,
	report types.PeriodicBriefReportV1,
) (string, error) {
	base, err := url.Parse(strings.TrimRight(origin, "/"))
	if err != nil || base == nil ||
		(base.Scheme != "https" && base.Scheme != "http") ||
		base.Host == "" || base.User != nil {
		return "", errors.New("periodic report origin is invalid")
	}
	query := url.Values{}
	query.Set("brief_period", report.Cadence)
	query.Set("report_id", fmt.Sprintf("%d", report.ID))
	base.Path = strings.TrimRight(base.Path, "/") + "/"
	base.RawQuery = ""
	base.Fragment = "/tasks/" + url.PathEscape(report.TaskID) +
		"?" + query.Encode()
	return base.String(), nil
}

func importantPeriodicSignalV1(
	report types.PeriodicBriefReportV1,
	grounding store.GroundedBriefContextV1,
) bool {
	type identity struct {
		briefID, insightID int64
	}
	sourcesByInsight := make(map[identity]map[string]struct{})
	for _, brief := range grounding.Evidence {
		for _, insight := range brief.Insights {
			key := identity{briefID: brief.BriefID, insightID: insight.ID}
			sources := make(map[string]struct{})
			if insight.Structured != nil {
				for _, claim := range insight.Structured.Claims {
					for _, source := range claim.SourceRefs {
						// Source reference IDs are only unique inside one
						// canonical Brief. Preserve that scope when a
						// periodic signal spans multiple Briefs.
						sources[fmt.Sprintf("%d:%s", brief.BriefID, source)] =
							struct{}{}
					}
				}
			}
			sourcesByInsight[key] = sources
		}
	}
	for _, signal := range report.Content.Signals {
		if signal.Kind != types.ExecutiveSignalRisk &&
			signal.Kind != types.ExecutiveSignalOpportunity &&
			signal.Lifecycle != types.ExecutiveSignalIntensified {
			continue
		}
		insights := make(map[identity]struct{})
		sources := make(map[string]struct{})
		for _, ref := range signal.EvidenceRefs {
			key := identity{briefID: ref.BriefID, insightID: ref.InsightID}
			insights[key] = struct{}{}
			for source := range sourcesByInsight[key] {
				sources[source] = struct{}{}
			}
		}
		if len(insights) >= 2 && len(sources) >= 2 {
			return true
		}
	}
	return false
}

func (a *Activities) DeliverPeriodicBriefV1(
	ctx context.Context,
	input DeliverInputV1,
) error {
	return deliverPeriodicBriefV1(
		ctx, input.Report, a.deliveryStore, a.sender,
		a.dashboardOrigin,
		input.Report.TaskID != "" &&
			input.Report.TaskID == a.deliveryTaskID)
}

func deliverPeriodicBriefV1(
	ctx context.Context,
	report types.PeriodicBriefReportV1,
	deliveryStore DeliveryStore,
	sender DeliverySender,
	dashboardOrigin string,
	channelEnabled bool,
) error {
	if report.Validate() != nil || deliveryStore == nil ||
		sender == nil || dashboardOrigin == "" {
		return types.NewAppError(
			types.CodeValidation, "周期报告推送输入无效", nil)
	}
	settings, err := deliveryStore.GetBriefReportSettingsV1(
		ctx, report.TenantID, report.UserID, report.TaskID)
	if err != nil {
		return err
	}
	grounding, err := deliveryStore.LoadGroundedBriefContextV1(
		ctx, report.TenantID, report.UserID, report.TaskID,
		store.GroundedBriefReport, report.ID)
	if err != nil {
		return err
	}
	shouldSend := channelEnabled &&
		settings.Delivery == store.BriefReportDeliveryAlways ||
		channelEnabled &&
			(len(report.Content.Signals) > 0 &&
				settings.Delivery == store.BriefReportDeliveryImportant &&
				(report.Content.DecisionState == types.ExecutiveDecisionAct ||
					importantPeriodicSignalV1(report, grounding)))
	webURL, err := periodicReportWebURLV1(dashboardOrigin, report)
	if err != nil {
		return err
	}
	card := feishu.BuildPeriodicBriefCardV1(report, webURL)
	if card == "" {
		return types.NewAppError(
			types.CodeValidation, "周期报告飞书卡无效", nil)
	}
	providerUUID := uuid.NewSHA1(
		uuid.NameSpaceURL,
		[]byte(fmt.Sprintf(
			"vane.periodic-report-delivery/v1:%d:%s",
			report.ID, report.Digest)),
	).String()
	appIdentity, targetOpenID := sender.AppIdentity(), sender.OwnerOpenID()
	if !shouldSend {
		if appIdentity == "" {
			appIdentity = "web-only"
		}
		if targetOpenID == "" {
			targetOpenID = "web-only"
		}
	}
	prepared, err := deliveryStore.PreparePeriodicReportDeliveryV1(
		ctx, report, settings.Delivery, []byte(card), providerUUID,
		appIdentity, targetOpenID,
		sender.OwnerChatID(), shouldSend)
	if err != nil {
		return err
	}
	if prepared.Status == store.PeriodicReportDeliverySkipped ||
		prepared.Status == store.PeriodicReportDeliverySent ||
		prepared.Status == store.PeriodicReportDeliveryAmbiguous {
		return nil
	}
	claimed, authority, err :=
		deliveryStore.ClaimPeriodicReportDeliveryV1(
			ctx, report.TenantID, report.UserID, report.ID)
	if err != nil {
		return err
	}
	if !authority {
		return types.NewAppError(
			types.CodeConflict, "周期报告推送已被领取", nil)
	}
	observation, sendErr := sender.SendCardWithUUIDResult(
		ctx, claimed.AppIdentity, claimed.TargetOpenID,
		string(claimed.CardPayload), claimed.ProviderUUID)
	switch observation.Disposition {
	case pusheffect.AttemptSent:
		return deliveryStore.FinalizePeriodicReportDeliveryV1(
			ctx, report.TenantID, report.UserID, report.ID,
			store.PeriodicReportDeliverySent, observation.MessageID)
	case pusheffect.AttemptDefiniteNotSent:
		_ = deliveryStore.FinalizePeriodicReportDeliveryV1(
			ctx, report.TenantID, report.UserID, report.ID,
			store.PeriodicReportDeliveryPrepared, "")
	default:
		// Keep the durable row in sending. Only provider-history recovery may
		// resolve an unknown boundary crossing; absence from history is not
		// proof that no send occurred.
	}
	if sendErr != nil {
		return sendErr
	}
	return types.NewAppError(
		types.CodePushFailed, "周期报告推送结果不确定", nil)
}
