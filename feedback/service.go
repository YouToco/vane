package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/promptguard"
	"github.com/YouToco/vane/types"
)

// Store 是 feedback 所需 store 方法的窄接口（契约 §10.4），与 *store.Store
// 签名逐字一致。收窄的目的：单测用内存假实现即可，不依赖数据库。
type Store interface {
	GetDeliveryForUser(ctx context.Context, id, userID int64) (*types.Delivery, error)
	GetDeliveryByFeishuMessageID(ctx context.Context, userID int64, msgID string) (*types.Delivery, error)
	// ListDeliveriesByFeishuMessage 同一条飞书消息承载的全部投递（聚合卡重建用），
	// 按分数降序；历史单条卡查回 1 行，重建路径据 len 分流。
	ListDeliveriesByFeishuMessage(ctx context.Context, userID int64, msgID string) ([]types.Delivery, error)
	InsertFeedback(ctx context.Context, f *types.Feedback) (int64, error)
	InsertDeepDiveFeedback(ctx context.Context, f *types.Feedback) (id int64, existingDetail string, existed bool, err error)
	LatestFeedbackAction(ctx context.Context, deliveryID int64, actions []types.FeedbackAction) (types.FeedbackAction, error)
	HasFeedback(ctx context.Context, deliveryID int64, action types.FeedbackAction) (bool, error)
	GetFeedbackDetail(ctx context.Context, deliveryID int64, action types.FeedbackAction) (string, error)
	GetContentItem(ctx context.Context, id int64) (*types.ContentItem, error)
	GetSource(ctx context.Context, id int64) (*types.Source, error)
	GetProfile(ctx context.Context, userID int64) (*types.Profile, error)
}

// Sender 把 markdown 回复到指定消息下（生产实现 *feishu.Manager.ReplyMarkdown）。
type Sender interface {
	ReplyMarkdown(ctx context.Context, parentMessageID, markdown string) error
}

// SessionNotifier 把外部事件通告进当前 agent 会话（生产实现 *agent.Loop.NotifyEvent）。
// 「[卡片回调]」前缀的完整通告文案由本包组装（契约 §12.4）；无 active 会话时
// 丢弃、写入纪律（锁序/预算）归 agent 侧，本包只管调用。
type SessionNotifier interface {
	NotifyEvent(ctx context.Context, userID int64, notice string)
}

// CardBuilder 构造带按钮与状态行的推送卡 JSON（生产实现 feishu.BuildDeliveryCard）。
// 函数注入而非 import feishu：feishu 反向引用本包（Click/ClickResult/CardState），
// import 环由此封死（契约 §10.4 依赖边界）。
// 卡片改版后签名从 (bodyMD, deliveryID, state) 扩展为 CardInput 单参。
type CardBuilder func(input CardInput) string

// Click 一次反馈按钮点击（feishu 回调解析后的规范化形态）。
type Click struct {
	Action     types.FeedbackAction
	DeliveryID int64
}

// ClickResult 点击处理结果：Toast 恒为可直接展示的人话，ToastOK 区分
// toast 的成功/失败样式；CardJSON 非空时 feishu 侧原地把卡片更新为该 JSON。
type ClickResult struct {
	Toast    string
	ToastOK  bool
	CardJSON string
}

// Deps 注入（main.go 装配，契约 §10.4）。
type Deps struct {
	Store     Store
	Client    *llm.Client
	Recorder  *llm.Recorder
	Sender    Sender
	Notifier  SessionNotifier
	BuildCard CardBuilder
	// BuildAggCard 聚合卡构造（生产装配传 feishu.BuildAggregateCard）。
	// nil 时聚合重建退化为"不更新卡片"（toast 仍达）——灰度/测试装配安全。
	BuildAggCard func(in AggregateCardInput) string
	// DeepDiveModel deep_dive 生成用模型（cfg.LLM.AgentModel——Boss 拍板③：
	// 深度解读值得 v4-pro，与打分/出卡的默认档分开）。
	DeepDiveModel string
}

// Service 处理推送卡反馈按钮点击与追问上下文包装。
type Service struct {
	deps Deps
	// inflight 是 deep_dive 三层幂等的第二层（同进程并发去重）：
	// key=delivery_id。进程重启丢失即半途生成作废，由第一层（feedbacks 行 +
	// detail 重发）与第三层（部分唯一索引）兜底，重点一次可恢复。
	inflight sync.Map
}

// New 构造 Service。
func New(deps Deps) *Service {
	return &Service{deps: deps}
}

// attitudeActions 是态度语义的动作集合。查询最新态度**恒传这个双值集合**——
// 传单值会命中旧行、复刻被否决的 (delivery_id,action) 唯一索引 bug（审查 F5：
// interested→not_interested→interested 的第三次点击必须被判定为"态度已变"并插行）。
var attitudeActions = []types.FeedbackAction{
	types.FeedbackActionInterested,
	types.FeedbackActionNotInterested,
}

// HandleClick 处理一次反馈按钮点击（契约 §10.4）。返回 error 仅表示未预期的
// 内部失败（DB 故障等），由 feishu 侧翻译成兜底 toast；业务上的"不能做"
// （越权/已记录过/原文已清理）都编码在 ClickResult 里。
func (s *Service) HandleClick(ctx context.Context, userID int64, click Click) (ClickResult, error) {
	// 归属校验进 WHERE：按钮 value 可伪造，越权与不存在统一同一响应、
	// 零副作用（M4 §10 红线对齐）。
	d, err := s.deps.Store.GetDeliveryForUser(ctx, click.DeliveryID, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return ClickResult{Toast: "找不到这条推送或不属于你"}, nil
		}
		return ClickResult{}, err
	}

	switch click.Action {
	case types.FeedbackActionInterested, types.FeedbackActionNotInterested:
		return s.handleAttitude(ctx, userID, d, click.Action)
	case types.FeedbackActionMisjudged:
		return s.handleMisjudged(ctx, userID, d)
	case types.FeedbackActionDeepDive:
		return s.handleDeepDive(ctx, userID, d)
	default:
		// feishu 侧白名单已挡未知值，这里是纵深兜底。
		return ClickResult{Toast: "未知操作"}, nil
	}
}

// handleAttitude 处理感兴趣/不感兴趣：追加式事件日志、最新为准。
// 重复点同一态度=幂等不插行；点相反态度=追加新行（态度可改）。
func (s *Service) handleAttitude(ctx context.Context, userID int64, d *types.Delivery, action types.FeedbackAction) (ClickResult, error) {
	latest, err := s.deps.Store.LatestFeedbackAction(ctx, d.ID, attitudeActions)
	if err != nil && !errors.Is(err, types.ErrNotFound) {
		return ClickResult{}, err
	}
	if err == nil && latest == action {
		// 幂等：不插行但仍重建卡——并发窗口下状态行的短暂缺项靠重复点击自愈。
		return s.rebuilt(ctx, d, "已记录过", true, nil), nil
	}
	if _, err := s.deps.Store.InsertFeedback(ctx, &types.Feedback{
		UserID: userID, DeliveryID: d.ID, Action: action,
	}); err != nil {
		return ClickResult{}, err
	}
	s.notifyClick(ctx, userID, d.ID, actionLabel(action), "")
	return s.rebuilt(ctx, d, "已记录："+actionLabel(action), true, nil), nil
}

// handleMisjudged 处理误判：一次性信号（MVP 不可撤销），独立于态度、可并存。
func (s *Service) handleMisjudged(ctx context.Context, userID int64, d *types.Delivery) (ClickResult, error) {
	has, err := s.deps.Store.HasFeedback(ctx, d.ID, types.FeedbackActionMisjudged)
	if err != nil {
		return ClickResult{}, err
	}
	if has {
		return s.rebuilt(ctx, d, "已标记过误判", true, nil), nil
	}
	if _, err := s.deps.Store.InsertFeedback(ctx, &types.Feedback{
		UserID: userID, DeliveryID: d.ID, Action: types.FeedbackActionMisjudged,
	}); err != nil {
		return ClickResult{}, err
	}
	s.notifyClick(ctx, userID, d.ID, "误判", "")
	return s.rebuilt(ctx, d, "已标记误判，将用于修正推送判断", true, nil), nil
}

// ReasonSubmit 表示 form 提交的反馈原因（点 👎 后出现的输入框）。
type ReasonSubmit struct {
	DeliveryID int64
	Reason     string // 用户填写的文字，可为空（"可跳过"）
}

// HandleReasonSubmit 处理 👎 后的 form 提交：记录 misjudged + detail。
// 用户已点过 👎（not_interested 已落库），form 是可选的补充——空 reason 也算
// 有效提交（语义：确认误判，但不想说原因）。
func (s *Service) HandleReasonSubmit(ctx context.Context, userID int64, submit ReasonSubmit) (ClickResult, error) {
	d, err := s.deps.Store.GetDeliveryForUser(ctx, submit.DeliveryID, userID)
	if err != nil {
		if errors.Is(err, types.ErrNotFound) {
			return ClickResult{Toast: "找不到这条推送或不属于你"}, nil
		}
		return ClickResult{}, err
	}
	has, err := s.deps.Store.HasFeedback(ctx, d.ID, types.FeedbackActionMisjudged)
	if err != nil {
		return ClickResult{}, err
	}
	if has {
		return s.rebuilt(ctx, d, "已标记过误判", true, nil), nil
	}
	reason := promptguard.TruncateRunes(strings.TrimSpace(submit.Reason), 500)
	if _, err := s.deps.Store.InsertFeedback(ctx, &types.Feedback{
		UserID: userID, DeliveryID: d.ID, Action: types.FeedbackActionMisjudged, Detail: reason,
	}); err != nil {
		return ClickResult{}, err
	}
	suffix := ""
	if reason != "" {
		suffix = "（附原因）"
	}
	s.notifyClick(ctx, userID, d.ID, "误判"+suffix, "")
	return s.rebuilt(ctx, d, "已标记误判，将用于修正推送判断", true, nil), nil
}

// rebuilt 组装"toast + 重建卡"的返回：每次点击都按库内状态重建整卡
// （契约 §10.2 最终一致）。force 非 nil 时在查得的状态上强制覆盖——
// deep_dive 启动路径此刻行尚未插入，DeepDiveRequested 必须人为置真。
// 状态重查失败只降级为不更新卡片：主操作已成功，不能把它报成失败。
func (s *Service) rebuilt(ctx context.Context, d *types.Delivery, toast string, ok bool, force func(*CardState)) ClickResult {
	res := ClickResult{Toast: toast, ToastOK: ok}

	// 聚合卡分流（附录 A）：同一 feishu_message_id 承载多个 delivery 时走聚合重建。
	// 历史单条卡查回恰好 1 行（每卡独立 message_id），自然落回单条路径——旧卡外观零变化。
	//
	// 已知取舍（对抗审查 #5/#6，单 owner 容忍）：推送后 MarkDeliverySent 逐条回填
	// msgID 的毫秒级窗口内点击，兄弟查询可能少行 → 重建出的卡短暂缺条目；回填完成后
	// 下一次点击按库内全量自愈。根治需批量回填或事务，不值当前复杂度。
	if d.FeishuMessageID != "" {
		siblings, serr := s.deps.Store.ListDeliveriesByFeishuMessage(ctx, d.UserID, d.FeishuMessageID)
		if serr != nil {
			slog.Warn("feedback: 查兄弟投递失败，本次不更新卡片", "delivery_id", d.ID, "err", serr)
			return res
		}
		if len(siblings) > 1 {
			// toast 带被点条标题回显（附录 A.4 吸收项）：聚合卡 N 条并排，toast 是
			// 用户确认"系统记到了我点的那条"的最后一道人眼防线。
			if ci, cerr := s.contentOf(ctx, d); cerr == nil && ci != nil && ci.Title != "" {
				res.Toast = toast + "《" + promptguard.TruncateRunes(ci.Title, 20) + "》"
			}
			if cardJSON, aerr := s.rebuildAggregate(ctx, d, siblings, force); aerr != nil {
				slog.Warn("feedback: 聚合卡重建失败，本次不更新卡片", "delivery_id", d.ID, "err", aerr)
			} else {
				res.CardJSON = cardJSON
			}
			return res
		}
	}

	st, err := s.cardState(ctx, d.ID)
	if err != nil {
		slog.Warn("feedback: 重查卡片状态失败，本次不更新卡片", "delivery_id", d.ID, "err", err)
		return res
	}
	if force != nil {
		force(&st)
	}
	input := CardInput{
		BodyMD:     d.BodyMD,
		DeliveryID: d.ID,
		State:      st,
		Score:      int(d.Score),
	}
	if d.ContentItemID != nil {
		if ci, err := s.deps.Store.GetContentItem(ctx, *d.ContentItemID); err == nil {
			input.Title = ci.Title
			input.URL = ci.URL
			input.PublishedAt = ci.PublishedAt
			if src, err := s.deps.Store.GetSource(ctx, ci.SourceID); err == nil {
				input.SourceTitle = src.Title
				input.Platform = src.Platform
			}
		}
	}
	res.CardJSON = s.deps.BuildCard(input)
	return res
}

// cardState 重查状态行三要素（状态行以库内查询为准，契约 §10.2）。
func (s *Service) cardState(ctx context.Context, deliveryID int64) (CardState, error) {
	var st CardState
	latest, err := s.deps.Store.LatestFeedbackAction(ctx, deliveryID, attitudeActions)
	if err == nil {
		st.Preference = latest
	} else if !errors.Is(err, types.ErrNotFound) {
		return CardState{}, err
	}
	mis, err := s.deps.Store.HasFeedback(ctx, deliveryID, types.FeedbackActionMisjudged)
	if err != nil {
		return CardState{}, err
	}
	st.Misjudged = mis
	dd, err := s.deps.Store.HasFeedback(ctx, deliveryID, types.FeedbackActionDeepDive)
	if err != nil {
		return CardState{}, err
	}
	st.DeepDiveRequested = dd
	return st, nil
}

// notifyClick 把按钮点击以「[卡片回调]」通告写入 agent 会话（契约 §12.4 文案；
// 前缀与 agent systemPrompt 的约定对应）。suffix 只有 deep_dive 用
// （"，长文结果将以新消息送达"，完成后不二次通告）；追问不走这里。
//
// 通告只含数据库主键与代码生成的动作标签，刻意不带内容标题/正文。标题来自
// RSS/网页，是外部不可信数据；即使消毒后也不该被装进 systemPrompt 定义为
// “真实用户操作”的高信任通道。用户仍可从原卡片看到标题。
func (s *Service) notifyClick(ctx context.Context, userID, deliveryID int64, label, suffix string) {
	if s.deps.Notifier == nil {
		return
	}
	s.deps.Notifier.NotifyEvent(ctx, userID,
		fmt.Sprintf("[卡片回调] 用户在推送卡片（delivery_id=%d）上点击了「%s」%s", deliveryID, label, suffix))
}

// actionLabel 反馈动作的中文标签（toast 与会话通告共用，与卡片按钮文案一致）。
func actionLabel(a types.FeedbackAction) string {
	switch a {
	case types.FeedbackActionInterested:
		return "感兴趣"
	case types.FeedbackActionNotInterested:
		return "不感兴趣"
	case types.FeedbackActionMisjudged:
		return "误判"
	case types.FeedbackActionDeepDive:
		return "深度解读"
	}
	return string(a)
}

// rebuildAggregate 按库内状态重建聚合卡：每个兄弟条目各查各的 CardState 与内容元数据，
// header 从库存 card_json 原样解析回填（重建时拿不到任务名，且 header 不该随点击变）。
// clicked/force 只作用于被点的那一条——deep_dive 启动等 force 语义不能误伤兄弟条目。
func (s *Service) rebuildAggregate(ctx context.Context, clicked *types.Delivery, siblings []types.Delivery, force func(*CardState)) (string, error) {
	if s.deps.BuildAggCard == nil {
		return "", errors.New("BuildAggCard 未装配")
	}
	items := make([]CardInput, 0, len(siblings))
	for i := range siblings {
		sib := &siblings[i]
		st, err := s.cardState(ctx, sib.ID)
		if err != nil {
			// 中止整卡重建（对抗审查：零值渲染=用已知错误的"未表态"覆盖用户已提交的
			// 表态上屏，与单条路径"查失败不更新卡片"的既有裁决相反）。主操作已成功，
			// 卡片保持旧版本，下次点击自愈。
			return "", fmt.Errorf("查兄弟条目 %d 状态失败: %w", sib.ID, err)
		}
		if force != nil && sib.ID == clicked.ID {
			force(&st)
		}
		input := CardInput{
			BodyMD:     sib.BodyMD,
			DeliveryID: sib.ID,
			State:      st,
			Score:      int(sib.Score),
		}
		if sib.ContentItemID != nil {
			if ci, err := s.deps.Store.GetContentItem(ctx, *sib.ContentItemID); err == nil {
				input.Title = ci.Title
				input.URL = ci.URL
				input.PublishedAt = ci.PublishedAt
				if src, err := s.deps.Store.GetSource(ctx, ci.SourceID); err == nil {
					input.SourceTitle = src.Title
					input.Platform = src.Platform
				}
			}
		}
		items = append(items, input)
	}
	title, tmpl := parseAggHeader(clicked.CardJSON)
	return s.deps.BuildAggCard(AggregateCardInput{
		HeaderTitle: title, HeaderTemplate: tmpl, Items: items,
	}), nil
}

// parseAggHeader 从库存 card_json 解析 header 标题与色名；失败返回空串（构卡落兜底）。
// 本地实现而非引 feishu 包：feishu→(agent)→…→feedback 的依赖方向不可倒转。
func parseAggHeader(cardJSON []byte) (title, template string) {
	var c struct {
		Header struct {
			Title struct {
				Content string `json:"content"`
			} `json:"title"`
			Template string `json:"template"`
		} `json:"header"`
	}
	if err := json.Unmarshal(cardJSON, &c); err != nil {
		return "", ""
	}
	return c.Header.Title.Content, c.Header.Template
}

// contentOf 取投递对应的内容行（toast 标题回显用）；无内容或查失败返回 nil。
func (s *Service) contentOf(ctx context.Context, d *types.Delivery) (*types.ContentItem, error) {
	if d.ContentItemID == nil {
		return nil, nil
	}
	return s.deps.Store.GetContentItem(ctx, *d.ContentItemID)
}
