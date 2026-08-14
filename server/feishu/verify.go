package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// 凭证校验刻意不走 lark SDK 而是裸 net/http：SDK 会把 code/msg 包进多层
// error，而向导第 3 步需要把原始 code、msg、activate_status 翻成精确的
// 中文人话（Detail 字段）给非技术用户看，直接拿原始响应最可控。
const (
	tenantTokenURL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
	botInfoURL     = "https://open.feishu.cn/open-apis/bot/v3/info"
)

// verifyHTTPClient 独立于业务 client：校验是用户点按钮触发的交互操作，
// 10 秒超时足够且能让前端尽快拿到"网络不通"的反馈。
var verifyHTTPClient = &http.Client{Timeout: 10 * time.Second}

// activateStatusDetail 把 bot/v3/info 的 activate_status 翻译成人话。
// 只有 2 是健康态（M2 事实基准）；3 常见于"创建应用后还没发布版本"，
// 向导第 4 步发布版本后自然就绪，因此只提示、不阻塞保存。
var activateStatusDetail = map[int]string{
	0: "机器人尚未安装到租户（初始状态），请在飞书控制台完成应用安装",
	1: "机器人已被租户停用，请在飞书管理后台重新启用",
	2: "机器人已启用",
	3: "应用尚未发布版本（安装后待启用）——可以先保存并连接，向导后续步骤发布版本后即就绪",
}

// botInfo 是 bot/v3/info 响应里 bot 对象的必要字段子集。
type botInfo struct {
	ActivateStatus int    `json:"activate_status"`
	AppName        string `json:"app_name"`
	OpenID         string `json:"open_id"`
}

// verifyCredentials 按"换 token → 查机器人信息"两步校验凭证，
// 每一步失败都在 Detail 里给出面向向导用户的中文原因。
func verifyCredentials(ctx context.Context, appID, appSecret string) VerifyResult {
	var res VerifyResult

	token, failDetail := fetchTenantToken(ctx, appID, appSecret)
	if token == "" {
		res.Detail = failDetail
		return res
	}
	res.CredentialsOK = true

	info, failDetail := fetchBotInfo(ctx, token)
	if info == nil {
		res.Detail = failDetail
		return res
	}
	res.BotName = info.AppName

	statusText, known := activateStatusDetail[info.ActivateStatus]
	if !known {
		statusText = fmt.Sprintf("未知的机器人状态（activate_status=%d），请到飞书控制台确认应用状态", info.ActivateStatus)
	}
	if info.ActivateStatus == 2 {
		res.BotOK = true
		res.Detail = fmt.Sprintf("凭证有效，机器人「%s」已启用", info.AppName)
	} else {
		// 凭证本身没问题，只是机器人未就绪：Detail 说明原因但不算校验失败，
		// 让前端可以继续走"保存并连接"。
		res.Detail = fmt.Sprintf("凭证有效，但机器人未就绪：%s", statusText)
	}
	return res
}

// fetchTenantToken 换取 tenant_access_token。失败时 token 返回空串，
// failDetail 为人话原因。
func fetchTenantToken(ctx context.Context, appID, appSecret string) (token, failDetail string) {
	payload, _ := json.Marshal(map[string]string{
		"app_id":     appID,
		"app_secret": appSecret,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tenantTokenURL, bytes.NewReader(payload))
	if err != nil {
		return "", "构造校验请求失败：" + err.Error()
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := verifyHTTPClient.Do(req)
	if err != nil {
		return "", "无法访问飞书开放平台（网络错误）：" + err.Error()
	}
	defer resp.Body.Close()

	// 注意：token 在响应顶层而非 data 下（M2 事实基准实测）。
	var body struct {
		Code              int    `json:"code"`
		Msg               string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", "飞书开放平台返回了无法解析的响应：" + err.Error()
	}
	if body.Code != 0 {
		return "", fmt.Sprintf("App ID 或 App Secret 校验失败（code %d：%s），请回飞书控制台「凭证与基础信息」页核对后重试", body.Code, body.Msg)
	}
	if body.TenantAccessToken == "" {
		return "", "飞书返回成功但缺少 tenant_access_token，请稍后重试"
	}
	return body.TenantAccessToken, ""
}

// fetchBotInfo 用 tenant_access_token 查询机器人信息。失败时 info 为 nil。
func fetchBotInfo(ctx context.Context, token string) (info *botInfo, failDetail string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, botInfoURL, nil)
	if err != nil {
		return nil, "构造机器人信息请求失败：" + err.Error()
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := verifyHTTPClient.Do(req)
	if err != nil {
		return nil, "无法访问飞书开放平台（网络错误）：" + err.Error()
	}
	defer resp.Body.Close()

	var body struct {
		Code int     `json:"code"`
		Msg  string  `json:"msg"`
		Bot  botInfo `json:"bot"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, "飞书开放平台返回了无法解析的响应：" + err.Error()
	}
	if body.Code != 0 {
		// 最常见的失败原因是应用没开通「机器人」能力，把排查方向直接写进人话。
		return nil, fmt.Sprintf("获取机器人信息失败（code %d：%s），请确认应用已开通「机器人」能力", body.Code, body.Msg)
	}
	return &body.Bot, ""
}
