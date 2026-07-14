# Changelog

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 SemVer。
里程碑与 tag 对应关系见 [docs/git-workflow.md](docs/git-workflow.md)。

## [Unreleased]

### 已知取舍（M2 审查记录，后续里程碑处理）
- logout 仅清 cookie，无状态 HMAC token 到期前（30 天）理论上仍有效——泄漏 token 无法即时吊销。
  收紧方案：缩短 TTL 或签名里混入服务端 "valid-after" 时间戳。
- 优雅关停不等待飞书消息处理 goroutine：关停瞬间在途消息的 LLM 记账可能丢失一条（仅日志）。
  收紧方案：Manager 加处理中 goroutine 的 WaitGroup，st.Close() 前 Wait。

## [0.2.0] - 2026-07-14 — M2 LLM + 飞书通道 + Dashboard

### Added
- llm/：DeepSeek v4-flash 客户端（OpenAI 兼容，纯标准库）+ 信号量限流 + 调用记账
  （trace_id / span_name / cache 命中三态 / cost_usd 计算，写 llm_calls 表）
- feishu/：lark-oapi-go v3.9.9 WS 长连接管理器（连接/重配/状态）+ 消息处理链
  （飞书消息 → DeepSeek → 交互卡片回复）+ 凭证校验 + owner 授权白名单
- api/：HMAC 无状态会话 + 登录（限流 + body 限制 + 常数时间对比）+ 飞书接入管理端点
- store/：migration 002（settings 表）+ settings/llmcalls/users 数据访问
- Web Dashboard（vane-web）：登录 + 状态总览 + 飞书接入五步向导
- 部署：Caddy 主域名托管 SPA + 反代 /api；vane-web CI 自动部署 dist 到 VPS
- 安全加固：VPS sshd 关闭密码登录（仅 key 认证）

### 过程
6 agent 并行实现（先 1 个研究 agent 实测 lark SDK/DeepSeek v4 事实基准）+ 3 视角对抗审查
（含独立 verify 层，9 confirmed / 0 误报）：修复 1 major（reload 并发泄漏活 WS 连接）+
5 minor（登录限流/body 限制/owner 授权白名单/owner 捕获 race/迁移注释）+ 1 info（key 常量导出）。

## [0.1.0] - 2026-07-14 — M1 行走骨架

### Added
- types/：9 个领域实体 + 7 组枚举 + AppError 错误体系（自定义 Is、哨兵映射、可重试默认表）
- config/：Viper 配置加载，VANE_ 环境变量覆盖，敏感键显式 BindEnv
- store/：pgxpool 连接层 + goose 嵌入式迁移 001（9 张表）+ 启动期数据库可达等待
- cmd/server：/healthz + /readyz 探针、JSON 日志、优雅关停
- CI/CD：部署改 SSH key 认证，部署后 readyz 健康验证（消灭假绿）
- systemd 最小加固（NoNewPrivileges / ProtectSystem=strict / ProtectHome / PrivateTmp）

### 过程
4 agent 并行实现 + 3 视角对抗审查（24 findings，8 major 全修复：
4 处 Go/SQL 可空性契约冲突、llm_calls 补 span_name/user_id、
环境变量命名错位、部署无健康验证、SSH 密码认证）。
