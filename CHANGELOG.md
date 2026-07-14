# Changelog

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 SemVer。
里程碑与 tag 对应关系见 [docs/git-workflow.md](docs/git-workflow.md)。

## [Unreleased]

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
