# Git 工作流规范（Vane monorepo）

> 参考大型开源项目实践（React / Vue / Next.js / Kubernetes / Angular），
> 按 solo + AI coding 的实际规模裁剪。2026-07-14 定稿。

## 分支模型：GitHub Flow（trunk-based）

参考 React / Next.js / Go：`main` 单主干常绿，短命功能分支，不用 Git Flow。

- **`main`**：永远可部署。合入前本地跑受影响的 `make test-server` / `make test-web` /
  `make test-contract`；push 到 main 后由 GitHub Actions `Deploy` workflow 自动部署
  （也可手动 dispatch 指定 ref）。生产凭证只存在于仓库 secrets 与 VPS，代码和测试环境均不可见。
- **功能分支**：从 main 切出，命名 `<type>/<slug>`：
  - `feat/agent-loop-policy`、`fix/rss-timeout`、`chore/bump-deps`、`docs/api-schema`
- **合并方式**：**squash merge**（保持 main 线性历史，React/Next.js 实践），
  squash 后的 commit message 遵循下方提交规范。
- **生命周期**：分支存活不超过一个里程碑；合完即删。
- **直推 main 的例外**：≤10 行的紧急修复 / 纯文档改动可直推，本地必需 Gate 必须通过；
  里程碑级功能一律走分支 + PR。
- Git Flow 的 release/develop 分支不用——那是为「同时维护多个发布版本」设计的
  （如 Kubernetes 的 release-1.x），SaaS 单版本部署用不上。

## 提交规范：Conventional Commits

参考 Angular / Vue / Electron。格式：`<type>(<scope>): <subject>`

- **type**：`feat` `fix` `docs` `refactor` `test` `chore` `ci` `perf`
- **scope**（可选）：模块名，如 `agent` `fetcher` `store` `deploy` `dashboard`
- **subject**：祈使句，首字母小写，不加句号
- 破坏性变更：type 后加 `!`（如 `feat(api)!: change auth header`）+ 正文 `BREAKING CHANGE:` 说明

```
feat(agent): add tool policy three-tier cascade
fix(fetcher): reject private IP redirect targets
chore(deps): bump pgx to v5.8
```

## 版本管理：SemVer + 里程碑 tag

- **MVP 期间 `v0.x.y`**：每完成一个里程碑打一个 minor tag——
  `v0.1.0`=M1 … `v0.7.0`=M7，**`v1.0.0` = MVP 全量验收**（连续 3 天自用零干预）。
- patch 版本（`v0.3.1`）：里程碑之间的修复。
- tag 一律打 **annotated tag**（`git tag -a v0.1.0 -m "M1: walking skeleton"`），
  push 后在 GitHub 建 Release，正文列本期变更（从 conventional commits 归纳）。
- **CHANGELOG.md**：keep-a-changelog 格式，每次打 tag 时更新 Unreleased 段落。
  v1.0.0 后如需自动化，接 release-please（Google 实践）。
- 产品 Release 使用根级 `v0.x.y` tag。服务端与 Web 部署状态仍独立记录；
  若未来对外发布 Go module，再同步创建 `server/v0.x.y` tag。

## PR 约定

- 标题即 squash 后的 commit message（Conventional Commits 格式）。
- 描述三段：**做了什么 / 为什么 / 怎么验证的**（附命令或截图）。
- self-review 后合并；部署 workflow 在远端做 exact-SHA CAS 校验、在线迁移、
  原子切换与失败自动回滚。

### 构建环境

- 部署构建在 GitHub Actions ubuntu runner 上执行，Go/Node 版本由
  `tools/toolchain.lock.json` 锁定；发布二进制从 clean exact-main 编译，
  构建与部署在同一 workflow 内完成，不经本地机器。

## 与里程碑排期的对应

| Tag | 里程碑 | 内容 |
|---|---|---|
| v0.1.0 | M1 | 行走骨架（healthz + migration 001） |
| v0.2.0 | M2 | LLM + 飞书通道 |
| v0.3.0 | M3 | 推送管道闭环（开始自用） |
| v0.4.0 | M4 | Agent Loop 对话交互 |
| v0.5.0 | M5 | 反馈闭环 |
| v0.6.0 | M6 | 源插件化 |
| v0.7.0 | M7 | Web Dashboard |
| v1.0.0 | — | MVP 全量验收 |
