# Vane Web 协作约束

本文件约束 `web/**`，与 monorepo 根 `AGENTS.md` 共同生效。

## 边界

- 生产代码只放 `src/`；原型只放 `prototypes/`。生产入口、路由和共享层不得导入原型。
- `src/shared/api/` 只承载 API client、wire 类型和 canonical contract。
- `src/shared/runtime/` 承载浏览器状态与生命周期副作用；纯函数放 `src/shared/utils/`。
- API shape 变化必须和 `server/` 契约、兼容顺序及前后端 Gate 一起审查。
- 不新增第二个 lockfile，不提交 `node_modules/`、`dist/`、`coverage/` 或本地凭证。

## 工具与 Gate

- Node 必须精确为 `22.23.2`，安装依赖只用 `npm ci`。
- 修改 TypeScript/React：至少运行 `npm run audit`、`npm test`、`npm run typecheck`、`npm run build`。
- 涉及共享逻辑或覆盖率：再运行 `npm run test:coverage`。
- 涉及原型：同时运行 `npm run prototype:p0a:build`，并保持 isolation tests 通过。
- 发布只接受 clean exact monorepo SHA；`dist/vane-release.json` 的 revision、
  dirty 标志和 tree digest 必须由固定脚本生成、复验，不能手工编辑；生产 Gate 必须设置
  `VANE_RELEASE_SHA` 和 `VANE_REQUIRE_CLEAN_RELEASE=1`。

本目录没有 GitHub Actions。不得绕过根目录本地 Gate 或在前端脚本中持有生产凭证。
