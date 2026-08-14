# 见微 Vane Web

Vane monorepo 的 React 单页前端。生产源码只位于 `src/`；设计验证原型位于
`prototypes/`，不得被生产路由或共享运行时代码导入。

## 目录

- `src/app/`：启动、顶层 shell、providers 与全局运行时边界。
- `src/pages/`：路由级组合；不作为可复用业务状态容器。
- `src/features/`：按 task、delivery、exploration 等业务域组织交互与状态。
- `src/components/ui/`：无业务知识的 UI primitives；不得依赖 `features/`。
- `src/shared/api/`：HTTP client、API 类型与前后端 canonical contract。
- `src/shared/runtime/`：浏览器生命周期、session storage 与 chunk 恢复。
- `src/shared/utils/`：无副作用的展示、时间和通用工具。
- `src/i18n/`：稳定文案键与本地化资源。
- `prototypes/`：独立 owner preview 与测试计划，不属于生产源码树。
- `scripts/`：bundle、版本和不可变发布树校验。

## 固定工具链

必须使用 Node `22.23.2`；`.node-version`、`.nvmrc`、`package.json#engines` 和
`.npmrc` 共同阻止版本漂移。依赖只认根目录唯一的 `package-lock.json`：

```bash
npm ci
npm run audit
npm test
npm run test:coverage
npm run typecheck
npm run build
```

`npm run build` 会在 `dist/.well-known/vane-release.json` 写入 monorepo Git SHA、
dirty 状态和排除 marker 自身后的确定性 tree SHA-256，并立即重新计算验证。
生产发布必须从 clean exact SHA 构建，并设置
`VANE_RELEASE_SHA=<exact-sha> VANE_REQUIRE_CLEAN_RELEASE=1`；本地 dirty 构建会被如实标记，
不能充当生产制品。

本仓不使用 GitHub Actions。合并、Gate、发布和生产验收由 monorepo 根目录固定脚本及
`AGENTS.md` 约束执行。
