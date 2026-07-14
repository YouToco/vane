# 见微 Vane — Web Dashboard

自用单用户 Dashboard（MVP 里程碑 M7）：信源管理 / 画像查看编辑 / 推送历史 / 成本监控。

## 技术栈

Vite 8 + React 19 + TypeScript 7（SPA，纯静态构建）

## 部署（前后端分离，双线 CDN）

- 域名 `vane.zhuoqidev.com`，阿里云 DNS 分线路：
  - 默认（国内）→ 阿里云 CDN 回源 OSS
  - 境外 → Cloudflare Pages
- 后端 API：`https://api.vane.zhuoqidev.com`（Go，仓库 [YouToco/vane](https://github.com/YouToco/vane)）
- CI：push main → typecheck + build；部署接线在 M7（见 ci.yml 注释）

## 开发

```bash
npm install
npm run dev        # /api 代理到线上 API，见 vite.config.ts
npm run build
```

## 工作流规范

分支 / 提交 / 版本管理遵循 [vane/docs/git-workflow.md](https://github.com/YouToco/vane/blob/main/docs/git-workflow.md)
（GitHub Flow + Conventional Commits + SemVer 里程碑 tag）。
