# 见微 Vane 设计系统 v3 —「天青雾 / Celadon Mist」

> 2026-07-19 三改定稿（vane-web#19）。Token 实体在 [`src/index.css`](../src/index.css)，
> 本文档解释设计决策与使用规则；两处冲突时以 index.css 为准。

## 品牌故事

**Logo：相风铜乌**——汉代观象台顶的风向铜鸟，比西方风向鸡早一千年，
是「vane」的中华本尊。铜乌立杆头，鎏金喙指向风来的方向。

**界面色系：汝窑天青**（「雨过天青云破处，这般颜色做将来」）。
底色纯净——light 近纯白、dark 近黑；UI 本身黑白克制，色彩只活在
大面积柔雾渐变里：**天蓝 → 天青 → 碧**。气质参照 OpenAI 官网插图的
清透 pastel blur：氛围感交给光晕、渐变字与鼠标光斑，不靠色块。

器物（铜乌）与釉色（天青）同属一个中华语汇；刻意避开 AI 产品
烂大街的紫蓝渐变与俗套科技蓝。

## Logo

实体在 [`src/components/brand/Logo.tsx`](../src/components/brand/Logo.tsx)：

| 组件 | 用途 | 颜色 |
|---|---|---|
| `<LogoMark />` | 品牌位（sidebar / header / 登录页 / favicon） | 印章式写死：青铜黑底 + 铜绿身 + 鎏金喙，**跨主题恒定** |
| `<Logo />` | 正文 / 单色场景 | `currentColor` |

favicon 是同一图形的独立文件 [`public/vane.svg`](../public/vane.svg)，改 logo 时两处同步。

**圆角对齐规则**：印章 rect 的 rx 是 32 视图里的 8（即 25%）。给 LogoMark
外层容器加阴影时，容器圆角必须等于「25% × 显示尺寸」——`size-16` 配
`rounded-2xl`(16px)、`size-12` 配 `rounded-xl`(12px)——否则阴影露直角
（vane-web#19 第四轮修过的坑）。

## 排版

- 正文 / UI：`Geist Variable`（无衬线）。
- **标题层：`font-heading` = Noto Serif SC**（衬线宋体，报刊气质）。
  仅用于落地页 h1/h2 与少数品牌文案；应用内 UI 不用衬线。

## 色彩层次

| 层 | Token | 用途 | 规则 |
|---|---|---|---|
| 界面主操作 | `primary` | 按钮、选中态 | light=近黑，dark=**纯白**（OpenAI 式克制）。品牌感不进 UI 控件 |
| 品牌强调 | `brand`(天青) / `brand-strong` / `signal`(碧/薄荷) | 高亮、hover、focus ring、图标 | `brand-strong` light 下够深可做文字；`brand` 做背景/边框；`signal` 只做点缀（呼吸点、光点） |
| 渐变 | `--grad-a/b/c` | hero 标题、大数字 | 「雨过天青」：天蓝→天青→碧。只用于 ≥24px bold |
| 光晕 | `--glow-a/b/c` | 彩雾三团 | 自带透明度，`bg-[var(--glow-a)]` |
| 氛围 | `--glow-spot` / `--grid-line` | 鼠标光斑 / 56px 网格线 | 网格顶部渐隐；光斑触屏与 reduced-motion 下不启用 |
| 语义色 | `destructive` / emerald | 错误 / 成功 | 不动品牌化 |

Tailwind 用法：brand 层已注册进 `@theme`，可用不透明度修饰符——
`text-brand-strong`、`bg-brand/10`、`border-brand/40`、`shadow-brand/5`。
渐变/光晕/网格用 arbitrary value：`from-[var(--grad-a)]`、`bg-[var(--glow-b)]`、
`bg-[linear-gradient(...var(--grid-line)...)]`。

## 对比度基线

- `brand-strong`(light L0.48) 对白底 ≈ 5.8:1 —— 可做小字。
- `brand`(light L0.70) 对白底 <3:1 —— **禁止做正文字色**，只做背景/边框/大图形。
- 渐变三停 light 版压在 L0.62–0.66，只出现在 ≥24px bold 标题（大文本 3:1 达标）。
- dark `primary`=白(L0.965) 配深字(L0.155)，对比充足。

## 多语言（i18n）

- 零依赖 context：[`src/i18n/`](../src/i18n)。**8 种语言**：简中 zh（source of
  truth）· 繁中 zh-Hant · en · ja · ko · es · fr · de，均以 `Dict = typeof zh`
  约束键齐全（缺键编译报错）。语言清单/原生名/`html lang` 映射在 `LOCALES` 表。
- 检测顺序：localStorage(`vane.locale`) → `navigator.language`（zh 变体按
  tw/hk/mo/hant 分流简繁，其他取语言主标签，未覆盖回退 en）；切换同步 `html lang`。
- 切换器是 header 的地球下拉菜单（移动端只显示图标）。
- 新增语言：加一个 `Dict` 字典文件 + `LOCALES` 加一行 + `DICTS` 注册，三步完成。
- 翻译口径:品牌名简繁用「见微 Vane」，其余语言用「Vane」；飞书在海外语言里叫
  Lark；小红书在非中文语言里叫 RED；文案要按目标语言习惯重写，不做直译。
- 文案禁止硬编码进组件（落地页已全量迁移，应用内页面随后续 PR）。
- 动效数据与语言解耦：列表项存索引，渲染时查字典（参照 `FilterShowcase.tsx`）。
- 带内部状态机 + AnimatePresence 的组件跨语言切换用 `key={locale}` 整体重挂。
- 衬线标题字体 Noto Serif SC 覆盖中日文；韩/欧文自动 fallback 到系统衬线族。

## 历史

- v1「破晓琥珀」（琥珀金+夜青，风向标指针 logo）→ Boss review：紫蓝太大众
  的替代方案，但琥珀被评「土」。
- v2「青铜鎏金」（铜绿+鎏金+青铜黑，相风铜乌 logo）→ logo 保留，
  界面暖调仍不合意。
- v3「天青雾」（本版）：logo 不动，界面转清透冷调。三次换装每次只动
  `index.css`（±logo/favicon），组件零改动——token 分层的意义。
