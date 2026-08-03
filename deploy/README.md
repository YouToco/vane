# Vane 部署

`vane-deploy` 必须构建并安装一次性控制面产物
`bin/vane-research-prepare` 到 `/opt/vane/bin/vane-research-prepare`。它不是
systemd 常驻服务，只能按 V3 shadow runbook 临时执行，并只在该进程中注入
migration-owner 凭证；长期运行的 `vane` 服务不得获得该凭证或
`vane_research_v3_cutover_operator` 角色。

目标环境：ByteVirt VPS（Debian 11，2C/4G/40G），域名 `vane.zhuoqidev.com`。

## 架构

```
用户 → Caddy (host 网络, 80/443, 自动 TLS)
         ├→ vane.zhuoqidev.com
         │    ├ /api/*  → 127.0.0.1:8080  vane 二进制 (systemd)
         │    └ 其余    → /srv/vane-web 静态站 (SPA, try_files 回落 index.html)
         │                 ↑ 挂载自宿主机 /opt/vane/web（vane-web CI 部署产物）
         ├→ api.vane.zhuoqidev.com → 127.0.0.1:8080（后端 API 永久入口）
         └→ vane（非 owner: vane_server_runtime）
              ├→ /run/vane-research-gateway/gateway.sock
              │    └→ vane-research-gateway（独立 Provider 凭证与 DB 角色）
              ├→ 127.0.0.1:5432  Postgres 18 (docker)
              └→ 127.0.0.1:7233  Temporal 1.29 (docker)
                                   └ UI: 127.0.0.1:8233 (仅本机)
```

- Postgres / Temporal / Temporal UI 只绑 127.0.0.1，不对公网暴露。
- **vane 二进制默认只绑 `127.0.0.1:8080`**（config `server.addr` 默认值）：8080 不对公网可达，
  唯一入口是本机 Caddy（host 网络经 loopback 反代）。这样即便 VPS 未配防火墙，也无法
  `http://<vps>:8080/api/...` 直连绕过 Caddy 的 TLS 与反代加固（含登录限流的 XFF 可信链路，
  见 api/ratelimit.go clientIP）。Caddyfile 上游写死 `127.0.0.1:8080` 与之精确配对。
  - 确需对外监听（如临时联调）：设环境变量 `VANE_SERVER_ADDR=0.0.0.0:8080`（写进
    `/opt/vane/env/server.env`）或改 config `server.addr`，**并同时用防火墙（ufw/nftables）限制来源**——
    改绑是零依赖根治，防火墙是可并行的第二道防线。
- vane 二进制由私有 `vane-deploy` 控制面独立检出当前 `main` 的精确 SHA，
  在无生产凭证的 build runner 上完成完整 Gate 和构建，再由隔离的 deploy runner
  校验制品、上传到 `/opt/vane/bin/vane` 并交给 systemd 管理。源仓库 CI 不持有生产凭证，
  也不直接部署。
- Web Dashboard 同样由私有 `vane-deploy` 控制面按 `vane-web/main` 的精确 SHA
  独立 Gate 和构建；经制品校验后分别发布到 OSS/CDN 与 Cloudflare Pages。
  前后端源仓库都不持有生产 secrets。
- Caddy 用 host 网络：主域名托管 SPA + 反代 /api，api 子域整体反代 8080，证书自动签发。

## 首次部署（bootstrap）

```bash
# 1. 装 Docker
curl -fsSL https://get.docker.com | sh

# 2. 专用系统身份、目录和文件
useradd --system --home /nonexistent --shell /usr/sbin/nologin vane
useradd --system --home /nonexistent --shell /usr/sbin/nologin vane-migrate
useradd --system --home /nonexistent --shell /usr/sbin/nologin vane-research-gateway
mkdir -p /opt/vane/bin /opt/vane/web /opt/vane/env /etc/vane/credentials
# 把本目录（deploy/）内容拷到 /opt/vane/
# 三份示例彼此隔离：Docker owner 密码绝不进入长期主服务环境。
cp /opt/vane/.env.example /opt/vane/.env
cp /opt/vane/server.env.example /opt/vane/env/server.env
cp /opt/vane/research-gateway.env.example /opt/vane/env/research-gateway.env
chmod 600 /opt/vane/.env /opt/vane/env/server.env /opt/vane/env/research-gateway.env
chown vane:vane /opt/vane/env/server.env
chown vane-research-gateway:vane-research-gateway /opt/vane/env/research-gateway.env

# 3. 起基础设施
cd /opt/vane && docker compose up -d

# 3a. 先由一次性 owner 迁移进程完成 001..098 和 cluster role provision。
# 文件内容是 postgres://vane:<owner-password>@127.0.0.1:5432/vane?sslmode=disable
install -o vane-migrate -g vane-migrate -m 0400 /dev/null /etc/vane/credentials/migration_db_url
# 用安全编辑器写入 owner URL，禁止放入 server.env。
cp /opt/vane/vane-migrate.service /etc/systemd/system/
systemctl daemon-reload
systemctl start vane-migrate

# 3b. 为四个长期非 owner 登录设置互不相同的密码。
read -rsp 'V3 research DB password: ' VANE_RESEARCH_DB_PASSWORD; echo
read -rsp 'V3 edit recovery DB password: ' VANE_EDIT_RECOVERY_DB_PASSWORD; echo
read -rsp 'V3 LLM gateway DB password: ' VANE_GATEWAY_DB_PASSWORD; echo
read -rsp 'Vane server DB password: ' VANE_SERVER_DB_PASSWORD; echo
docker compose exec -T postgres psql -U vane -d vane \
  -v runtime_password="$VANE_RESEARCH_DB_PASSWORD" \
  -v edit_recovery_password="$VANE_EDIT_RECOVERY_DB_PASSWORD" \
  -v gateway_password="$VANE_GATEWAY_DB_PASSWORD" \
  -v server_password="$VANE_SERVER_DB_PASSWORD" <<'SQL'
ALTER ROLE vane_research_runtime LOGIN PASSWORD :'runtime_password';
ALTER ROLE vane_native_v3_edit_recovery_runtime LOGIN PASSWORD :'edit_recovery_password';
ALTER ROLE vane_research_llm_gateway_runtime LOGIN PASSWORD :'gateway_password';
ALTER ROLE vane_server_runtime LOGIN PASSWORD :'server_password';
SQL
unset VANE_RESEARCH_DB_PASSWORD VANE_EDIT_RECOVERY_DB_PASSWORD VANE_GATEWAY_DB_PASSWORD VANE_SERVER_DB_PASSWORD
# 将 server/runtime URL 写入 /opt/vane/env/server.env：
# VANE_DB_URL=postgres://vane_server_runtime:<server-password>@127.0.0.1:5432/vane?sslmode=disable
# VANE_DB_RESEARCH_CONTROL_URL=postgres://vane_server_runtime:<server-password>@127.0.0.1:5432/vane?sslmode=disable
# VANE_DB_RESEARCH_RUNTIME_URL=postgres://vane_research_runtime:<url-encoded-password>@127.0.0.1:5432/vane?sslmode=disable
# 将 recovery URL 写入以下 0400 credential，禁止放入 server.env：
install -o vane -g vane -m 0400 /dev/null /etc/vane/credentials/native_v3_edit_recovery_db_url
# 文件内容使用 vane_native_v3_edit_recovery_runtime，绝不能使用 server、research 或 owner 登录。
# 将 gateway URL 与 Provider key 分别写入以下 0400 credential 文件：
install -o vane-research-gateway -g vane-research-gateway -m 0400 /dev/null /etc/vane/credentials/gateway_db_url
install -o vane-research-gateway -g vane-research-gateway -m 0400 /dev/null /etc/vane/credentials/research_llm_api_key_gen1
# gateway_db_url 内容使用 vane_research_llm_gateway_runtime，绝不能使用 owner。
# research-gateway.env 还必须填写 `id -u vane` 得到的 VANE_GATEWAY_ALLOWED_UID。
# 路由 JSON 中每个冻结 generation 都必须对应一个独立 LoadCredential；旧 generation
# 在 Temporal 保留窗口结束前不能删除，缺失时网关会在 Provider send 前 fail-closed。

# 4. systemd
cp /opt/vane/vane.service /opt/vane/vane-research-gateway.service \
  /opt/vane/vane-research-gateway.socket /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now vane-research-gateway.socket
systemctl enable vane-migrate vane-research-gateway vane

# 5. 之后 push main；私有 vane-deploy 控制面定时解析最新 SHA，
#    独立 Gate 通过后上传二进制、优雅重启并执行生产探针
```

## 日常运维

```bash
docker compose ps               # 基础设施状态
systemctl status vane           # 应用状态
journalctl -u vane -f           # 应用日志
journalctl -u vane-migrate -u vane-research-gateway -f
docker compose logs temporal    # Temporal 日志

# 核验 8080 只绑 loopback（部署后 / 改过 config 后必查）：应为 127.0.0.1:8080，
# 若显示 0.0.0.0:8080 说明加固被覆盖——查 /opt/vane/config/config.yaml 是否残留
# 旧 addr: ":8080"，或 server.env 里误设了 VANE_SERVER_ADDR。启动日志也会 WARN 提示。
ss -ltnp | grep ':8080'
```

## 升级失败与回滚

- 091–098 是 forward-only 安全迁移。已出现 reservation、attempt、receipt 或 quota-floor
  记录后，Down 会 fail-closed；运维不得删除证据或手工恢复旧 ACL 来强行降级。
- 第一个 V3 Boss Gate 前，新 binary、migration、socket 和 gateway 以“能力在线、任务 authority
  hard-dark”方式部署并完成探针，不改任何 Temporal schedule。Owner Agent 无条件提供原生 V3
  create，因此 `VANE_PIPELINE_RESEARCH_V3_RUNTIME_ENABLED=true` 是 server 启动前置条件；保持
  shadow/authority ID 为空且数据库没有 enabled authority，就不会切流任何任务。现有
  `TOOL_RUNTIME_CANARY` 属于旧 compiled runtime，不能拿来切 V3。
- 新服务启动失败时，保留数据库版本和全部不可变记录，停止切流并修复后向前发布。只有在
  预先保存的上一版 binary/unit 仍在运行且尚未产生任何 V3 effect 时，才可恢复上一制品；
  不运行 migration Down。旧版若要求 owner 常驻，视为短时事故模式，必须立即回到本边界。
- Shadow、cutover 与 rollback 始终使用 exact-task canary；正式 V3 运行只认 Action 身份及数据库
  enabled authority token。持久 runtime 随 Agent-first server 始终开启，但本身不授予任务权限。
  回滚不删除网关和 retained route generation，保留它们完成在途结算。禁止暂停/改写周一 9:00
  正式 schedule 作为配置回滚手段。
- endpoint/key 轮换只追加 route generation 和对应 systemd credential。旧 generation 在
  Temporal retention、run capability TTL 和所有在途 run 全部结束前不得移除。

## 凭证

凭证按进程最小化分开保存，绝不进本仓库：

- `vane-migrate` 独占 owner URL，只运行迁移和 runtime role provision；主服务启动时不迁移。
- `vane` 的 `VANE_DB_URL` 固定使用 `vane_server_runtime`，V3 research pool 固定使用
  `vane_research_runtime`；两者都不是 owner、superuser 或 BYPASSRLS。
- `vane-research-gateway` 只从 systemd credential 读取 gateway DB URL 和 Provider key；它的
  environment 明确不含主 DB、Agent、fetch 或通用 LLM 凭证。
- gateway 仅接受主服务 Unix Socket peer UID，HTTP 请求中只能出现 reservation、digest 和
  opaque run capability；prompt、模型策略和结算事实由数据库冻结记录决定。

任何角色/ACL 漂移、owner 误配、额外角色继承或 socket 身份不符都会 fail-closed。迁移与网关
部署不会修改 Temporal schedule；周一 9:00 的正式调度保持原值。
