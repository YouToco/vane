# Vane 部署

目标环境：ByteVirt VPS（Debian 11，2C/4G/40G），域名 `vane.zhuoqidev.com`。

## 架构

```
用户 → Caddy (host 网络, 80/443, 自动 TLS)
         ├→ vane.zhuoqidev.com
         │    ├ /api/*  → 127.0.0.1:8080  vane 二进制 (systemd)
         │    └ 其余    → /srv/vane-web 静态站 (SPA, try_files 回落 index.html)
         │                 ↑ 挂载自宿主机 /opt/vane/web（vane-web CI 部署产物）
         ├→ api.vane.zhuoqidev.com → 127.0.0.1:8080（后端 API 永久入口）
         └→ vane 二进制
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
    `/opt/vane/.env`）或改 config `server.addr`，**并同时用防火墙（ufw/nftables）限制来源**——
    改绑是零依赖根治，防火墙是可并行的第二道防线。
- vane 二进制由 CI（GitHub Actions）构建并 scp 到 `/opt/vane/bin/vane`，systemd 管理。
- Web Dashboard（vane-web 仓库）由其 CI 构建，dist 内容 scp 到 `/opt/vane/web/`，
  compose 里以只读卷 `./web:/srv/vane-web:ro` 挂给 Caddy 做静态托管；
  两个仓库共用同一组 CI secrets（VPS_HOST/VPS_PORT/VPS_USER/VPS_SSH_KEY）。
- Caddy 用 host 网络：主域名托管 SPA + 反代 /api，api 子域整体反代 8080，证书自动签发。

## 首次部署（bootstrap）

```bash
# 1. 装 Docker
curl -fsSL https://get.docker.com | sh

# 2. 目录 + 文件
mkdir -p /opt/vane/bin /opt/vane/web   # web 目录先建好，caddy 卷挂载不会因缺目录失败
# 把本目录（deploy/）内容拷到 /opt/vane/
# 复制 .env.example 为 /opt/vane/.env 并填入真实密码/密钥
chmod 600 /opt/vane/.env   # 密钥文件必须锁权限

# 3. 起基础设施
cd /opt/vane && docker compose up -d

# 4. systemd
cp /opt/vane/vane.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable vane

# 5. 之后每次 push main，CI 自动构建上传二进制并 restart vane
```

## 日常运维

```bash
docker compose ps               # 基础设施状态
systemctl status vane           # 应用状态
journalctl -u vane -f           # 应用日志
docker compose logs temporal    # Temporal 日志

# 核验 8080 只绑 loopback（部署后 / 改过 config 后必查）：应为 127.0.0.1:8080，
# 若显示 0.0.0.0:8080 说明加固被覆盖——查 /opt/vane/config/config.yaml 是否残留
# 旧 addr: ":8080"，或 .env 里误设了 VANE_SERVER_ADDR。启动日志也会 WARN 提示。
ss -ltnp | grep ':8080'
```

## 凭证

真实密码/密钥只存在 VPS 的 `/opt/vane/.env` 和私有库 `YouToco/my-credentials`，
绝不进本仓库。
