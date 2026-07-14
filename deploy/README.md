# Vane 部署

目标环境：ByteVirt VPS（Debian 11，2C/4G/40G），域名 `vane.zhuoqidev.com`。

## 架构

```
用户 → Caddy (host 网络, 80/443, 自动 TLS)
         └→ localhost:8080  vane 二进制 (systemd)
                              ├→ 127.0.0.1:5432  Postgres 18 (docker)
                              └→ 127.0.0.1:7233  Temporal 1.29 (docker)
                                                   └ UI: 127.0.0.1:8233 (仅本机)
```

- Postgres / Temporal / Temporal UI 只绑 127.0.0.1，不对公网暴露。
- vane 二进制由 CI（GitHub Actions）构建并 scp 到 `/opt/vane/bin/vane`，systemd 管理。
- Caddy 用 host 网络反代 localhost:8080，证书自动签发。

## 首次部署（bootstrap）

```bash
# 1. 装 Docker
curl -fsSL https://get.docker.com | sh

# 2. 目录 + 文件
mkdir -p /opt/vane/bin
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
```

## 凭证

真实密码/密钥只存在 VPS 的 `/opt/vane/.env` 和私有库 `YouToco/my-credentials`，
绝不进本仓库。
