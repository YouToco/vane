-- +goose Up
-- probewatch 告警指纹落盘（探针实现债 P2，2026-07-20）。
--
-- 此前指纹只存进程内存，重启即忘——红灯存续期间每次部署重启都对同一红灯重发
-- 告警卡（2026-07-19 一天 6 次部署收到 5 张同内容卡的生产实锤）。落盘后同一
-- 红灯集合跨重启不重发；「部署后复跑」保留（启动后 3 分钟照跑，新红灯照发）。
--
-- 系统级单行表：无 tenant_id，刻意不入 022 的 RLS 覆盖面——它不含任何用户数据，
-- 只有一串探针 ID 拼成的指纹。
CREATE TABLE probewatch_state (
    id               smallint PRIMARY KEY DEFAULT 1 CONSTRAINT probewatch_state_singleton CHECK (id = 1),
    last_fingerprint text NOT NULL DEFAULT '',
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- 预置唯一行：读写路径都按"行恒存在"写（写侧仍用 upsert 兜底防误删）。
INSERT INTO probewatch_state (id) VALUES (1);

-- +goose Down
DROP TABLE probewatch_state;
