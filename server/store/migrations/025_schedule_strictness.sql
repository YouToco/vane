-- 025: schedules 加 push_strictness —— 任务级推送门槛档位（2026-07-19 Boss 拍板）
--
-- 背景：workflow Select 此前是纯 TopN 无分数下限，2026-07-19 07:26 UTC 一批 5 条
-- 与画像无关的内容全部被打 0 分后照样出卡推送（deliveries 155-159），"不相关的不推"
-- 被击穿。Boss 拍板：门槛不在代码里写死分数，而是任务级动态档位——用户对某任务要求
-- 严格就高、宽松就放宽。
--
-- 为什么是档位（loose/normal/strict）而不是数字列：Boss 质疑过"分数凭模型感觉"——
-- 中段分（45 vs 55）确实无校准意义。档位把消费面钉在语义上（loose=只滤打分 prompt
-- 明确指令的 0-20"不该推"档；normal/strict 逐级抬高），数字映射收敛在代码常量
-- （types.PushStrictness.MinKeepScore）一处，将来调整映射不动数据。
--
-- NULL = 未设置：Select 按全局兜底（loose 等价）处理。刻意不 DEFAULT 'loose'——
-- "用户没说" 与 "用户明确要宽松" 是两种事实，塌缩成一个值就再也分不开了
-- （与 push_batches 计数字段"没跑"≠"跑了得 0"同一条纪律）。

-- +goose Up
ALTER TABLE schedules ADD COLUMN push_strictness text
    CHECK (push_strictness IN ('loose', 'normal', 'strict'));

-- +goose Down
ALTER TABLE schedules DROP COLUMN push_strictness;
