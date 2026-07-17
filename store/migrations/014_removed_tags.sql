-- 014: 人工移除标签黑名单（Gate ⑧ FAIL 修复，m5-profile-contract §16 修订）
--
-- 2026-07-17 实测：人工删掉的标签 2 分钟后被演化当"合法新增"加回——契约「人工删掉/
-- 加回的标签，演化无权再动」此前在数据层无承载（只增不减守门只防删、不防加回）。
-- removed_tags 记录人工删除过且未被人工加回的标签：人工删标签入列、人工加回出列
-- （均在 UpsertProfileFields 单语句内维护），演化新增标签时硬过滤本列。

-- +goose Up

ALTER TABLE profiles ADD COLUMN removed_tags TEXT[] NOT NULL DEFAULT '{}';

-- +goose Down

ALTER TABLE profiles DROP COLUMN removed_tags;
