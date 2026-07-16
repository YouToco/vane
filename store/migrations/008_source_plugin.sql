-- +goose Up

-- ① 三轴的前两轴落列。DEFAULT '' 只为让 ALTER 在非空表上成立，回填后立刻收紧。
ALTER TABLE sources ADD COLUMN platform   TEXT NOT NULL DEFAULT '';
ALTER TABLE sources ADD COLUMN capability TEXT NOT NULL DEFAULT '';

-- ② 回填。不按 status 过滤——disabled 只是不抓，不是不存在。
UPDATE sources SET platform='web', capability='feed'   WHERE type='rss';
UPDATE sources SET platform='web', capability='search' WHERE type='exa';
UPDATE sources SET platform='xhs', capability='search' WHERE type='tikhub_xhs';

-- ③ 幂等键去供应商化：纯前缀替换，不重新转义。
--    绝不能改用 url.Values.Encode() 重新生成——它按字母序排键，产出不同的字符串
--    → 与 Build() 算出的键不一致 → 重复源 → 双倍付费。
UPDATE sources SET url = 'vane://web/search?' || substring(url from length('exa://search?') + 1)
 WHERE type = 'exa';
UPDATE sources SET url = 'vane://xhs/search?' || substring(url from length('tikhub://xhs/search?') + 1)
 WHERE type = 'tikhub_xhs';

-- ③' 后置守卫：把最贵的静默失败变成响亮失败。
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sources WHERE url LIKE 'exa://%' OR url LIKE 'tikhub://%') THEN
        RAISE EXCEPTION '008: 幂等键去供应商化未覆盖全部行，仍有 exa:// 或 tikhub:// 残留';
    END IF;
    IF EXISTS (SELECT 1 FROM sources WHERE platform = '' OR capability = '') THEN
        RAISE EXCEPTION '008: 存在未映射的 sources 行（platform/capability 为空）';
    END IF;
END $$;

-- ④ type 列退役。前端兼容由 api 层派生（契约 §13.2）。
ALTER TABLE sources DROP COLUMN type;

-- ⑤ 内容种类。存量 231 条全部 article——DEFAULT 即正确语义。
ALTER TABLE content_items ADD COLUMN kind TEXT NOT NULL DEFAULT 'article';

-- +goose Down
ALTER TABLE sources ADD COLUMN type TEXT NOT NULL DEFAULT 'rss';
UPDATE sources SET type = CASE
    WHEN platform='web' AND capability='feed'   THEN 'rss'
    WHEN platform='web' AND capability='search' THEN 'exa'
    WHEN platform='xhs' AND capability='search' THEN 'tikhub_xhs'
    ELSE 'rss'
END;
UPDATE sources SET url = 'exa://search?' || substring(url from length('vane://web/search?') + 1)
 WHERE platform='web' AND capability='search';
UPDATE sources SET url = 'tikhub://xhs/search?' || substring(url from length('vane://xhs/search?') + 1)
 WHERE platform='xhs' AND capability='search';
ALTER TABLE content_items DROP COLUMN IF EXISTS kind;
ALTER TABLE sources DROP COLUMN IF EXISTS capability;
ALTER TABLE sources DROP COLUMN IF EXISTS platform;
