import { useEffect, useState } from "react";
import { api, ApiError } from "../api";
import type { Profile as ProfileData } from "../api";

// 画像查看页（M7 功能 6.3，只读版）：展示当前 owner 的结构化标签 + 摘要画像。
// 只读——画像的产生与修正入口在飞书对话（首采 2.1 / 修正 2.3）与反馈演化（2.2），
// 这里是系统性回看的地方，不是第二个编辑入口。编辑写回涉及演化恒赢逻辑
// （removed_tags 黑名单，Gate ⑧），留二期，故本页不做任何写操作。

const BEIJING_TZ = "Asia/Shanghai";

// 后端全 UTC，换算只在展示层做（与 History.tsx / Observability.tsx 同策略）。
function fmtBeijing(iso?: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-CN", { timeZone: BEIJING_TZ, hour12: false });
}

export default function Profile() {
  const [profile, setProfile] = useState<ProfileData | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  // 画像尚未生成（owner 从未走首采）后端回 404——这是正常空态，与加载失败区分开。
  const [notGenerated, setNotGenerated] = useState(false);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    api
      .profile()
      .then((p) => {
        if (!alive) return;
        setProfile(p);
        setNotGenerated(false);
        setLoadError("");
      })
      .catch((err) => {
        if (!alive) return;
        if (err instanceof ApiError && err.status === 404) {
          setNotGenerated(true);
          setProfile(null);
          setLoadError("");
          return;
        }
        setLoadError(err instanceof ApiError ? err.message : "加载失败");
        setProfile(null);
        setNotGenerated(false);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  return (
    <div className="page">
      <div className="page-head">
        <h2 className="page-title">用户画像</h2>
        <button
          type="button"
          className="btn btn-ghost btn-mini"
          onClick={() => setNonce((n) => n + 1)}
          disabled={loading}
        >
          {loading ? <span className="spinner spinner-dark" /> : "刷新"}
        </button>
      </div>
      <p className="muted src-intro">
        机器人对你的理解：结构化标签 + 摘要画像。画像由对话首采与反馈演化维护——
        要修改请在飞书对话里进行（如「帮我把关注领域改成…」），这里只做查看。
      </p>

      {loadError && <div className="alert alert-error">{loadError}</div>}

      {loading ? (
        <div className="page-loading">
          <span className="spinner spinner-dark" /> 加载中…
        </div>
      ) : notGenerated ? (
        <div className="empty-hint">
          画像尚未生成。在飞书对话里向机器人做一次自我介绍（行业 / 职业 / 关注领域），
          它会自动建立你的画像。
        </div>
      ) : profile ? (
        <>
          <section className="card">
            <dl className="kv-grid">
              <div>
                <dt>行业</dt>
                <dd>{profile.industry || <span className="muted">未填写</span>}</dd>
              </div>
              <div>
                <dt>职业</dt>
                <dd>{profile.occupation || <span className="muted">未填写</span>}</dd>
              </div>
              <div>
                <dt>更新时间（北京）</dt>
                <dd>{fmtBeijing(profile.updated_at)}</dd>
              </div>
            </dl>
          </section>

          <h3 className="section-title">兴趣标签</h3>
          <section className="card">
            {profile.tags.length === 0 ? (
              <div className="empty-hint">还没有兴趣标签。</div>
            ) : (
              <div className="tag-cloud">
                {profile.tags.map((t) => (
                  <span key={t} className="badge badge-type">
                    {t}
                  </span>
                ))}
              </div>
            )}
          </section>

          <h3 className="section-title">摘要画像</h3>
          <section className="card">
            {profile.summary ? (
              <pre className="profile-summary">{profile.summary}</pre>
            ) : (
              <div className="empty-hint">摘要画像还没生成（需累积几轮反馈后由演化写入）。</div>
            )}
            <p className="muted chart-note">
              负偏好（不感兴趣的内容）由演化写在摘要末尾的「不感兴趣：」句式里，不单列。
            </p>
          </section>

          {profile.removed_tags.length > 0 && (
            <>
              <h3 className="section-title">已移除标签（黑名单）</h3>
              <section className="card">
                <p className="muted chart-note">
                  你亲手删过的标签，演化不会再把它们加回来（Gate ⑧ 人工修正恒赢）。
                </p>
                <div className="tag-cloud">
                  {profile.removed_tags.map((t) => (
                    <span key={t} className="badge badge-muted profile-tag-removed">
                      {t}
                    </span>
                  ))}
                </div>
              </section>
            </>
          )}
        </>
      ) : null}
    </div>
  );
}
