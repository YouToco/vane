import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import {
  Copy,
  Crown,
  Loader2,
  MailPlus,
  RefreshCw,
  Shield,
  Trash2,
  UserRound,
  Users,
} from "lucide-react";
import { toast } from "sonner";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  api,
  ApiError,
  type MeResponse,
  type WorkspaceInvite,
  type WorkspaceMember,
  type WorkspaceRole,
} from "@/shared/api/client";

const roleLabels: Record<WorkspaceRole, string> = {
  owner: "Owner",
  admin: "Admin",
  member: "Member",
};

function inviteStatus(invite: WorkspaceInvite): "pending" | "used" | "revoked" | "expired" {
  if (invite.consumed_at) return "used";
  if (invite.revoked_at) return "revoked";
  if (new Date(invite.expires_at).getTime() <= Date.now()) return "expired";
  return "pending";
}

function roleIcon(role: WorkspaceRole) {
  if (role === "owner") return <Crown className="size-3.5" />;
  if (role === "admin") return <Shield className="size-3.5" />;
  return <UserRound className="size-3.5" />;
}

export default function WorkspaceMembers({
  me,
  onAuthorityChanged = () => location.reload(),
}: {
  me: MeResponse;
  onAuthorityChanged?: () => void;
}) {
  const workspace = me.workspaces?.find((item) => item.id === me.tenant_id);
  const tenantID = me.tenant_id;
  const scopeRef = useRef(`${tenantID}:${me.user_id}`);
  scopeRef.current = `${tenantID}:${me.user_id}`;

  const isTeam = workspace?.kind === "team";
  const isOwner = me.role === "owner";
  const isAdmin = me.role === "admin";
  const canManageInvites = Boolean(isTeam && (isOwner || isAdmin));
  const [members, setMembers] = useState<WorkspaceMember[]>([]);
  const [invites, setInvites] = useState<WorkspaceInvite[]>([]);
  const [issuedInvite, setIssuedInvite] = useState<WorkspaceInvite | null>(null);
  const [email, setEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<"admin" | "member">("member");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");

  const memberByID = useMemo(
    () => new Map(members.map((member) => [member.user_id, member])),
    [members],
  );

  useEffect(() => {
    let current = true;
    setMembers([]);
    setInvites([]);
    setIssuedInvite(null);
    setEmail("");
    setInviteRole("member");
    setError("");
    setBusy("");
    setLoading(true);
    Promise.all([
      api.listWorkspaceMembers(tenantID),
      canManageInvites ? api.listWorkspaceInvites(tenantID) : Promise.resolve([]),
    ])
      .then(([nextMembers, nextInvites]) => {
        if (!current) return;
        setMembers(nextMembers);
        setInvites(nextInvites);
      })
      .catch((cause) => {
        if (!current) return;
        setError(cause instanceof Error ? cause.message : "加载成员失败");
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [tenantID, me.user_id, canManageInvites]);

  async function refresh() {
    const scope = scopeRef.current;
    setBusy("refresh");
    setError("");
    try {
      const [nextMembers, nextInvites] = await Promise.all([
        api.listWorkspaceMembers(tenantID),
        canManageInvites ? api.listWorkspaceInvites(tenantID) : Promise.resolve([]),
      ]);
      if (scope !== scopeRef.current) return;
      setMembers(nextMembers);
      setInvites(nextInvites);
    } catch (cause) {
      if (scope === scopeRef.current) {
        setError(cause instanceof Error ? cause.message : "刷新失败");
      }
    } finally {
      if (scope === scopeRef.current) setBusy("");
    }
  }

  async function issueInvite(event: FormEvent) {
    event.preventDefault();
    if (!canManageInvites || busy) return;
    const scope = scopeRef.current;
    setBusy("invite");
    setError("");
    try {
      const invite = await api.issueWorkspaceInvite(tenantID, email.trim(), inviteRole);
      if (scope !== scopeRef.current) return;
      setIssuedInvite(invite);
      setEmail("");
      const nextInvites = await api.listWorkspaceInvites(tenantID);
      if (scope !== scopeRef.current) return;
      setInvites(nextInvites);
      toast.success("邀请已签发；令牌只显示这一次");
    } catch (cause) {
      if (scope === scopeRef.current) {
        setError(cause instanceof Error ? cause.message : "签发邀请失败");
      }
    } finally {
      if (scope === scopeRef.current) setBusy("");
    }
  }

  async function revokeInvite(invite: WorkspaceInvite) {
    if (!canManageInvites || busy || inviteStatus(invite) !== "pending") return;
    if (!confirm(`撤销发送给 ${invite.email} 的邀请？`)) return;
    const scope = scopeRef.current;
    setBusy(`invite:${invite.id}`);
    setError("");
    try {
      await api.revokeWorkspaceInvite(tenantID, invite.id);
      const nextInvites = await api.listWorkspaceInvites(tenantID);
      if (scope !== scopeRef.current) return;
      setInvites(nextInvites);
      if (issuedInvite?.id === invite.id) setIssuedInvite(null);
    } catch (cause) {
      if (scope === scopeRef.current) {
        setError(cause instanceof Error ? cause.message : "撤销邀请失败");
      }
    } finally {
      if (scope === scopeRef.current) setBusy("");
    }
  }

  async function changeRole(member: WorkspaceMember, role: "admin" | "member") {
    if (!isOwner || busy || member.role === "owner" || member.user_id === me.user_id) return;
    const scope = scopeRef.current;
    setBusy(`member:${member.user_id}`);
    setError("");
    try {
      await api.updateWorkspaceMemberRole(tenantID, member.user_id, role);
      const nextMembers = await api.listWorkspaceMembers(tenantID);
      if (scope !== scopeRef.current) return;
      setMembers(nextMembers);
    } catch (cause) {
      if (scope === scopeRef.current) {
        setError(cause instanceof Error ? cause.message : "修改角色失败");
      }
    } finally {
      if (scope === scopeRef.current) setBusy("");
    }
  }

  async function removeMember(member: WorkspaceMember) {
    const allowed = isOwner || (isAdmin && member.role === "member");
    if (!allowed || busy || member.role === "owner" || member.user_id === me.user_id) return;
    const label = member.email || member.name || `#${member.user_id}`;
    if (!confirm(`从工作区移除 ${label}？该成员在此工作区的会话会立即失效。`)) return;
    const scope = scopeRef.current;
    setBusy(`member:${member.user_id}`);
    setError("");
    try {
      await api.removeWorkspaceMember(tenantID, member.user_id);
      const nextMembers = await api.listWorkspaceMembers(tenantID);
      if (scope !== scopeRef.current) return;
      setMembers(nextMembers);
    } catch (cause) {
      if (scope === scopeRef.current) {
        setError(cause instanceof Error ? cause.message : "移除成员失败");
      }
    } finally {
      if (scope === scopeRef.current) setBusy("");
    }
  }

  async function transferOwnership(member: WorkspaceMember) {
    if (!isOwner || !isTeam || busy || member.role === "owner" || member.user_id === me.user_id) return;
    const label = member.email || member.name || `#${member.user_id}`;
    if (!confirm(`将 ${workspace?.name ?? "当前工作区"} 的 Owner 转让给 ${label}？你将变为 Member，并需要重新登录。`)) return;
    const scope = scopeRef.current;
    setBusy(`transfer:${member.user_id}`);
    setError("");
    try {
      await api.transferWorkspaceOwnership(tenantID, member.user_id);
      if (scope !== scopeRef.current) return;
      // The backend revokes both ownership-related sessions. Never retain a
      // re-auth proof or old-workspace state across that authority change.
      onAuthorityChanged();
    } catch (cause) {
      if (scope === scopeRef.current) {
        setError(cause instanceof ApiError || cause instanceof Error ? cause.message : "转让失败");
        setBusy("");
      }
    }
  }

  async function copyToken() {
    if (!issuedInvite?.token) return;
    try {
      await navigator.clipboard.writeText(issuedInvite.token);
      toast.success("邀请令牌已复制");
    } catch {
      toast.error("复制失败，请手动复制令牌");
    }
  }

  if (!workspace) {
    return (
      <Alert>
        <AlertTitle>工作区信息尚未就绪</AlertTitle>
        <AlertDescription>刷新页面后重试；界面不会根据 tenant ID 猜测成员权限。</AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="space-y-6" data-workspace-scope={`${tenantID}:${me.user_id}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="flex items-center gap-2 text-lg font-semibold">
            <Users className="size-5" />成员与邀请
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {workspace.name} · {workspace.member_count}/{workspace.seat_limit} 席位 · {roleLabels[me.role]}
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void refresh()} disabled={Boolean(busy)}>
          <RefreshCw className={busy === "refresh" ? "animate-spin" : ""} />刷新
        </Button>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {!isTeam && (
        <Alert>
          <AlertDescription>个人空间固定只有本人；团队成员管理仅在团队空间开放。</AlertDescription>
        </Alert>
      )}

      {canManageInvites && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base"><MailPlus className="size-4" />邀请成员</CardTitle>
            <CardDescription>邀请绑定邮箱、仅可使用一次，7 天后自动过期，并占用一个待处理席位。</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <form className="flex flex-col gap-3 sm:flex-row sm:items-end" onSubmit={issueInvite}>
              <div className="min-w-0 flex-1 space-y-2">
                <Label htmlFor="workspace-invite-email">邮箱</Label>
                <Input id="workspace-invite-email" type="email" required value={email}
                  onChange={(event) => setEmail(event.target.value)} placeholder="member@example.com" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="workspace-invite-role">角色</Label>
                <select id="workspace-invite-role" value={inviteRole}
                  onChange={(event) => setInviteRole(event.target.value as "admin" | "member")}
                  className="flex h-9 rounded-lg border border-input bg-background px-3 text-sm"
                >
                  <option value="member">Member</option>
                  {isOwner && <option value="admin">Admin</option>}
                </select>
              </div>
              <Button type="submit" disabled={Boolean(busy) || !email.trim()}>
                {busy === "invite" && <Loader2 className="animate-spin" />}签发邀请
              </Button>
            </form>
            {issuedInvite?.token && (
              <Alert>
                <AlertTitle>一次性邀请令牌</AlertTitle>
                <AlertDescription>
                  <p>请通过受信渠道发送给 {issuedInvite.email}。关闭或切换工作区后不会再次显示。</p>
                  <div className="mt-2 flex items-center gap-2">
                    <code className="min-w-0 flex-1 overflow-hidden text-ellipsis rounded bg-muted px-2 py-1 font-mono text-xs">
                      {issuedInvite.token}
                    </code>
                    <Button type="button" size="sm" variant="outline" onClick={() => void copyToken()}>
                      <Copy />复制
                    </Button>
                  </div>
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">成员</CardTitle>
          <CardDescription>
            {me.role === "member" ? "你可以查看团队成员；管理操作仅对 Owner/Admin 显示。" : "角色变更和移除会立即撤销目标成员在此工作区的会话。"}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex h-24 items-center justify-center text-muted-foreground"><Loader2 className="mr-2 animate-spin" />加载成员</div>
          ) : (
            <Table>
              <TableHeader><TableRow><TableHead>成员</TableHead><TableHead>角色</TableHead><TableHead className="text-right">操作</TableHead></TableRow></TableHeader>
              <TableBody>
                {members.map((member) => {
                  const canRemove = member.user_id !== me.user_id && member.role !== "owner" &&
                    (isOwner || (isAdmin && member.role === "member"));
                  return (
                    <TableRow key={member.user_id}>
                      <TableCell>
                        <div className="font-medium">{member.name || member.email || `#${member.user_id}`}</div>
                        {member.name && member.email && <div className="text-xs text-muted-foreground">{member.email}</div>}
                      </TableCell>
                      <TableCell>
                        {isOwner && member.role !== "owner" && member.user_id !== me.user_id ? (
                          <select aria-label={`修改 ${member.email || member.user_id} 的角色`} value={member.role}
                            onChange={(event) => void changeRole(member, event.target.value as "admin" | "member")}
                            disabled={Boolean(busy)} className="h-8 rounded-lg border border-input bg-background px-2 text-sm">
                            <option value="member">Member</option><option value="admin">Admin</option>
                          </select>
                        ) : (
                          <Badge variant="outline">{roleIcon(member.role)}{roleLabels[member.role]}</Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex justify-end gap-2">
                          {isOwner && isTeam && member.role !== "owner" && member.user_id !== me.user_id && (
                            <Button size="sm" variant="outline" disabled={Boolean(busy)} onClick={() => void transferOwnership(member)}>
                              <Crown />转让 Owner
                            </Button>
                          )}
                          {canRemove && (
                            <Button size="sm" variant="destructive" disabled={Boolean(busy)} onClick={() => void removeMember(member)}>
                              <Trash2 />移除
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {canManageInvites && (
        <Card>
          <CardHeader><CardTitle className="text-base">邀请记录</CardTitle><CardDescription>历史邀请不会回显令牌；只有当前待处理邀请可以撤销。</CardDescription></CardHeader>
          <CardContent>
            <Table>
              <TableHeader><TableRow><TableHead>邮箱</TableHead><TableHead>角色</TableHead><TableHead>状态</TableHead><TableHead className="text-right">操作</TableHead></TableRow></TableHeader>
              <TableBody>
                {invites.map((invite) => {
                  const status = inviteStatus(invite);
                  return (
                    <TableRow key={invite.id}>
                      <TableCell>{invite.email}</TableCell><TableCell>{roleLabels[invite.role]}</TableCell>
                      <TableCell><Badge variant={status === "pending" ? "secondary" : "outline"}>{status}</Badge></TableCell>
                      <TableCell className="text-right">
                        {status === "pending" && <Button size="sm" variant="ghost" disabled={Boolean(busy)} onClick={() => void revokeInvite(invite)}>撤销</Button>}
                      </TableCell>
                    </TableRow>
                  );
                })}
                {invites.length === 0 && <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground">暂无邀请</TableCell></TableRow>}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {/* Keep exact current member discoverable for tests and assistive tech. */}
      <span className="sr-only">当前成员：{memberByID.get(me.user_id)?.email || `#${me.user_id}`}</span>
    </div>
  );
}
