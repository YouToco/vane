import { cn } from "@/shared/utils/class-names";

/**
 * 见微 Vane 品牌标：相风铜乌——汉代观象台顶的风向铜鸟，
 * 比西方风向鸡早一千年，是「vane」的中华本尊。
 * 铜身经千年氧化生出铜绿，唯喙上鎏金未褪——它还在指着风向。
 *
 * Logo     — 单色线标（currentColor），用于正文/单色场景。
 * LogoMark — 印章版（青铜黑底 + 铜绿身 + 鎏金喙），跨主题恒定，
 *            用于 sidebar / header / 登录页等品牌位。
 */

// 品牌常量色：印章跨 light/dark 恒定，不随主题变量走
const SEAL_BG = "oklch(0.20 0.025 210)"; // 青铜黑
const SEAL_BODY = "oklch(0.68 0.12 172)"; // 铜绿（氧化的铜身）
const SEAL_BEAK = "oklch(0.82 0.12 90)"; // 鎏金（未褪的喙）

function CrowGlyph({ body, beak, eye }: { body: string; beak: string; eye?: string }) {
  return (
    <g>
      {/* 铜身：立于杆顶、头朝右的乌 */}
      <path
        d="M4.2 9.9 C6.6 10.2 8.6 9.9 10.6 9.2 C12 8.7 13.3 8.3 14 7.7 C14.3 6.4 15.3 5.7 16.4 5.8 C17.5 5.9 18.3 6.6 18.4 7.6 L18.4 8.9 C18 10.2 17 11.1 15.5 11.7 C13.2 12.6 10.4 12.8 8.3 12.3 C6.8 12 5.4 11.5 4.2 10.9 Z"
        fill={body}
      />
      {/* 鎏金喙：指向风来的方向 */}
      <path d="M18.2 7.5 L21.4 8.5 L18.3 9.8 C18.1 9 18.1 8.2 18.2 7.5 Z" fill={beak} />
      {/* 乌目 */}
      {eye && <circle cx="16.6" cy="7.4" r="0.75" fill={eye} />}
      {/* 立杆与底座 */}
      <path
        d="M11.4 12.7 V19.9 M8 20.7 H14.8"
        stroke={body}
        strokeWidth="2"
        strokeLinecap="round"
      />
    </g>
  );
}

export function Logo({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" aria-hidden className={cn("size-5", className)}>
      <CrowGlyph body="currentColor" beak="currentColor" />
    </svg>
  );
}

export function LogoMark({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 32 32" aria-hidden className={cn("size-8", className)}>
      <rect x="0" y="0" width="32" height="32" rx="8" fill={SEAL_BG} />
      <svg x="4" y="4" width="24" height="24" viewBox="0 0 24 24">
        <CrowGlyph body={SEAL_BODY} beak={SEAL_BEAK} eye={SEAL_BG} />
      </svg>
    </svg>
  );
}
