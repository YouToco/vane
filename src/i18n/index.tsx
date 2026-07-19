import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { zh } from "./zh";
import { zhHant } from "./zh-hant";
import { en } from "./en";
import { ja } from "./ja";
import { ko } from "./ko";
import { es } from "./es";
import { fr } from "./fr";
import { de } from "./de";

export type Dict = typeof zh;

/** 支持的语言清单：code 用于存储/切换，native 是切换器里展示的原生名，html 写进 <html lang>。 */
export const LOCALES = [
  { code: "zh", native: "简体中文", html: "zh-CN" },
  { code: "zh-Hant", native: "繁體中文", html: "zh-TW" },
  { code: "en", native: "English", html: "en" },
  { code: "ja", native: "日本語", html: "ja" },
  { code: "ko", native: "한국어", html: "ko" },
  { code: "es", native: "Español", html: "es" },
  { code: "fr", native: "Français", html: "fr" },
  { code: "de", native: "Deutsch", html: "de" },
] as const;

export type Locale = (typeof LOCALES)[number]["code"];

const DICTS: Record<Locale, Dict> = { zh, "zh-Hant": zhHant, en, ja, ko, es, fr, de };

const STORAGE_KEY = "vane.locale";

function isLocale(v: string | null): v is Locale {
  return v !== null && v in DICTS;
}

function detectLocale(): Locale {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (isLocale(saved)) return saved;
  } catch {
    // localStorage 不可用（隐私模式等）时静默走浏览器语言
  }
  const lang = (navigator.language ?? "en").toLowerCase();
  if (lang.startsWith("zh")) {
    // zh-TW / zh-HK / zh-MO / zh-Hant-* → 繁体；其余中文变体 → 简体
    const traditional = ["tw", "hk", "mo", "hant"].some((t) => lang.includes(t));
    return traditional ? "zh-Hant" : "zh";
  }
  const base = lang.split("-")[0] ?? "en";
  return isLocale(base) ? base : "en";
}

interface I18nValue {
  locale: Locale;
  setLocale: (l: Locale) => void;
  t: Dict;
}

const I18nContext = createContext<I18nValue | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(detectLocale);

  useEffect(() => {
    document.documentElement.lang =
      LOCALES.find((l) => l.code === locale)?.html ?? "en";
  }, [locale]);

  function setLocale(l: Locale) {
    setLocaleState(l);
    try {
      localStorage.setItem(STORAGE_KEY, l);
    } catch {
      // 持久化失败不影响本次会话切换
    }
  }

  return (
    <I18nContext.Provider value={{ locale, setLocale, t: DICTS[locale] }}>
      {children}
    </I18nContext.Provider>
  );
}

export function useI18n(): I18nValue {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error("useI18n must be used within I18nProvider");
  return ctx;
}
