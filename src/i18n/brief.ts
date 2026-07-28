import type { Locale } from "@/i18n";

export interface BriefDict {
  briefTitle: string;
  briefPartial: string;
  briefInsightCount: string;
  briefUnknownSource: string;
  briefPublished: string;
  briefDiscovered: string;
  briefFeedback: string;
  briefFeedbackInterested: string;
  briefFeedbackNotInterested: string;
  briefFeedbackIssue: string;
  briefFeedbackDeepDive: string;
  briefsEmpty: string;
  briefsShown: string;
  legacyDiscoveries: string;
}

const BRIEF_DICTS: Record<Locale, BriefDict> = {
  zh: {
    briefTitle: "本次简报",
    briefPartial: "本次检查不完整",
    briefInsightCount: "{n} 条情报",
    briefUnknownSource: "来源未命名",
    briefPublished: "原文发布于",
    briefDiscovered: "发现于",
    briefFeedback: "当前反馈",
    briefFeedbackInterested: "感兴趣",
    briefFeedbackNotInterested: "不感兴趣",
    briefFeedbackIssue: "有问题",
    briefFeedbackDeepDive: "深入了解",
    briefsEmpty: "还没有非空简报。最近检查结果会单独显示，不会覆盖上一份有内容的简报。",
    briefsShown: "已显示 {shown} / {total} 份简报",
    legacyDiscoveries: "旧版逐条发现记录",
  },
  "zh-Hant": {
    briefTitle: "本次簡報",
    briefPartial: "本次檢查不完整",
    briefInsightCount: "{n} 條情報",
    briefUnknownSource: "來源未命名",
    briefPublished: "原文發佈於",
    briefDiscovered: "發現於",
    briefFeedback: "目前回饋",
    briefFeedbackInterested: "感興趣",
    briefFeedbackNotInterested: "不感興趣",
    briefFeedbackIssue: "有問題",
    briefFeedbackDeepDive: "深入瞭解",
    briefsEmpty: "還沒有非空簡報。最近檢查結果會單獨顯示，不會覆蓋上一份有內容的簡報。",
    briefsShown: "已顯示 {shown} / {total} 份簡報",
    legacyDiscoveries: "舊版逐條發現紀錄",
  },
  en: {
    briefTitle: "Brief",
    briefPartial: "Incomplete check",
    briefInsightCount: "{n} insights",
    briefUnknownSource: "Unnamed source",
    briefPublished: "Published",
    briefDiscovered: "Discovered",
    briefFeedback: "Current feedback",
    briefFeedbackInterested: "Interested",
    briefFeedbackNotInterested: "Not interested",
    briefFeedbackIssue: "Reported issue",
    briefFeedbackDeepDive: "Deep dive",
    briefsEmpty: "No non-empty Brief yet. The latest check is shown separately and never replaces the last useful Brief.",
    briefsShown: "Showing {shown} of {total} Briefs",
    legacyDiscoveries: "Legacy item-level discoveries",
  },
  ja: {
    briefTitle: "今回のブリーフ",
    briefPartial: "チェック未完了",
    briefInsightCount: "{n} 件",
    briefUnknownSource: "名称未設定の情報源",
    briefPublished: "公開",
    briefDiscovered: "発見",
    briefFeedback: "現在のフィードバック",
    briefFeedbackInterested: "興味あり",
    briefFeedbackNotInterested: "興味なし",
    briefFeedbackIssue: "問題を報告",
    briefFeedbackDeepDive: "詳しく見る",
    briefsEmpty: "内容のあるブリーフはまだありません。最新チェックは別に表示され、前回の有用なブリーフを上書きしません。",
    briefsShown: "{total} 件中 {shown} 件のブリーフを表示",
    legacyDiscoveries: "旧形式の個別発見記録",
  },
  ko: {
    briefTitle: "이번 브리프",
    briefPartial: "불완전한 확인",
    briefInsightCount: "인사이트 {n}개",
    briefUnknownSource: "이름 없는 출처",
    briefPublished: "게시",
    briefDiscovered: "발견",
    briefFeedback: "현재 피드백",
    briefFeedbackInterested: "관심 있음",
    briefFeedbackNotInterested: "관심 없음",
    briefFeedbackIssue: "문제 신고",
    briefFeedbackDeepDive: "자세히 보기",
    briefsEmpty: "내용이 있는 브리프가 아직 없습니다. 최신 확인은 별도로 표시되며 이전의 유용한 브리프를 덮어쓰지 않습니다.",
    briefsShown: "브리프 {total}개 중 {shown}개 표시",
    legacyDiscoveries: "기존 항목별 발견 기록",
  },
  es: {
    briefTitle: "Informe",
    briefPartial: "Comprobación incompleta",
    briefInsightCount: "{n} novedades",
    briefUnknownSource: "Fuente sin nombre",
    briefPublished: "Publicado",
    briefDiscovered: "Descubierto",
    briefFeedback: "Comentarios actuales",
    briefFeedbackInterested: "Me interesa",
    briefFeedbackNotInterested: "No me interesa",
    briefFeedbackIssue: "Problema reportado",
    briefFeedbackDeepDive: "Profundizar",
    briefsEmpty: "Aún no hay informes con contenido. La última comprobación se muestra aparte y no reemplaza el último informe útil.",
    briefsShown: "Mostrando {shown} de {total} informes",
    legacyDiscoveries: "Hallazgos individuales del formato anterior",
  },
  fr: {
    briefTitle: "Brief",
    briefPartial: "Vérification incomplète",
    briefInsightCount: "{n} informations",
    briefUnknownSource: "Source sans nom",
    briefPublished: "Publié",
    briefDiscovered: "Découvert",
    briefFeedback: "Retours actuels",
    briefFeedbackInterested: "Intéressant",
    briefFeedbackNotInterested: "Pas intéressant",
    briefFeedbackIssue: "Problème signalé",
    briefFeedbackDeepDive: "Approfondir",
    briefsEmpty: "Aucun Brief non vide pour le moment. La dernière vérification est affichée séparément et ne remplace pas le dernier Brief utile.",
    briefsShown: "{shown} Briefs affichés sur {total}",
    legacyDiscoveries: "Anciennes découvertes élément par élément",
  },
  de: {
    briefTitle: "Brief",
    briefPartial: "Prüfung unvollständig",
    briefInsightCount: "{n} Erkenntnisse",
    briefUnknownSource: "Unbenannte Quelle",
    briefPublished: "Veröffentlicht",
    briefDiscovered: "Entdeckt",
    briefFeedback: "Aktuelles Feedback",
    briefFeedbackInterested: "Interessant",
    briefFeedbackNotInterested: "Nicht interessant",
    briefFeedbackIssue: "Problem gemeldet",
    briefFeedbackDeepDive: "Vertiefen",
    briefsEmpty: "Noch kein nicht-leerer Brief. Die letzte Prüfung wird separat angezeigt und ersetzt den letzten nützlichen Brief nicht.",
    briefsShown: "{shown} von {total} Briefs angezeigt",
    legacyDiscoveries: "Ältere Einzelfund-Aufzeichnungen",
  },
};

export function briefDict(locale: Locale): BriefDict {
  return BRIEF_DICTS[locale];
}
