import type { Dict } from "./index";

export const fr: Dict = {
  brandName: "Vane",
  landing: {
    badge: "Veille personnelle par IA · Bêta sur invitation",
    login: "Se connecter",
    heroL1: "Une phrase suffit,",
    heroL2Pre: "bâtissez votre ",
    heroL2Brand: "système de veille",
    heroSub:
      "Dites à l'IA ce qu'il faut surveiller — elle veille pour vous, filtre le bruit et ne pousse que l'essentiel.",
    ctaStart: "Commencer",
    ctaHow: "Voir comment ça marche",
    demoPrompt: "Dites à Vane",
    examples: [
      {
        label: "Suivi de créateurs",
        text: "Surveille les posts RED de ce créateur et préviens-moi des nouveautés",
        result: "Tâche créée · 1 créateur · temps réel",
      },
      {
        label: "Brief quotidien",
        text: "Chaque matin, l'actualité du secteur IA, sans articles sponsorisés",
        result: "Tâche créée · 3 sources · chaque jour à 8 h 30",
      },
      {
        label: "Recherche ponctuelle",
        text: "Trouve-moi 5 blogueurs qui testent des outils de dev IA",
        result: "Tâche ponctuelle · résultats in-app + Lark",
      },
      {
        label: "Veille concurrentielle",
        text: "Alerte-moi si les concurrents A/B/C bougent, avec un récap hebdo",
        result: "Tâche créée · 3 cibles · récap hebdomadaire",
      },
    ],
    showcaseTitlePre: "Sur 100, seuls ",
    showcaseTitleBrand: "3",
    showcaseTitlePost: " vous parviennent",
    showcaseSub:
      "Vane surveille toutes vos sources et laisse le bruit à la porte — c'est toute sa raison d'être.",
    feedIn: "Entrant",
    feedOut: "Poussé vers vous",
    unit: "éléments",
    justNow: "à l'instant",
    filterName: "Filtre Vane",
    filterDesc: "Comprendre · Noter · Débruiter",
    sources: ["RSS", "RED", "X", "Web", "Podcast"],
    tags: ["Très pertinent", "Mise à jour clé", "À lire"],
    scenesTitle: "Ce que vous pouvez en faire",
    scenesSub: "Une phrase, c'est une mission de veille.",
    stepsTitle: "Comment ça marche",
    steps: [
      {
        title: "Dites une phrase",
        desc: "Confiez à Vane ce qu'il faut surveiller. Ni « sources » ni « planification » à connaître : il traduit pour vous.",
      },
      {
        title: "L'IA veille",
        desc: "Collecte, comprend et note en continu. Publicité et doublons restent à la porte.",
      },
      {
        title: "De plus en plus juste",
        desc: "Chaque 👍 et 👎 lui apprend vos goûts ; les envois évoluent avec vos retours.",
      },
    ],
    ctaCardTitle: "Confiez-lui la veille",
    ctaCardSub: "Sur invitation pour l'instant. Une fois invité, une phrase suffit pour démarrer.",
    footerLine1: "Vane · Voir l'infime, le voir en premier",
    footerLine2: "Veille personnelle par IA · Bêta sur invitation",
  },
};
