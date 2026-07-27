import { defineConfig } from "vite";
import type { Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

const root = path.resolve(__dirname);
const ownerPreviewHtml = path.resolve(
  root,
  "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html",
);

function normalizeModuleID(id: string): string {
  const normalized = id.replaceAll("\\", "/").split("?")[0];
  const normalizedRoot = root.replaceAll("\\", "/");
  if (normalized.startsWith(`${normalizedRoot}/`)) {
    return normalized.slice(normalizedRoot.length + 1);
  }
  const nodeModules = normalized.lastIndexOf("/node_modules/");
  if (nodeModules >= 0) return normalized.slice(nodeModules + 1);
  return normalized;
}

// Vite's public manifest captures chunk edges but intentionally omits the
// module membership of each chunk. Emit a companion audit manifest so the CI
// budget can prove that marketing-only source and packages stay out of the
// authenticated initial graph without controlling Rollup's chunk strategy.
function bundleAuditManifest(): Plugin {
  return {
    name: "vane-bundle-audit-manifest",
    apply: "build",
    generateBundle(_options, bundle) {
      const chunks = Object.values(bundle)
        .filter((item) => item.type === "chunk")
        .map((chunk) => ({
          file: chunk.fileName,
          modules: Object.keys(chunk.modules).map(normalizeModuleID).sort(),
        }))
        .sort((left, right) => left.file.localeCompare(right.file));
      this.emitFile({
        type: "asset",
        fileName: ".vite/bundle-modules.json",
        source: `${JSON.stringify({ version: 1, chunks }, null, 2)}\n`,
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), tailwindcss(), bundleAuditManifest()],
  resolve: {
    alias: {
      "@": path.resolve(root, "src"),
    },
  },
  build: {
    manifest: true,
    rollupOptions: {
      input: {
        app: path.resolve(root, "index.html"),
        ownerPreview: ownerPreviewHtml,
      },
    },
  },
  server: {
    proxy: {
      "/api": {
        target: "https://api.vane.zhuoqidev.com",
        changeOrigin: true,
        headers: {
          Origin: "https://vane.zhuoqidev.com",
        },
      },
    },
  },
});
