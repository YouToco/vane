import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

const root = path.resolve(__dirname);
const ownerPreviewHtml = path.resolve(
  root,
  "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html",
);

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(root, "src"),
    },
  },
  build: {
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
