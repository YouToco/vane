import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

const root = path.resolve(__dirname);

export default defineConfig({
  base: "./",
  publicDir: false,
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(root, "src"),
    },
  },
  build: {
    outDir: path.resolve(root, ".prototype-dist/p0a"),
    emptyOutDir: true,
    rollupOptions: {
      input: path.resolve(
        root,
        "_preview/p0a-7d7f47e8506f4e49aa8cb4bfdab78e42/index.html",
      ),
    },
  },
});
