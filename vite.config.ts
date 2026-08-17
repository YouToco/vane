import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
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
