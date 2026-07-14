import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // 本地开发直连线上 API（或改为 http://localhost:8080 连本地后端）
      "/api": {
        target: "https://api.vane.zhuoqidev.com",
        changeOrigin: true,
      },
    },
  },
});
