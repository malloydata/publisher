import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      react: path.resolve(__dirname, "../../node_modules/react"),
      "react-dom": path.resolve(__dirname, "../../node_modules/react-dom"),
      "@emotion/react": path.resolve(__dirname, "../../node_modules/@emotion/react"),
      "@emotion/styled": path.resolve(__dirname, "../../node_modules/@emotion/styled"),
    },
  },
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
    process: { env: {} }, // or a more specific mock
  },
  server: {
    proxy: {
      "/api/v0": {
        target: "http://localhost:4000",
        changeOrigin: true,
        secure: false,
      },
    },
  },
});
