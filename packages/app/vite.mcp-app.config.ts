// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The `show_report` MCP App panel, built apart from the Console: one classic
// script and one stylesheet at fixed names, `dist/mcp-app/report.js` and
// `dist/mcp-app/report.css`, which the server's panel resource names
// (packages/server/src/mcp/tools/report_app_tools.ts). A classic script, not a
// module: the panel lives on the host's origin and loads this one from the
// REST origin, and a cross-origin module script needs CORS headers a classic
// one does not. Run after the Console build, which empties `dist/`.

import react from "@vitejs/plugin-react";
import path from "path";
import { defineConfig } from "vite";
import { sdkResolve } from "./vite.config";

export default ({ mode }: { mode: string }) =>
   defineConfig({
      plugins: [react()],
      resolve: sdkResolve(mode),
      // The Console's public/ files are the Console's; the panel needs none.
      publicDir: false,
      define: {
         "process.env.NODE_ENV": JSON.stringify(mode),
         "process.env.NODE_DEBUG": "false",
         __DEV__: JSON.stringify(mode !== "production"),
         __PROD__: JSON.stringify(mode === "production"),
      },
      build: {
         outDir: "dist/mcp-app",
         emptyOutDir: true,
         minify: mode === "production",
         sourcemap: false,
         target: "es2020",
         chunkSizeWarningLimit: 8000,
         cssCodeSplit: false,
         lib: {
            entry: path.resolve(__dirname, "src/mcp-app/main.tsx"),
            name: "PublisherReportPanel",
            formats: ["iife"],
            fileName: () => "report.js",
            cssFileName: "report",
         },
         rollupOptions: {
            onwarn(warning, warn) {
               if (
                  warning.code === "MODULE_LEVEL_DIRECTIVE" ||
                  warning.code === "SOURCEMAP_ERROR"
               ) {
                  return;
               }
               warn(warning);
            },
            output: { inlineDynamicImports: true },
         },
      },
   });
