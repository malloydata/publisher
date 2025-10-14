import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import dts from "vite-plugin-dts";
import svgr from "vite-plugin-svgr";
import { peerDependencies } from "./package.json";

export default ({ mode }) => {
   return defineConfig({
      define: {
         // This is REQUIRED for React and other libraries to eliminate debug code
         "process.env.NODE_ENV": JSON.stringify(mode),
         "process.env.NODE_DEBUG": "false",
         // Custom defines for your own code (optional)
         __DEV__: JSON.stringify(mode !== "production"),
         __PROD__: JSON.stringify(mode === "production"),
      },
      build: {
         minify: mode === "production",
         lib: {
            entry: {
               index: "./src/index.ts",
               "client/index": "./src/client-entry.ts",
            },
            name: "@malloy-publisher/sdk",
            fileName: (format, entryName) => {
               if (entryName === "index") {
                  return `index.${format}.js`;
               } else if (entryName === "client/index") {
                  return `client/index.${format}.js`;
               }
               return `${entryName}.${format}.js`;
            },
            formats: ["cjs", "es"],
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
            // Externalize ALL React ecosystem and large dependencies
            external: [
               // React core
               "react",
               "react-dom",
               "react/jsx-runtime",
               "react-dom/client",

               // React ecosystem
               "@emotion/react",
               "@emotion/styled",

               // MUI (Material-UI) - these are huge
               "@mui/material",
               "@mui/icons-material",
               "@mui/system",
               "@mui/x-tree-view",

               // Other large React libraries
               "@react-spring/web",
               "@tanstack/react-query",
               "@uiw/react-md-editor",

               // Malloy dependencies (should be provided by host)
               "@malloydata/malloy-explorer",
               "@malloydata/malloy-interfaces",
               "@malloydata/malloy-query-builder",
               "@malloydata/render",

               // Utility libraries
               "axios",
               "markdown-to-jsx",

               // All peer dependencies
               ...Object.keys(peerDependencies),
            ],
            output: [
               // ES modules build - bundled to avoid resolution issues with external dependencies
               {
                  format: "es",
                  exports: "named",
                  entryFileNames: "[name].es.js",
                  chunkFileNames: "[name]-[hash].es.js",
                  // Don't preserve modules to avoid node_modules path issues
                  preserveModules: false,
               },
               // CommonJS build bundled (not preserved) to avoid resolution issues
               {
                  format: "cjs",
                  exports: "named",
                  entryFileNames: "[name].cjs.js",
                  chunkFileNames: "[name]-[hash].cjs.js",
                  // Don't preserve modules for CJS to avoid resolution issues
                  preserveModules: false,
               },
            ],
         },
         sourcemap: mode !== "production",
         emptyOutDir: true,
         chunkSizeWarningLimit: 1000,
         target: "esNext",
      },
      plugins: [
         dts({
            outDir: "dist",
            entryRoot: "src",
            include: ["src/**/*.ts", "src/**/*.tsx"],
            exclude: ["src/**/__test__/**", "src/**/__docs__/**"],
            staticImport: true,
            insertTypesEntry: true,
         }),
         svgr(),
         react(),
      ],
   });
};
