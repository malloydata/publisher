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
               "client/index": "./src/client/index.ts",
            },
            name: "@malloy-publisher/sdk",
            fileName: (format, entryName) =>
               entryName === "index"
                  ? `index.${format}.js`
                  : `client/index.${format}.js`,
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
            output: {
               preserveModules: true,
               preserveModulesRoot: "src",
               // Provide global variable names for externalized dependencies
               globals: {
                  react: "React",
                  "react-dom": "ReactDOM",
                  "react/jsx-runtime": "ReactJSXRuntime",
                  "@emotion/react": "EmotionReact",
                  "@emotion/styled": "EmotionStyled",
                  "@mui/material": "MaterialUI",
                  "@mui/icons-material": "MaterialUIIcons",
                  "@mui/system": "MaterialUISystem",
               },
            },
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
