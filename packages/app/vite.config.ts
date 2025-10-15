import react from "@vitejs/plugin-react";
import path from "path";
import { defineConfig } from "vite";

export default ({ mode }) => {
   const isDev = mode === "development";
   // In Dev, Resolve the SDK locally as src, not /dist so it can hot reload
   // But for CSS files, we need to point to dist since they're only generated during build
   const resolve = {
      alias: {
         // CSS files must come BEFORE the general SDK alias
         "@malloy-publisher/sdk/styles.css": path.resolve(
            __dirname,
            "../sdk/dist/styles.css",
         ),
         "@malloy-publisher/sdk/malloy-explorer.css": path.resolve(
            __dirname,
            "../sdk/dist/malloy-explorer.css",
         ),
         "@malloy-publisher/sdk/markdown-editor.css": path.resolve(
            __dirname,
            "../sdk/dist/markdown-editor.css",
         ),
         // Client subpath must come BEFORE the general SDK alias
         "@malloy-publisher/sdk/client": isDev
            ? path.resolve(__dirname, "../sdk/src/client-entry.ts")
            : path.resolve(__dirname, "../sdk/dist/client/index.es.js"),
         "@malloy-publisher/sdk": isDev
            ? // General SDK alias for everything else
              path.resolve(__dirname, "../sdk/src/index.ts")
            : // In production, use the built SDK to avoid duplicate dependencies
              path.resolve(__dirname, "../sdk/dist/index.es.js"),
      },
   };

   // Disable chunking entirely to avoid initialization order issues
   const manualChunks = undefined;

   return defineConfig({
      server: isDev
         ? {
              proxy: {
                 "/api/v0": {
                    target: "http://localhost:4000",
                    changeOrigin: true,
                 },
              },
           }
         : {},
      plugins: [react()],
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
         sourcemap: mode !== "production",
         emptyOutDir: true,
         chunkSizeWarningLimit: 1000,
         target: "esnext",
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
            output: {
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
               manualChunks,
            },
         },
      },
      resolve,
      // Optimize for faster builds
      optimizeDeps: {
         include: [
            "react",
            "react-dom",
            "@emotion/react",
            "@emotion/styled",
            "@mui/material",
            "@mui/icons-material",
            "@mui/system",
         ],
         exclude: [],
      },
   });
};
