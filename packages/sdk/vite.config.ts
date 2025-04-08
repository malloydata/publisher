import { defineConfig } from "vite";
import dts from "vite-plugin-dts";
import { peerDependencies } from "./package.json";
import svgr from "vite-plugin-svgr";
import react from "@vitejs/plugin-react";

export default ({ mode }) => {
   return defineConfig({
      define: {
         "process.env": JSON.stringify(mode),
      },
      build: {
         minify: mode === "production",
         lib: {
            entry: "./src/index.ts", // Specifies the entry point for building the library.
            name: "@malloy-publisher/sdk", // Sets the name of the generated library.
            fileName: (format) => `index.${format}.js`, // Generates the output file name based on the format.
            formats: ["cjs", "es"], // Specifies the output formats (CommonJS and ES modules).
         },
         rollupOptions: {
            onwarn(warning, warn) {
               if (warning.code === "MODULE_LEVEL_DIRECTIVE") {
                  return;
               }
               if (warning.code === "SOURCEMAP_ERROR") {
                  return;
               }
               warn(warning);
            },
            external: [...Object.keys(peerDependencies)], // Defines external dependencies for Rollup bundling.
         },
         sourcemap: true, // Generates source maps for debugging.
         emptyOutDir: true, // Clears the output directory before building.
      },
      plugins: [dts(), svgr(), react()],
   });
};
