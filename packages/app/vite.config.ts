import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

// https://vitejs.dev/config/
export default ({ mode }) => {
   const isDev = mode === "development";
   // In Dev, Resolve the SDK locally as src, not /dist so it can hot reload
   const resolve = isDev
      ? {
           alias: {
              "@malloy-publisher/sdk": path.resolve(__dirname, "../sdk/src"),
           },
        }
      : undefined;
   return defineConfig({
      plugins: [react()],
      define: {
         "process.env": JSON.stringify(mode),
      },
      resolve,
   });
};
