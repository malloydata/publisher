// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import path from "path";
import { fileURLToPath } from "url";
import { mergeConfig } from "vite";
import appConfig from "../../../vite.config";

const here = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(here, "../../..");

// The Console's own Vite configuration, rooted at the harness page. The
// harness imports the app's source from outside its root, so the dev server is
// told it may read there; the API proxy is the one `bun run dev` uses.
export default mergeConfig(appConfig({ mode: "development" }), {
   root: here,
   cacheDir: path.resolve(appRoot, "node_modules/.vite-harness"),
   server: {
      port: Number(process.env.HARNESS_PORT ?? 5199),
      strictPort: true,
      fs: { allow: [path.resolve(appRoot, "../..")] },
   },
});
