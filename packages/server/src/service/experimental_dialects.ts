// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getDialect } from "@malloydata/malloy";

/**
 * Dialects Malloy marks experimental that this server bundles a connector for.
 * The compiler refuses such a connection unless the file opens with
 * `##! experimental.dialect.<name>` or the host accepts the dialect; shipping
 * the connector is that acceptance, so models need no pragma.
 */
const BUNDLED_EXPERIMENTAL_DIALECTS = ["sqlserver"] as const;

/**
 * Accept the bundled experimental dialects for this thread's compiler.
 * The dialect registry is per-thread module state, so each compiling worker
 * calls this itself as well as the main thread. Idempotent.
 */
export function acceptBundledExperimentalDialects(): void {
   for (const name of BUNDLED_EXPERIMENTAL_DIALECTS) {
      getDialect(name).acceptExperimental();
   }
}
