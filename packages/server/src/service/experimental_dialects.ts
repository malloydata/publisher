// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getDialect } from "@malloydata/malloy";

/**
 * Dialects Malloy marks experimental that this server bundles a connector
 * for. The compiler refuses a model that names a connection on an
 * experimental dialect unless the file opens with
 * `##! experimental.dialect.<name>` or the host program accepts the dialect on
 * the author's behalf. Shipping the connector is that acceptance: a modeler
 * configures a connection of this type in Publisher, and the dialect's
 * upstream status is Publisher's to document, not a pragma for every model.
 *
 * SQL Server: the connector is read-only and the connector README lists what
 * the dialect does not translate yet.
 */
const BUNDLED_EXPERIMENTAL_DIALECTS = ["sqlserver"] as const;

/**
 * Accept the bundled experimental dialects for this thread's compiler.
 *
 * The dialect registry is module state, so a worker thread that compiles
 * (the package-load pool) has its own copy and calls this itself; the main
 * thread calls it at startup. Idempotent.
 */
export function acceptBundledExperimentalDialects(): void {
   for (const name of BUNDLED_EXPERIMENTAL_DIALECTS) {
      getDialect(name).acceptExperimental();
   }
}
