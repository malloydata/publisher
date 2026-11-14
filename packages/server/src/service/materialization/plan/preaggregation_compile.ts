// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The runtime a generated companion model (a rollup, `#(materialize)` or
 * persisted-view build source, a v1 build copy) compiles in: the author's
 * package, with the generated text served from an overlay at a URL inside
 * the package directory, so its `import` of the author's model resolves while
 * it never shows up in `getModelPaths()`, the API, or the package's own
 * compile.
 *
 * The v1 `#@ preaggregate` composite this module once compiled is retired
 * (the v1 cutover, 6g): a v1 rollup is the rollup leg's.
 */

import type { Runtime } from "@malloydata/malloy";

export type SynthesizedRuntimeFactory = (
   overlay: ReadonlyMap<string, string>,
) => Promise<{ runtime: Runtime; importBaseURL: URL }>;
