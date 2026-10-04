// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

type Malloy = typeof import("@malloydata/malloy");

// A bundler's CommonJS interop (Vite's dep optimizer) can hand back a namespace holding only `default`.
export function unwrapMalloy(ns: Malloy): Malloy {
   if (ns.MalloyTranslator) return ns;
   const inner = (ns as unknown as { default?: Malloy }).default;
   return inner?.MalloyTranslator ? inner : ns;
}

/**
 * Imported dynamically, never statically: `builder-entry.ts` installs the
 * `process.env` shim the parser's dependencies read at module scope, and a
 * static import would be evaluated before that shim runs.
 */
export async function loadMalloy(): Promise<Malloy> {
   return unwrapMalloy(await import("@malloydata/malloy"));
}
