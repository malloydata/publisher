// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

type Malloy = typeof import("@malloydata/malloy");
type MalloyTag = typeof import("@malloydata/malloy-tag");
type QueryBuilder = typeof import("@malloydata/malloy-query-builder");

// A bundler's CommonJS interop (Vite's dep optimizer) can hand back a namespace holding only `default`.
function unwrapNamespace<T extends object>(ns: T, probe: keyof T): T {
   if (ns[probe]) return ns;
   const inner = (ns as unknown as { default?: T }).default;
   return inner?.[probe] ? inner : ns;
}

export const unwrapMalloy = (ns: Malloy): Malloy =>
   unwrapNamespace(ns, "MalloyTranslator");

export const unwrapMalloyTag = (ns: MalloyTag): MalloyTag =>
   unwrapNamespace(ns, "parseAnnotation");

export const unwrapQueryBuilder = (ns: QueryBuilder): QueryBuilder =>
   unwrapNamespace(ns, "ASTQuery");

/**
 * Imported dynamically, never statically: `builder-entry.ts` installs the
 * `process.env` shim the parser's dependencies read at module scope, and a
 * static import would be evaluated before that shim runs.
 */
export async function loadMalloy(): Promise<Malloy> {
   return unwrapMalloy(await import("@malloydata/malloy"));
}

export async function loadMalloyTag(): Promise<MalloyTag> {
   return unwrapMalloyTag(await import("@malloydata/malloy-tag"));
}

/**
 * The Explorer's query builder, for `SourcesExplorer` alone: ESLint lets only
 * the Explorer's callers import this (`EXPLORER_CALLERS`).
 */
export async function loadQueryBuilder(): Promise<QueryBuilder> {
   return unwrapQueryBuilder(
      // eslint-disable-next-line no-restricted-syntax -- the query builder's one loader, for the Explorer; every other rule still holds in this file
      await import("@malloydata/malloy-query-builder"),
   );
}
