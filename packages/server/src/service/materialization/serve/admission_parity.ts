// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What read admission reports as it judges, for the query pipeline
 * (`query_serving.ts`) to count: each walk of the IR for a fact a compile
 * or bind records but did not record for this query, and each catalog read
 * or extra compile a collation probe made on a query's path.
 */

/** Where on a query's path a probe reads the warehouse or compiles. */
export type AdmissionReadSite = "source_probe" | "stored_probe";

/**
 * Where a query walks the IR for a fact a compile or bind records, because
 * the record did not cover it (an inline `extend`, a lane or a stored set
 * the record does not hold): the served copy's per-source declines (`served_copy_declines`),
 * the gates' and row scope's reads and flags (`scope`), the rollups a
 * query's source could read and what it declines by before its shape is
 * read (`rollup_candidates`), the queried source's chain for the path
 * report (`path_chain`).
 */
export type QueryWalkSite =
   | "served_copy_declines"
   | "scope"
   | "rollup_candidates"
   | "path_chain";

/**
 * What admission reports as it judges, for the shell to count and log: this
 * layer reads no telemetry and writes no log of its own, so the query
 * pipeline (`query_serving.ts`) installs it once.
 */
interface AdmissionObserver {
   catalogRead(site: AdmissionReadSite): void;
   probeCompile(site: AdmissionReadSite): void;
   queryWalk?(site: QueryWalkSite): void;
}

let observer: AdmissionObserver | undefined;

/** Every query-path walk reported in this process, by site, for specs. */
const walks = new Map<QueryWalkSite, number>();

/** Report one query-path walk for a fact a compile or bind records. */
export function reportQueryWalk(site: QueryWalkSite): void {
   walks.set(site, (walks.get(site) ?? 0) + 1);
   observer?.queryWalk?.(site);
}

/** The query-path walks reported so far, by site (a copy). */
export function queryWalks(): ReadonlyMap<QueryWalkSite, number> {
   return new Map(walks);
}

/** Forget the query-path walks reported so far. */
export function clearQueryWalks(): void {
   walks.clear();
}

/** Install what admission reports to; the last one installed wins. */
export function observeAdmission(o: AdmissionObserver): void {
   observer = o;
}

/** Report one catalog read a probe made on a query's path. */
export function reportCatalogRead(site: AdmissionReadSite): void {
   observer?.catalogRead(site);
}

/** Report one compile a probe made on a query's path, beyond the query's own. */
export function reportProbeCompile(site: AdmissionReadSite): void {
   observer?.probeCompile(site);
}
