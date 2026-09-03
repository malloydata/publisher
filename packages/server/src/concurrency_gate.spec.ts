// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";

/**
 * Finding F-11: the per-pod query-admission gate is applied unevenly. The
 * `/query`, `/sqlQuery`, and `/sqlTemporaryTable` routes pass through
 * `queryConcurrency()` middleware, which caps concurrent warehouse work at
 * PUBLISHER_MAX_CONCURRENT_QUERIES so a flood cannot saturate the pod. The
 * `/compile` route and both `/sqlSource` routes reach the same controllers but
 * were registered with no `queryConcurrency()` in their middleware chain, so a
 * flood of compile or sqlSource requests bypassed the cap the sibling query
 * routes enforce.
 *
 * The desired contract: every route that drives compile or sqlSource work must
 * include `queryConcurrency()` in its middleware chain, the same admission gate
 * its query siblings carry. Asserted as a source-scan over the route
 * registrations (like the query-route wiring test): the pre-fix registrations
 * omit the middleware, so these are RED until it is added.
 *
 * Both surfaces are covered: the modern `${API_PREFIX}` routes in server.ts and
 * the legacy `/projects/...` twins in server-old.ts. The legacy surface reaches
 * the same controllers, so leaving it ungated would be an unbounded bypass of the
 * modern gate.
 */
const BACKTICK = String.fromCharCode(96);

const readServerSource = (file: string): string =>
   readFileSync(resolve(import.meta.dir, file), "utf8");

/**
 * Whether each route registration in `source` whose template literal ends with
 * the given path suffix carries `queryConcurrency()` in its middleware chain.
 *
 * Anchors on the raw source at `<suffix>` followed by the literal's closing
 * backtick, which matches only an actual route template (prose mentioning the
 * path is not immediately followed by a backtick). Then reads the short window
 * between the route literal and the handler function -- the middleware slot --
 * and strips comments WITHIN that window only. Comment stripping is deliberately
 * not applied to the whole file: route wildcards contain the literal character
 * pairs that open and close a block comment (`/environments/:x/packages/:y/*`),
 * so a file-wide strip would swallow every route between two such wildcards.
 */
const routeGatePresence = (source: string, pathSuffix: string): boolean[] => {
   const needle = pathSuffix + BACKTICK;
   const results: boolean[] = [];
   let from = 0;
   for (;;) {
      const literalEndSuffix = source.indexOf(needle, from);
      if (literalEndSuffix === -1) break;
      const windowStart = literalEndSuffix + needle.length;
      from = windowStart;
      const afterLiteral = source.slice(windowStart);
      // The handler function is the registration's last argument; it begins
      // `async (` or `(req`/`(_req`/`(_`. Everything before it is the
      // middleware chain.
      const handlerStart = afterLiteral.search(/async\b|\((?:_?req|_)\b/);
      const rawWindow =
         handlerStart === -1
            ? afterLiteral.slice(0, 300)
            : afterLiteral.slice(0, handlerStart);
      const window = rawWindow
         .replace(/\/\*[\s\S]*?\*\//g, "")
         .replace(/\/\/.*$/gm, "");
      results.push(window.includes("queryConcurrency()"));
   }
   return results;
};

describe("query-concurrency gate coverage (modern surface, server.ts)", () => {
   const source = readServerSource("server.ts");

   it("registers the compile route with queryConcurrency() in its chain", () => {
      const compileGates = routeGatePresence(source, "/compile");
      // Exactly one compile route on this surface; asserting the count keeps the
      // test honest if the route is renamed or removed rather than gated.
      expect(compileGates).toHaveLength(1);
      expect(compileGates.every((present) => present)).toBe(true);
   });

   it("registers every sqlSource route with queryConcurrency() in its chain", () => {
      const sqlSourceGates = routeGatePresence(source, "/sqlSource");
      // Two variants: environment-scoped and package-scoped. Both must carry the
      // gate.
      expect(sqlSourceGates).toHaveLength(2);
      expect(sqlSourceGates.every((present) => present)).toBe(true);
   });

   it("the query routes that establish the gate contract carry it (scan sanity check)", () => {
      // Guards the scan itself: if these ever read as ungated, the extractor has
      // drifted and a red compile/sqlSource result would be meaningless rather
      // than a real finding. `/query`, `/sqlQuery`, and `/sqlTemporaryTable` are
      // gated in the source today.
      expect(
         routeGatePresence(source, "/sqlTemporaryTable").every((p) => p),
      ).toBe(true);
      const queryGates = routeGatePresence(source, "/models/*?/query");
      expect(queryGates).toHaveLength(1);
      expect(queryGates.every((p) => p)).toBe(true);
   });
});

describe("query-concurrency gate coverage (legacy surface, server-old.ts)", () => {
   const source = readServerSource("server-old.ts");

   it("registers the legacy compile route with queryConcurrency() in its chain", () => {
      const compileGates = routeGatePresence(source, "/compile");
      expect(compileGates).toHaveLength(1);
      expect(compileGates.every((present) => present)).toBe(true);
   });

   it("registers every legacy sqlSource route with queryConcurrency() in its chain", () => {
      const sqlSourceGates = routeGatePresence(source, "/sqlSource");
      expect(sqlSourceGates).toHaveLength(2);
      expect(sqlSourceGates.every((present) => present)).toBe(true);
   });

   it("the legacy query routes that establish the gate contract carry it (scan sanity check)", () => {
      // The legacy queryData GET routes are gated today; if they ever read as
      // ungated the extractor has drifted.
      const queryDataGates = routeGatePresence(source, "/queryData");
      expect(queryDataGates.length).toBeGreaterThan(0);
      expect(queryDataGates.every((p) => p)).toBe(true);
   });
});
