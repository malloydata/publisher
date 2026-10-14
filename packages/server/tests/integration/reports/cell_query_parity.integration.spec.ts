// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Governance parity between running a linear `.malloy` report's tile through
 * the deprecated cell route (`GET …/notebooks/{path}/cells/{i}`, which reads
 * the report as its definitions, then its tiles) and through the governed
 * query endpoint every tile uses (`POST …/models/{path}/query`, the report's
 * path as the model and `run: <tile>` as `query`).
 *
 * For every query cell of every fixture linear report, under several sets of
 * givens, the two must refuse alike or answer with the same
 * result: the package surface (a hidden file, a source derived from one, a
 * raw `connection.sql`), `#(authorize)` on a source and on one derived from
 * it, a row-level `#(access_filter)` graft, givens a cell reads, and givens it
 * leaves unbound.
 *
 * One difference is expected, and both sides of it refuse: a row-level gate
 * whose given has no value. The cell route compiles the graft and fails on
 * the missing value (400, "Given … has no value"); the query route denies the
 * source (403, "Access denied"). Neither runs anything.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   fixture,
   STOREFRONT,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

/** The packages, and the givens each is run under besides none. */
const CASES: Record<string, { location: string; givens: object[] }> = {
   "report-cell-governance": {
      location: fixture("report-cell-governance"),
      givens: [
         { GROUPS: [1, 2] },
         { GROUPS: [] },
         { ROLES: ["admin"] },
         { ROLES: ["viewer"] },
         { ROLES: ["admin"], GROUPS: [4, 5], REGION: "EU" },
         { ROLES: ["viewer"], GROUPS: [1] },
      ],
   },
   "notebooks-malloyyo-surface": {
      location: fixture("notebooks-malloyyo-surface"),
      givens: [{ REGION: "US" }, { GROUPS: [1, 2] }, { ROLES: ["admin"] }],
   },
   "notebook-unbound-given": {
      location: fixture("notebook-unbound-given"),
      givens: [{ ORG: 1 }],
   },
   "notebooks-malloyyo": {
      location: fixture("notebooks-malloyyo"),
      givens: [],
   },
   "reports-anywhere": {
      location: fixture("reports-anywhere"),
      givens: [],
   },
   storefront: {
      location: STOREFRONT,
      givens: [{ CATEGORY: "Jeans" }],
   },
};

interface Cell {
   kind?: string;
   text: string;
}

type Outcome = { result: unknown } | { refused: number; message?: string };

/** What an answer gives the renderer, minus the ids minted per compile. */
const outcomeOf = (
   status: number,
   body: { result?: string; message?: string },
): Outcome =>
   status === 200 && body.result !== undefined
      ? {
           result: JSON.parse(
              body.result.replace(
                 /reference_id = \\"[0-9a-f-]+\\"/g,
                 "reference_id",
              ),
           ),
        }
      : { refused: status, message: body.message };

/** The one expected difference: an unbound row-level given, refused both ways. */
const unboundRowLevelGiven = (a: Outcome, b: Outcome) =>
   "refused" in a &&
   "refused" in b &&
   a.refused === 400 &&
   /has no value and no default/.test(a.message ?? "") &&
   b.refused === 403 &&
   /^Access denied for source/.test(b.message ?? "");

describe("A .malloy report's cell: cell route vs query route (E2E)", () => {
   let env: PackageEnv;
   const pkgUrl = (pkg: string, sub: string) => env.api(sub, pkg);

   beforeAll(async () => {
      env = await startPackageEnv(
         "cell-query-parity-env",
         Object.fromEntries(
            Object.entries(CASES).map(([name, { location }]) => [
               name,
               location,
            ]),
         ),
      );
   }, 300_000);

   afterAll(() => env?.stop());

   it("refuses and answers alike, cell by cell", async () => {
      const differences: unknown[] = [];
      const seen = { answered: 0, refused: 0, expectedDifference: 0 };
      for (const [pkg, { givens }] of Object.entries(CASES)) {
         const reports = (await (
            await fetch(pkgUrl(pkg, "/reports"))
         ).json()) as { path: string }[];
         for (const { path: reportPath } of reports) {
            if (!reportPath.endsWith(".malloy")) continue;
            // The deprecated route serves a linear report's cells; a grid one has none.
            const read = await fetch(pkgUrl(pkg, `/notebooks/${reportPath}`));
            if (read.status !== 200) continue;
            const notebook = (await read.json()) as { notebookCells?: Cell[] };
            const cells = notebook.notebookCells ?? [];
            for (let i = 0; i < cells.length; i++) {
               if (cells[i].kind !== "query") continue;
               for (const given of [{}, ...givens]) {
                  const viaCell = await fetch(
                     pkgUrl(
                        pkg,
                        `/notebooks/${reportPath}/cells/${i}?givens=${encodeURIComponent(JSON.stringify(given))}`,
                     ),
                  );
                  const viaQuery = await env.query(
                     reportPath,
                     { query: cells[i].text, givens: given },
                     pkg,
                  );
                  const a = outcomeOf(viaCell.status, await viaCell.json());
                  const b = outcomeOf(viaQuery.status, await viaQuery.json());
                  if (unboundRowLevelGiven(a, b)) {
                     seen.expectedDifference++;
                  } else if (JSON.stringify(a) !== JSON.stringify(b)) {
                     differences.push({
                        pkg,
                        reportPath,
                        cell: i,
                        given,
                        cellRoute: a,
                        queryRoute: b,
                     });
                  } else if ("refused" in a) {
                     seen.refused++;
                  } else {
                     seen.answered++;
                  }
               }
            }
         }
      }
      expect(differences).toEqual([]);
      // Both kinds of outcome were exercised, governance refusals included.
      expect(seen.answered).toBeGreaterThan(40);
      expect(seen.refused).toBeGreaterThan(40);
      expect(seen.expectedDifference).toBeGreaterThan(0);
   });
});
