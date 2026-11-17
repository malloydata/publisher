// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * L4: a composite source answered from the member live would choose, through
 * the real package path (plan, bind, router, `govern()`).
 *
 * A composite resolves a query to its first member that can answer it. Over
 * a `#(materialize)` base, the served copy redeclares a big-integer column as
 * a native read of the stored one. A first member whose `where:` reads that
 * column (a tenant filter under a lock, `compose(y extend { where: org =
 * $ORG }, y)`) was then passed over for the unfiltered member, so the served
 * answer dropped the filter and returned every tenant's rows. Every caller,
 * interleaved, now gets exactly live's outcome.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference } from "./harness";

const TRIPS =
   "d,cat,amt,dur,org\n" +
   "2017-01-02,a,1.5,10,1\n" +
   "2017-03-02,b,2.5,4000,2\n" +
   "2018-01-02,a,3.5,5000,1\n" +
   "2017-05-05,c,-1,7200,2\n" +
   "2017-06-01,b,4.0,3700,1\n" +
   "2017-07-01,a,0.5,9000,2\n";

const MODEL = `##! experimental { persistence givens composite_sources }
given: ORG :: number
given: ROLE :: string
source: trips is duckdb.table('data/trips.csv')
#(materialize)
source: y is trips extend { where: amt > 0 }
#(materialize)
source: long_y is y extend { where: dur > 3600 }

#(authorize) 'analyst' = $ROLE
source: comp_lock_org is compose(y extend { where: org = $ORG }, y)

source: comp_org is compose(y extend { where: org = $ORG }, y)
source: comp_long is compose(long_y, y)
source: comp_inline_long is compose(y extend { where: dur > 3600 }, y)

// The control: the first member filters on a string column.
#(authorize) 'analyst' = $ROLE
source: comp_lock_cat is compose(y extend { where: cat = 'a' }, y)
`;

const SOURCES = [
   "comp_lock_org",
   "comp_org",
   "comp_long",
   "comp_inline_long",
   "comp_lock_cat",
];

const QUERIES = SOURCES.flatMap((s) => [
   `run: ${s} -> { group_by: cat; aggregate: n is count(); order_by: cat }`,
   `run: ${s} -> { aggregate: n is count(), t is amt.sum() }`,
   `run: ${s} -> { group_by: org; aggregate: n is count(); order_by: org }`,
]);

const CALLERS: Record<string, Record<string, string | number>> = {
   "an analyst of org 1": { ROLE: "analyst", ORG: 1 },
   "an analyst of org 2": { ROLE: "analyst", ORG: 2 },
   "an analyst of no org's rows": { ROLE: "analyst", ORG: 99 },
   "a viewer of org 1": { ROLE: "viewer", ORG: 1 },
};
const ORDER = [...Object.keys(CALLERS), ...Object.keys(CALLERS).reverse()];

function show(o: Outcome): string {
   return JSON.stringify(o, (_k, v) =>
      typeof v === "bigint" ? `${v}n` : (v as unknown),
   );
}

function difference(live: Outcome, served: Outcome): string | undefined {
   if ("error" in live || "error" in served) {
      return "error" in live && "error" in served
         ? undefined
         : `live ${show(live)} against served ${show(served)}`;
   }
   return rowDifference(live.answer.rows, served.answer.rows);
}

describe("L4: a composite is answered from the member live chooses", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome[]>();
   const key = (q: string, caller: string) => `${q}\u0000${caller}`;

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({
         "m.malloy": MODEL,
         "data/trips.csv": TRIPS,
      });
      for (const q of QUERIES) {
         for (const caller of Object.keys(CALLERS)) {
            live.set(
               key(q, caller),
               await loader.run("m.malloy", q, CALLERS[caller]),
            );
         }
      }
      fixture = await loader.bind();
      for (const q of QUERIES) {
         for (const caller of ORDER) {
            const outcomes = served.get(key(q, caller)) ?? [];
            outcomes.push(await fixture.run("m.malloy", q, CALLERS[caller]));
            served.set(key(q, caller), outcomes);
         }
      }
   }, 600_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("builds the stored bases", () => {
      expect(fixture.buildErrors).toEqual({});
      const built = fixture.built.map((b) => b.name);
      expect(built).toContain("y");
      expect(built).toContain("long_y");
   });

   it("answers live's rows for a filtered member, so it is not vacuous", () => {
      const got = live.get(key(QUERIES[1], "an analyst of org 1"))!;
      expect("answer" in got).toBe(true);
      if ("answer" in got) expect(got.answer.rows).toEqual([{ n: 3, t: 9 }]);
   });

   it("still serves a composite whose member matches live's from the table", () => {
      const got = served.get(key(QUERIES[13], "an analyst of org 1"))![0];
      expect(QUERIES[13]).toContain("comp_lock_cat");
      expect("answer" in got).toBe(true);
      if ("answer" in got) expect(got.answer.via).toBe("served_copy");
   });

   for (const q of QUERIES) {
      it(`${q}: every caller gets live's answer`, () => {
         for (const caller of Object.keys(CALLERS)) {
            const want = live.get(key(q, caller))!;
            for (const got of served.get(key(q, caller)) ?? []) {
               const diff = difference(want, got);
               expect(
                  diff,
                  `${caller}: ${diff ?? ""}\n${show(got)}`,
               ).toBeUndefined();
            }
         }
      });
   }
});
