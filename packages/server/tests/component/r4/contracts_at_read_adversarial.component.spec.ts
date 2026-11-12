// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R3, contracts at read, adversarially: for each kind of gate, two callers
 * with different givens each get exactly live's rows from the stored table,
 * and never the other caller's, in any order; and each guard that keeps a
 * routed answer behind its gate is shown to be load-bearing by breaking what
 * it guards and watching the query be denied rather than answered.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10.0, TIMESTAMP '2026-01-05 03:00:00'),
      (2, 'acme', 'west', 20.0, TIMESTAMP '2026-02-05 13:00:00'),
      (3, 'globex', 'east', 30.0, TIMESTAMP '2026-03-05 03:00:00'),
      (4, 'globex', 'west', 40.0, TIMESTAMP '2026-04-05 23:00:00'),
      (5, 'initech', 'east', 50.0, TIMESTAMP '2026-05-05 03:00:00')
    ) AS t(id, org_id, region, amount, ts)`,
];

const HEAD = `##! experimental.givens

given: GROUPS :: string[]
given: ROLE :: string
`;

/** A gated `orders`, the gate written over `field`, which `extend` defines. */
const gated = (gate: string, extend = "") => `${HEAD}
${gate}
#(materialize) storage="lake"
source: orders is duckdb.table('orders') extend {
  ${extend}
  measure: total is amount.sum(), n is count()
}

source: east_orders is orders extend { where: region = 'east' }
`;

const ORG_A = { GROUPS: ["acme"], ROLE: "admin" };
const ORG_B = { GROUPS: ["globex", "initech"], ROLE: "analyst" };

interface GateKind {
   name: string;
   model: string;
   /** The query each caller runs; it groups by `org_id` so rows name an org. */
   malloy: string;
   /** Whether a caller's rows must all be of the caller's orgs. */
   rowsAreTheCallersOrgs: boolean;
}

const BY_ORG = "run: orders -> { group_by: org_id; aggregate: total, n }";

const KINDS: GateKind[] = [
   {
      name: "an access filter on a stored column",
      model: gated("#(access_filter) org_id in $GROUPS"),
      malloy: BY_ORG,
      rowsAreTheCallersOrgs: true,
   },
   {
      name: "an access filter on a stored computed dimension",
      model: gated(
         "#(access_filter) org in $GROUPS",
         "dimension: org is lower(org_id)",
      ),
      malloy: BY_ORG,
      rowsAreTheCallersOrgs: true,
   },
   {
      name: "an access filter on a computed dimension that is not stored",
      model: gated(
         "#(access_filter) org in $GROUPS",
         "internal dimension: org is lower(org_id)",
      ),
      malloy: BY_ORG,
      rowsAreTheCallersOrgs: true,
   },
   {
      name: "an access filter on a stored truncation, re-applied over its stored column",
      model: gated(
         "#(access_filter) org_id in $GROUPS and order_month >= @2026-02",
         "dimension: order_month is ts.month",
      ),
      malloy: BY_ORG,
      rowsAreTheCallersOrgs: true,
   },
   {
      name: "an access filter inherited by an extension",
      model: gated("#(access_filter) org_id in $GROUPS"),
      malloy: "run: east_orders -> { group_by: org_id; aggregate: total, n }",
      rowsAreTheCallersOrgs: true,
   },
   {
      name: "an #(authorize) lock",
      model: gated("#(authorize) $ROLE = 'admin'"),
      malloy: BY_ORG,
      rowsAreTheCallersOrgs: false,
   },
];

const R3_ON = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
};

/** A caller's run, or the denial it got. */
async function attempt(
   fixture: R4Fixture,
   malloy: string,
   givens: Record<string, unknown>,
): Promise<
   | { denied: false; live: Row[]; served: Row[]; via?: string }
   | { denied: true }
> {
   try {
      const run = await fixture.run({
         name: malloy,
         malloy,
         givens: givens as Record<string, string[]>,
      });
      return {
         denied: false,
         live: run.live,
         served: run.served,
         via: run.servedVia,
      };
   } catch (error) {
      if (error instanceof AccessDeniedError) return { denied: true };
      throw error;
   }
}

describe("R3 adversarially: two callers per gate kind", () => {
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({ env: R3_ON }).restore;
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const kind of KINDS) {
      it(`${kind.name}: each caller gets live's rows from the stored table, never the other's`, async () => {
         const fixture = await buildScenario({
            files: { "m.malloy": kind.model },
            seedSql,
            materialize: true,
         });
         // Interleaved, then repeated: a graft or its cache shared across
         // callers would hand the second caller the first one's rows.
         const order = [ORG_A, ORG_B, ORG_A, ORG_B];
         const results = [];
         for (const givens of order) {
            results.push(await attempt(fixture, kind.malloy, givens));
         }
         for (const [i, result] of results.entries()) {
            if (result.denied) {
               // Only the lock denies, and only the caller it locks out.
               expect(kind.rowsAreTheCallersOrgs).toBe(false);
               expect(order[i]).toBe(ORG_B);
               continue;
            }
            expect(result.via).toBe("served_copy");
            expect(rowDifference(result.live, result.served)).toBeUndefined();
            if (kind.rowsAreTheCallersOrgs) {
               for (const row of result.served) {
                  expect(order[i].GROUPS).toContain(row.org_id as string);
               }
            }
         }
         // The same caller's answers agree however the calls interleave.
         expect(results[2]).toEqual(results[0]);
         expect(results[3]).toEqual(results[1]);
         if (kind.rowsAreTheCallersOrgs) {
            const orgs = (r: (typeof results)[number]) =>
               r.denied ? [] : r.served.map((row) => row.org_id);
            expect(orgs(results[0])).not.toEqual(orgs(results[1]));
         }
      });
   }
});

describe("R3 adversarially: each guard is load-bearing", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   const model = gated("#(access_filter) org_id in $GROUPS");
   beforeAll(async () => {
      restore = installSettings({ env: R3_ON }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   /** Run `body` with `object[method]` replaced, restoring it after. */
   async function patched<T extends object>(
      object: T,
      method: string,
      replace: (original: (...args: unknown[]) => unknown) => unknown,
      body: () => Promise<void>,
   ): Promise<void> {
      const target = object as Record<string, unknown>;
      const original = target[method] as (...args: unknown[]) => unknown;
      target[method] = replace(original.bind(object));
      try {
         await body();
      } finally {
         target[method] = original;
      }
   }

   it("baseline: the caller's rows, from the served copy", async () => {
      const run = await attempt(fixture, BY_ORG, ORG_A);
      expect(run.denied).toBe(false);
      if (run.denied) return;
      expect(run.via).toBe("served_copy");
      expect(run.served.map((r) => r.org_id)).toEqual(["acme"]);
   });

   it("denies a served-copy answer whose graft scope went missing, rather than grafting on the live model", async () => {
      const servedCopy = fixture.servedCopy();
      await patched(
         servedCopy,
         "loadQuery",
         (original) =>
            async (...args: unknown[]) => ({
               ...((await original(...args)) as object),
               graftScope: undefined,
            }),
         async () => {
            expect((await attempt(fixture, BY_ORG, ORG_A)).denied).toBe(true);
         },
      );
   });

   it("denies a served-copy answer on which governing finds no gate, rather than answering every row", async () => {
      // As if the served copy had lost the author's gate text: the router's
      // veto, read off the live compile, still says the entry point is gated.
      const governor = (fixture.model as unknown as { governor: object })
         .governor;
      await patched(
         governor,
         "probeEntryPointGates",
         () => async () => [],
         async () => {
            expect((await attempt(fixture, BY_ORG, ORG_A)).denied).toBe(true);
         },
      );
   });
});
