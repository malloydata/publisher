// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Coverage for the two `#(authorize)` gates that ship OUTSIDE `packages/server/src`
 * and are therefore invisible to every other suite in this directory:
 *
 *   1. `examples/governed-analytics/secured.malloy`, which ships in the default
 *      `examples` environment. A broken gate here is a broken product example,
 *      not a broken test.
 *   2. The fixture model in `packages/app/tests/playwright/notebook-authorize.spec.ts`,
 *      which only a live server plus a browser exercises. Its assertions are about
 *      the opaque-403 path, so they are worth pinning at this tier too rather than
 *      only in CI's Playwright job.
 *
 * Both authored a retired gate form and would have failed to load, which is why
 * this file exists: the suites that were green throughout that migration could
 * not see either one.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { Model } from "./model";

/** The tracked example package, not the gitignored `publisher_data/` copy. */
const GOVERNED_ANALYTICS = path.resolve(
   import.meta.dir,
   "../../../../examples/governed-analytics",
);

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

describe("examples/governed-analytics ships a working gate", () => {
   /** Distinct tenants visible to a caller, via the example's own `by_tenant`. */
   async function tenantsVisibleTo(
      model: Model,
      givens: Record<string, string>,
   ): Promise<number> {
      const result = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders_secured -> { aggregate: n is count() group_by: tenant }",
         {},
         true,
         givens,
      );
      return (result.compactResult as unknown as unknown[]).length;
   }

   it("loads clean, and admits per caller", async () => {
      // The connection's working directory is the package, so the example's own
      // relative `duckdb.table('orders.parquet')` resolves as it does at serve time.
      const duckdb = new DuckDBConnection(
         "duckdb",
         ":memory:",
         GOVERNED_ANALYTICS,
      );
      try {
         const model = await Model.create(
            "governed-analytics",
            GOVERNED_ANALYTICS,
            "secured.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(compilationErrorOf(model)).toBeUndefined();

         // Neither given carries a default (G4 forbids it for a gate-referenced
         // given), so every request sends both keys and the one off the caller's
         // path is sent blank -- exactly what the example's README documents.
         expect(
            await tenantsVisibleTo(model, { ROLE: "admin", TENANT: "" }),
         ).toBe(3);
         expect(
            await tenantsVisibleTo(model, { ROLE: "", TENANT: "acme" }),
         ).toBe(1);
         // A tenant that is off the allow-list is admitted nowhere: zero rows,
         // not a 403. This is the denial shape the example's header comment
         // promises, and the one a 403-keyed alert would never see.
         expect(
            await tenantsVisibleTo(model, { ROLE: "", TENANT: "nope" }),
         ).toBe(0);
      } finally {
         await duckdb.close();
      }
   });
});

/**
 * Byte-equivalent to `notebook-authorize.spec.ts`'s `MODEL_SOURCE`, minus the
 * `primary_key:` its notebook needs -- kept in sync deliberately: the Playwright
 * spec asserts 403-then-200 through a browser, and this asserts the same two
 * outcomes at the load/query tier where a failure is legible.
 */
const PLAYWRIGHT_FIXTURE = `##! experimental.givens

given: role :: string

#(authorize) $role = 'analyst'
source: gated_products is duckdb.table('products.parquet') extend {
  view: spotlight is {
    where: category = 'Jeans'
    select: product_id, name
    order_by: product_id
    limit: 1
  }
}
`;

describe("the Playwright notebook-authorize fixture model", () => {
   it("denies opaquely with no given, and serves with role = analyst", async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pw-authz-fixture-"));
      const duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
      try {
         await duckdb.runSQL(
            `COPY (SELECT 1 AS product_id, 'Cobalt Bootcut Jean' AS name, 'Jeans' AS category) ` +
               `TO '${path.join(dir, "products.parquet")}' (FORMAT PARQUET);`,
         );
         fs.writeFileSync(path.join(dir, "m.malloy"), PLAYWRIGHT_FIXTURE);
         const model = await Model.create(
            "pw",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(compilationErrorOf(model)).toBeUndefined();

         const run = (givens: Record<string, string>) =>
            model.getQueryResults(
               undefined,
               undefined,
               "run: gated_products -> spotlight",
               {},
               true,
               givens,
            );

         // `role` has no default, so an unsupplied given cannot resolve and the
         // gate cannot be attached: a 403 naming the SOURCE and never the
         // expression. The Playwright spec asserts exactly this pair of facts.
         const denied = await run({}).then(
            () => undefined,
            (e: Error) => e,
         );
         expect(denied).toBeInstanceOf(AccessDeniedError);
         expect(denied!.message).toContain("gated_products");
         expect(denied!.message).not.toContain("analyst");

         const allowed = await run({ role: "analyst" });
         expect((allowed.compactResult as unknown as unknown[]).length).toBe(1);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
