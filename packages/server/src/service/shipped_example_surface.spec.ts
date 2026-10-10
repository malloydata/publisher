// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `examples/governed-analytics` is the package that proves `explores` can be
 * retired: it was the only one in the repo declaring a curated surface, and it
 * declared a two-FILE one, which is the shape the `index.malloy` convention was
 * least obviously able to express.
 *
 * It can, and this pins the result, because converting it moved two things that
 * no unit fixture covers:
 *
 *   - Every query now enters through an `import`. Malloy marks imported entries
 *     `exported: false` and inlines their struct at the reference site, which is
 *     exactly the shape that once denied AUTHORIZED callers by grafting a gate
 *     onto an entry nothing consults (see authorize_import_hop.integration.spec.ts,
 *     whose header notes no other fixture put a gated source behind an import).
 *     The example now does, permanently.
 *   - The givens that drive the notebook's Parameters panel are declared in the
 *     two imported files, not in the entry model. They have to reach the entry
 *     model's own surface or the controls disappear, and a gate can only
 *     reference a given that is on it.
 *
 * Runs through `Package.create` rather than `Model.create`, because the query
 * boundary is a package-level policy and is the half `shipped_example_authorize`
 * cannot see.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";
import * as path from "path";
import { NotQueryableError } from "../errors";
import { Package } from "./package";

/** The tracked example package, not the gitignored `publisher_data/` copy. */
const GOVERNED_ANALYTICS = path.resolve(
   import.meta.dir,
   "../../../../examples/governed-analytics",
);

async function loadExample(): Promise<{
   pkg: Package;
   close: () => Promise<void>;
}> {
   const { MalloyConfig, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   // Rooted at the package dir so `duckdb.table('orders.parquet')` resolves.
   const duckdb = new DuckDBConnection(
      "duckdb",
      ":memory:",
      GOVERNED_ANALYTICS,
   );
   const connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   const pkg = await Package.create(
      "examples",
      "governed-analytics",
      GOVERNED_ANALYTICS,
      malloyConfig,
   );
   return { pkg, close: () => duckdb.close() };
}

describe("examples/governed-analytics is curated by its index.malloy", () => {
   it("lists only index.malloy, exporting both public sources", async () => {
      const { pkg, close } = await loadExample();
      try {
         expect((await pkg.listModels()).map((m) => m.path)).toEqual([
            "index.malloy",
         ]);
         const api = (await pkg.getModel("index.malloy")!.getModel()) as {
            sources?: { name?: string }[];
         };
         expect((api.sources ?? []).map((s) => s.name).sort()).toEqual([
            "orders_secured",
            "sales",
         ]);
      } finally {
         await close();
      }
   });

   it("carries every given from the imported files onto the entry model", async () => {
      const { pkg, close } = await loadExample();
      try {
         const api = (await pkg.getModel("index.malloy")!.getModel()) as {
            givens?: { name?: string }[];
         };
         // REGION and MIN_AMOUNT come from orders.malloy, TENANTS from
         // secured.malloy. index.malloy declares none of them. If this list
         // ever narrows, the notebook's Parameters panel loses controls and
         // the gate loses the given it reads -- so the conversion would have
         // to be reverted, not patched.
         expect((api.givens ?? []).map((g) => g.name).sort()).toEqual([
            "MIN_AMOUNT",
            "REGION",
            "TENANTS",
         ]);
      } finally {
         await close();
      }
   });

   it("answers a filter-given query through the index model", async () => {
      const { pkg, close } = await loadExample();
      try {
         const { result } = await pkg
            .getModel("index.malloy")!
            .getQueryResults(
               "sales",
               "by_region",
               undefined,
               undefined,
               false,
               {
                  REGION: "",
                  MIN_AMOUNT: 0,
               },
            );
         expect(result.data).toBeDefined();
      } finally {
         await close();
      }
   });

   it("still scopes the gated source to the caller's tenants, across the import hop", async () => {
      const { pkg, close } = await loadExample();
      try {
         const tenantsFor = async (TENANTS: string[]): Promise<string[]> => {
            const { compactResult } = await pkg
               .getModel("index.malloy")!
               .getQueryResults(
                  "orders_secured",
                  "by_tenant",
                  undefined,
                  undefined,
                  false,
                  { TENANTS },
               );
            return (compactResult as { tenant: string }[]).map((r) => r.tenant);
         };

         // The numbers track the caller rather than being uniformly filtered
         // or uniformly not -- the property the import-hop bug broke.
         expect(await tenantsFor(["acme"])).toEqual(["acme"]);
         expect(await tenantsFor(["globex"])).toEqual(["globex"]);
         expect((await tenantsFor(["acme", "globex"])).sort()).toEqual([
            "acme",
            "globex",
         ]);
      } finally {
         await close();
      }
   });

   it("refuses the internal base source, which nothing exports", async () => {
      const { pkg, close } = await loadExample();
      try {
         // Same outcome the retired `explores` + `queryableSources: "declared"`
         // pair produced, now from no manifest key at all.
         await expect(
            pkg
               .getModel("internal.malloy")!
               .getQueryResults(
                  undefined,
                  undefined,
                  "run: orders_base -> { aggregate: c is count() }",
               ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await close();
      }
   });
});
