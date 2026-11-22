// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database persisted view or named query over a database `#@ persist`
 * source is compiled a second time with that source's own `#@ persist`
 * kept, so its build reads the source's table. Its content address is
 * unchanged by the kept line (Malloy keys a target on its fully inlined
 * SQL), and its build SQL reads the upstream's table once the manifest
 * holds it. A view in a `storage=` destination, or over a `storage=`
 * source, is not offered the compile.
 */

import { describe, expect, it } from "bun:test";
import { Manifest } from "@malloydata/malloy";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import {
   buildSQLOf,
   compilerBuildId,
   computeSourceEntityId,
} from "./addressing";

const RAW = `##! experimental { persistence }
source: raw is duckdb.sql("""
  select * from (values ('a', 1.5), ('b', 2.5)) as t(cat, amt)
""")
`;

const packages = planPackages();

describe("a persisted view or query compiled over its upstream's table", () => {
   it("keeps its planned address, and reads the upstream's table", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
#@ persist name="os_a"
source: a is raw -> { select: cat, amt }

source: s is a extend {
  #@ persist name="os_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist name="os_q"
query: q is a -> { group_by: cat; aggregate: total is amt.sum() }
`,
      });
      const a = Object.values(c.tables).find((t) => t.source.name === "a")!;
      const aAddress = computeSourceEntityId(a.source, c.connectionDigests);
      for (const name of ["s__by_cat__view", "q__query"]) {
         const table = Object.values(c.tables).find(
            (t) => t.source.name === name,
         )!;
         expect(table.kind).toBe("view");
         // Decided and recorded at plan time.
         const variant = (
            table as {
               overStoredBase?: {
                  source: Parameters<typeof computeSourceEntityId>[0];
               };
            }
         ).overStoredBase!;
         expect(variant).toBeDefined();
         // The address-unchanged pin: kept or blanked, the same address.
         expect(
            computeSourceEntityId(variant.source, c.connectionDigests),
         ).toBe(
            computeSourceEntityId(
               table.source,
               c.connectionDigests,
               table.target,
            ),
         );
         const manifest = new Manifest();
         manifest.update(aAddress, { tableName: "os_a_table" });
         manifest.update(compilerBuildId(a.source), {
            tableName: "os_a_table",
         });
         expect(buildSQLOf(variant.source, manifest.buildManifest)).toContain(
            "os_a_table",
         );
         // The planned (blanked) source never reads it.
         expect(
            buildSQLOf(table.source, manifest.buildManifest, table.target),
         ).not.toContain("os_a_table");
      }
   });

   it("is not offered for a storage= view", async () => {
      packages.settings({
         PUBLISHER_MATERIALIZE: "true",
         PERSIST_STORAGE_MODE: "on",
      });
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
#@ persist storage="lake"
source: a is raw -> { select: cat, amt }

source: s is a extend {
  #@ persist name="os_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist name="os_b"
source: b is raw -> { select: cat, amt }

source: t is b extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      });
      // Over a `storage=` source: refused at plan (downstream_refusals.spec).
      expect(
         Object.values(c.tables).some(
            (t) => t.source.name === "s__by_cat__view",
         ),
      ).toBe(false);
      const table = Object.values(c.tables).find(
         (t) => t.source.name === "t__by_cat__view",
      );
      expect(table).toBeDefined();
      expect(
         (table as { overStoredBase?: unknown }).overStoredBase,
      ).toBeUndefined();
   });
});
