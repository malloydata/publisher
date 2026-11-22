// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database persisted view or named query over a database `#@ persist`
 * source is compiled a second time with that source's own `#@ persist`
 * kept, so its build reads the source's table. Its content address is
 * unchanged by the kept line (Malloy keys a target on its fully inlined
 * SQL), and its build SQL reads the upstream's table once the manifest
 * holds it. A view in a `storage=` destination is offered it too (its build
 * runs in the warehouse); one over a `storage=` source is not.
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

   it("is offered for a storage= view over a database table, whose build runs in the warehouse", async () => {
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
      ).toBeDefined();
   });

   it("re-roots a database #(materialize) source, and declines one read in another zone", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
#(materialize)
source: a is raw extend { dimension: cat2 is cat }

source: s is a extend {
  #@ persist name="os_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

source: z is a extend {
  timezone: 'America/New_York'
  #@ persist name="os_z"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      });
      const tableOf = (name: string) =>
         Object.values(c.tables).find((t) => t.source.name === name) as
            | {
                 overStoredBase?: {
                    baseBuildSource?: unknown;
                    rerootedSourceID?: string;
                 };
              }
            | undefined;
      const a = Object.values(c.tables).find((t) => t.source.name === "a")!;
      const s = tableOf("s__by_cat__view")!.overStoredBase!;
      expect(s.baseBuildSource).toBeDefined();
      expect(s.rerootedSourceID).toBe(a.source.sourceID);
      // Another zone would truncate the stored values again: it builds from
      // its definition.
      expect(tableOf("z__by_cat__view")).toBeDefined();
      expect(tableOf("z__by_cat__view")!.overStoredBase).toBeUndefined();
   });

   it("declines a #(materialize) pair table (an inlined join_many): it holds a row per pair, not the source's rows", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
source: picks is duckdb.sql("""select 'a' as cat, 1 as pick_id""") extend {
  primary_key: pick_id
}

#(materialize) inline=["p"]
source: a is raw extend {
  primary_key: cat
  join_many: p is picks on p.cat = cat
}

source: s is a extend {
  #@ persist name="os_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      });
      const a = Object.values(c.tables).find((t) => t.source.name === "a");
      expect(a?.kind).toBe("pair");
      const view = Object.values(c.tables).find(
         (t) => t.source.name === "s__by_cat__view",
      ) as { overStoredBase?: unknown } | undefined;
      // Built from its definition, as before, never over the pair table.
      expect(view).toBeDefined();
      expect(view!.overStoredBase).toBeUndefined();
   });

   it("decides each query alone: one declined does not stop another reading the table", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
#(materialize)
source: a is raw extend { dimension: cat2 is cat }

source: s is a extend {
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist name="os_q1"
query: q1 is s -> by_cat

#@ persist name="os_q2"
query: q2 is s -> { timezone: 'America/New_York'; group_by: cat; aggregate: total is amt.sum() }
`,
      });
      const over = (name: string) =>
         (
            Object.values(c.tables).find((t) => t.source.name === name) as
               | { overStoredBase?: unknown }
               | undefined
         )?.overStoredBase;
      expect(over("q1__query")).toBeDefined();
      // Another zone: declined alone, built from its definition.
      expect(
         Object.values(c.tables).some((t) => t.source.name === "q2__query"),
      ).toBe(true);
      expect(over("q2__query")).toBeUndefined();
   });

   it("decides each view alone: a storage= view first on the same source leaves a database view over the table", async () => {
      packages.settings({
         PUBLISHER_MATERIALIZE: "true",
         PERSIST_STORAGE_MODE: "on",
      });
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
#(materialize)
source: a is raw extend { dimension: cat2 is cat }

source: s is a extend {
  #@ persist storage="lake"
  view: a_first is { group_by: cat; aggregate: total is amt.sum() }
  #@ persist name="os_db"
  view: b_second is { group_by: cat; aggregate: n is count() }
}
`,
      });
      const db = Object.values(c.tables).find(
         (t) => t.source.name === "s__b_second__view",
      ) as { overStoredBase?: unknown } | undefined;
      expect(db?.overStoredBase).toBeDefined();
      expect(
         (c.annotationFindings ?? []).filter(
            (f) => f.refused?.reason === "generated_build_failed",
         ),
      ).toEqual([]);
   });
});
