// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a database v1 `#@ persist` table the served copy cannot carry whole.
 *
 * - A table holding a MAP, BLOB, INTERVAL, UUID or JSON column (types Malloy
 *   reads as `sql native` or `json`, which no stored type spells) serves every
 *   query that reads none of them from the table, a `select: *` over a source
 *   that drops them and an entry point under its own `#(access_filter)` on a
 *   carried column included; a query reading one, and a `select: *` that
 *   expands to one, answer live with the reason. Through 0.17.0 every query
 *   answered live (`served-copy-not-served`, `fields_not_carried`).
 * - A source the served copy leaves as written (here `fields_not_carried`: a
 *   persisted extension of a base with zoned dimensions) answers live, and
 *   its path report names its own table `NOT_SERVED` with the served copy's
 *   reason, as the package warning does. Through 0.17.0 the report considered
 *   nothing, so the live answer gave no reason at all.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

/** Rows with every value an exact string, in the order they came. */
function asStrings(rows: Record<string, unknown>[]): Record<string, string>[] {
   return rows.map((r) =>
      Object.fromEntries(
         Object.entries(r).map(([k, v]) => [
            k,
            v instanceof Date
               ? v.toISOString()
               : typeof v === "object" && v !== null
                 ? JSON.stringify(v, (_k, x) =>
                      typeof x === "bigint" ? `${x}n` : x,
                   )
                 : String(v),
         ]),
      ),
   );
}

describe("R4: a database v1 #@ persist source the served copy leaves as written names why in its path report", () => {
   let r4: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      r4 = await buildScenario({
         files: {
            "m.malloy": `##! experimental { persistence }
source: ev_ny is duckdb.sql("SELECT * FROM ev") extend {
  primary_key: id
  timezone: 'America/New_York'
  dimension: ts_hr is hour(ts), ts_day is ts.day
}

#@ persist name="r4lo_inh"
source: inh is ev_ny extend { measure: total is amt.sum() }
`,
         },
         seedSql: [
            `CREATE TABLE ev AS SELECT * FROM (VALUES
               (1, 10.5, TIMESTAMP '2024-03-15 01:45:10'),
               (2, 20.0, TIMESTAMP '2024-03-15 03:05:00')
             ) AS t(id, amt, ts)`,
         ],
         database: ["inh"],
      });
   });
   afterAll(disposeScenarios);

   it("is bound in the database tier, and left as written for its inherited fields", async () => {
      expect(r4.bindings.map((b) => `${b.sourceName}:${b.tier}`)).toEqual([
         "inh:database",
      ]);
      expect(await r4.servedCopyProblems()).toEqual([
         { sourceName: "inh", reason: "fields_not_carried" },
      ]);
   });

   it("answers live, equal to live, naming its own table NOT_SERVED with the reason", async () => {
      const malloy =
         "run: inh -> { group_by: h is hour(ts); aggregate: n is count(), total; order_by: h }";
      const run = await r4.run({ name: "inh", malloy });
      expect(run.live.length).toBe(2);
      expect(asStrings(run.served)).toEqual(asStrings(run.live));
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      r4.setBindings(r4.bindings);
      const { path } = await r4.runAsIs(malloy);
      expect(path?.servedBy?.kind).toBe("live");
      expect(
         (path?.considered ?? []).map((c) => ({
            tableId: c.tableId,
            outcome: c.outcome,
            reason: c.reason,
            detail: c.detail,
         })),
      ).toEqual([
         {
            tableId: r4.bindings[0].virtualHandle,
            outcome: "skipped",
            reason: "NOT_SERVED",
            detail: expect.stringContaining("fields_not_carried"),
         },
      ]);
   });
});

/** One DuckDB column type no stored type spells, per row id (1, 2, 3). */
const UNCARRIED: {
   column: string;
   label: string;
   value: (i: number) => string;
}[] = [
   { column: "m", label: "MAP", value: (i) => `MAP {'k${i}': ${i}}` },
   { column: "b", label: "BLOB", value: (i) => `'\\x0${i}\\x02'::BLOB` },
   { column: "iv", label: "INTERVAL", value: (i) => `INTERVAL ${i} DAY` },
   {
      column: "u",
      label: "UUID",
      value: (i) => `'00000000-0000-0000-0000-00000000000${i}'::UUID`,
   },
   { column: "j", label: "JSON", value: (i) => `'{"a": ${i}}'::JSON` },
];

const ROWS = [
   [1, "east", 10.5, 9007199254740993n],
   [2, "east", 20.25, 2n],
   [3, "west", 40.0, 3n],
];

const RAW = ROWS.map(
   ([id, g, amt, big]) =>
      `(${id}, '${g}', ${amt}, ${big}::BIGINT, TIMESTAMPTZ '2024-0${id}-02 03:04:05+00', ${UNCARRIED.map((u) => u.value(Number(id))).join(", ")})`,
).join(",\n    ");

const EXOTIC_MODEL = `##! experimental { persistence givens }
given: REGIONS :: string[]

source: raw is duckdb.sql("""
  SELECT * FROM (VALUES
    ${RAW}
  ) AS t(id, g, amt, big, ts, ${UNCARRIED.map((u) => u.column).join(", ")})
""")

#@ persist name="r4lo_exotic"
source: exotic is raw -> { select: * } extend {
  measure: total is amt.sum(), n is count()
}

#@ persist name="r4lo_dropped"
source: dropped is raw -> { select: * } extend {
  except: ${UNCARRIED.map((u) => u.column).join(", ")}
}

#(access_filter) g in $REGIONS
#@ persist name="r4lo_gated"
source: gated is raw -> { select: * } extend {
  measure: total is amt.sum()
}
`;

describe("R4: a database v1 #@ persist table holding MAP, BLOB, INTERVAL, UUID and JSON columns serves every query that reads none of them", () => {
   let r4: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      r4 = await buildScenario({
         files: { "m.malloy": EXOTIC_MODEL },
         seedSql: [],
         database: ["exotic", "dropped", "gated"],
      });
   });
   afterAll(disposeScenarios);

   /** Run live and served; the served rows must be live's, value for value. */
   async function both(malloy: string, givens?: Record<string, GivenValue>) {
      const run = await r4.run({ name: malloy, malloy, givens });
      // Every caller but the one the gate admits to nothing gets rows.
      const nobody = (givens?.REGIONS as string[] | undefined)?.length === 0;
      expect(run.live.length > 0).toBe(!nobody);
      expect(asStrings(run.served)).toEqual(asStrings(run.live));
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      r4.setBindings(r4.bindings);
      const { path } = await r4.runAsIs(malloy, givens);
      return { run, path };
   }

   it("binds each source in the database tier and leaves none as written", async () => {
      expect(
         r4.bindings.map((b) => `${b.sourceName}:${b.tier}`).sort(),
      ).toEqual(["dropped:database", "exotic:database", "gated:database"]);
      expect(await r4.servedCopyProblems()).toEqual([]);
   });

   for (const malloy of [
      "run: exotic -> { group_by: g; aggregate: total, n, big_max is big.max(); order_by: g }",
      "run: exotic -> { select: id, g, amt, big, ts; order_by: id }",
      "run: exotic -> { where: amt > 15; group_by: d is ts.day; aggregate: n; order_by: d }",
      "run: dropped -> { select: *; order_by: id }",
   ]) {
      it(`serves from its table, equal to live: ${malloy}`, async () => {
         const { run } = await both(malloy);
         expect(run.servedVia).toBe("served_copy");
      });
   }

   for (const u of UNCARRIED) {
      it(`answers a query reading the ${u.label} column live, equal, naming it`, async () => {
         const { run, path } = await both(
            `run: exotic -> { select: id, ${u.column}; order_by: id }`,
         );
         expect(run.servedVia).toBeUndefined();
         expect(path?.servedBy?.kind).toBe("live");
         const [skip] = path?.considered ?? [];
         expect(skip).toMatchObject({
            outcome: "skipped",
            reason: "MISSING_FIELD",
         });
         expect(skip?.detail).toContain(`column \`${u.column}\``);
      });
   }

   it("answers select: * live, equal, naming every column its result would lack", async () => {
      const { run, path } = await both(
         "run: exotic -> { select: *; order_by: id }",
      );
      expect(run.servedVia).toBeUndefined();
      const [skip] = path?.considered ?? [];
      expect(skip).toMatchObject({ outcome: "skipped", reason: "NOT_SERVED" });
      for (const u of UNCARRIED) {
         expect(skip?.detail).toContain(`column \`${u.column}\``);
      }
   });

   const CALLERS: Record<string, GivenValue>[] = [
      { REGIONS: ["east"] },
      { REGIONS: ["west"] },
      { REGIONS: [] },
      { REGIONS: ["east", "west"] },
      { REGIONS: ["east"] },
   ];

   it("serves an entry point under its own row-level gate on a carried column, each caller exactly their rows", async () => {
      for (const givens of CALLERS) {
         const { run } = await both(
            "run: gated -> { group_by: g; aggregate: total, c is count(); order_by: g }",
            givens,
         );
         if ((givens.REGIONS as string[]).length > 0) {
            expect(run.servedVia).toBe("served_copy");
            expect(run.served.map((r) => r.g)).toEqual(
               [...(givens.REGIONS as string[])].sort(),
            );
         }
      }
   });

   it("answers a gated query reading an uncarried column live, each caller exactly their rows", async () => {
      for (const givens of CALLERS) {
         const { run } = await both(
            "run: gated -> { select: id, g, u; order_by: id }",
            givens,
         );
         expect(run.servedVia).toBeUndefined();
      }
   });
});

describe("the same table as a package serves it: no served-copy-not-served warning, and the route the harness takes", () => {
   let restore: () => void = () => {};
   let pkg: LoadedFixture;
   const QUERIES: [string, "served_copy" | "live"][] = [
      [
         "run: exotic -> { group_by: g; aggregate: total, n, big_max is big.max(); order_by: g }",
         "served_copy",
      ],
      ["run: dropped -> { select: *; order_by: id }", "served_copy"],
      ["run: exotic -> { select: id, m, u, j; order_by: id }", "live"],
      ["run: exotic -> { select: *; order_by: id }", "live"],
   ];
   const live = new Map<string, Outcome>();

   function answered(o: Outcome | undefined) {
      if (!o || "error" in o) throw new Error(o ? o.error : "no answer");
      return { rows: o.answer.rows, via: o.answer.via ?? o.answer.servedBy };
   }

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      const loader = await loadFixture({ "m.malloy": EXOTIC_MODEL });
      for (const [malloy] of QUERIES) {
         live.set(malloy, await loader.run("m.malloy", malloy));
      }
      pkg = await loader.bind();
   }, 120_000);

   afterAll(async () => {
      await pkg?.dispose();
      restore();
      await freshLoadPool();
   });

   it("builds each source in the database tier, with no served-copy warning", () => {
      expect(pkg.buildErrors).toEqual({});
      expect(pkg.built.map((b) => `${b.name}:${b.tier}`).sort()).toEqual([
         "dropped:database",
         "exotic:database",
         "gated:database",
      ]);
      expect(
         pkg.warnings.filter((w) => w.code === "served-copy-not-served"),
      ).toEqual([]);
   });

   for (const [malloy, via] of QUERIES) {
      it(`answers by the ${via === "live" ? "live route" : "served copy"}, equal to live: ${malloy}`, async () => {
         const served = answered(await pkg.run("m.malloy", malloy));
         const before = answered(live.get(malloy));
         expect(served.via).toBe(via);
         expect(asStrings(served.rows)).toEqual(asStrings(before.rows));
      });
   }
});
