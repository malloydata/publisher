// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a top-level `query:`, through the real package path (load,
 * build, bind, query). With PUBLISHER_MATERIALIZE on, a persisted query's
 * table answers exactly the shapes that call it (REST and MCP `queryName`, a
 * report naming it, `run: q`, `run: q -> { … }`), equal to live: its rows, and
 * each column's name, kind and annotations, apart from Malloy's per-compile
 * drill expressions. A refinement and any use of the query inside another
 * definition are answered live, equal to live. A persisted query over a gated
 * source is never built, and the leg is never tried past its gate (CONTRACT),
 * whatever PUBLISHER_MATERIALIZE_GATED_SOURCES says. The route counter counts
 * the leg as `view`. With PUBLISHER_MATERIALIZE off, a persisted query is
 * warned and answered live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { startMetricsHarness } from "../../../src/test_helpers/metrics_harness";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";
import {
   type Answer,
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "./conformance/package_harness";
import {
   typeWithoutDrillExpression,
   withoutDrillExpression,
} from "./conformance/drill_expression";
import { rowDifference } from "./harness";

const TABLE = `duckdb.sql("""
  select * from (values
    (1, 'aa', 't1', 10.5, timestamp '2026-01-01 10:00:00'),
    (2, 'aa', 't2', 20.0, timestamp '2026-02-02 11:00:00'),
    (3, 'bb', 't1', 5.5, timestamp '2026-01-02 12:00:00'),
    (4, 'bb', 't1', 11.0, timestamp '2026-03-03 13:00:00'),
    (5, 'cc', 't2', 2.0, timestamp '2026-03-04 14:00:00'),
    (6, 'aa', 't1', 7.0, timestamp '2026-03-05 15:00:00')
  ) as t(id, carrier, tenant, d, ts)
""")`;

/** The same rows under other SQL, so the gated source shares no struct with the others. */
const GATED_TABLE = TABLE.replace(
   "as t(id, carrier, tenant, d, ts)",
   "as g(id, carrier, tenant, d, ts)",
);

const MODEL = `##! experimental { givens }
given: TENANT :: string

source: fl is ${TABLE} extend {
  # currency
  #(doc) "Distance flown"
  measure: dist is d.sum()
  measure: n is count()
}

# bar_chart
#@ persist storage="${LAKE}"
query: by_carrier is fl -> { group_by: carrier; aggregate: n, dist; order_by: dist desc }

#@ persist
query: by_carrier_colocated is fl -> { group_by: carrier; aggregate: n, dist; order_by: dist desc, carrier }

#@ persist
query: top is fl -> { group_by: carrier; aggregate: n; order_by: n desc, carrier; limit: 2 }

#@ persist
query: by_month is fl -> { group_by: m is ts.month; aggregate: n; order_by: m }

#@ persist storage="${LAKE}"
query: by_month_stored is fl -> { group_by: m is ts.month; aggregate: n, dist; order_by: m }

#@ persist
query: listing is fl -> { select: id, carrier, tenant; order_by: id }

#@ persist
query: flagged is fl -> { group_by: carrier; aggregate: busy is n > 1; order_by: carrier }

#@ persist
query: two_stage is fl -> { group_by: carrier, tenant; aggregate: n } -> { group_by: carrier; aggregate: tenants is count(); order_by: carrier }

#@ persist
query: nested is fl -> {
  group_by: carrier
  aggregate: dist
  nest: by_tenant is { group_by: tenant; aggregate: n; order_by: tenant }
  order_by: carrier
}

#@ persist
query: select_nest is fl -> { group_by: carrier; nest: rows is { select: tenant } }

#@ persist
query: with_dupes is fl -> { select: carrier, tenant; order_by: carrier, tenant }

#@ persist
query: having_only is fl -> { aggregate: n; having: n > 100 }

#@ persist
query: totals is fl -> { aggregate: n, dist }

#@ persist
query: null_flag is fl -> { group_by: carrier; aggregate: big is d.sum() { where: d > 1000 } > 1; order_by: carrier }

query: over_by_carrier is by_carrier -> { aggregate: total is dist.sum() }

#(access_filter) tenant = $TENANT
source: ops is ${GATED_TABLE} extend { measure: n is count() }

#@ persist
query: ops_by_tenant is ops -> { group_by: tenant, carrier; aggregate: n; order_by: tenant, carrier }

#@ persist
query: ops_by_carrier is ops -> { group_by: carrier; aggregate: n }
`;

/** A grid report over the model: a tile naming a query, and a single-query report's name. */
const REPORT = `## artifact layout=grid tiles=["by_carrier"]
import "m.malloy"
`;

/** A query declared in another file than its (ungated) source, and one over a gated source. */
const OTHER_FILE = `import "m.malloy"

#@ persist
query: elsewhere is fl -> { group_by: tenant; aggregate: n; order_by: tenant }

#@ persist
query: elsewhere_gated is ops -> { group_by: tenant; aggregate: n }
`;

/**
 * A query over an inline run target, in a model with no gate (a governed
 * model refuses a run target with no name, live or served).
 */
const INLINE = `#@ persist
query: inline_target is ${TABLE} -> { group_by: tenant; aggregate: c is count(); order_by: tenant }
`;

const FILES = {
   "inline.malloy": INLINE,
   "m.malloy": MODEL,
   "report.malloy": REPORT,
   "other.malloy": OTHER_FILE,
};

/** A number's integer/decimal subtype, which a `storage=` read loses (a separate tolerance, as for views). */
function stripSubtype(t: unknown): unknown {
   if (Array.isArray(t)) return t.map(stripSubtype);
   if (t && typeof t === "object") {
      return Object.fromEntries(
         Object.entries(t as Record<string, unknown>)
            .filter(([k]) => k !== "subtype")
            .map(([k, v]) => [k, stripSubtype(v)]),
      );
   }
   return t;
}

/**
 * Every column's name, kind (a measure stays a measure), type (a nest's
 * fields included) and annotations, with only Malloy's drill expressions set
 * aside, as the persisted-view metadata spec compares them.
 */
function metadata(answer: Answer): unknown {
   return answer.fields.map((f) => ({
      name: f.name,
      kind: f.kind,
      type: typeWithoutDrillExpression(stripSubtype(f.type)),
      annotations: f.annotations.map(withoutDrillExpression),
   }));
}

function answerOf(outcome: Outcome): Answer {
   if ("error" in outcome) throw new Error(outcome.error);
   return outcome.answer;
}

describe("R4: a persisted named query answers its exact call, equal to live", () => {
   let restore: () => void;
   let loaded: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live = new Map<string, Answer>();

   /** Shapes queried live before the bind, served after it, by model. */
   const served: [label: string, model: string, malloy: string][] = [
      ["an exact call, storage=", "m.malloy", "run: by_carrier"],
      ["a backticked call", "m.malloy", "run: `by_carrier`"],
      ["an exact call, colocated", "m.malloy", "run: by_carrier_colocated"],
      ["a limited query", "m.malloy", "run: top"],
      ["a timeframe column, colocated", "m.malloy", "run: by_month"],
      ["a timeframe column, storage=", "m.malloy", "run: by_month_stored"],
      ["a query that selects", "m.malloy", "run: listing"],
      ["a query that selects duplicate rows", "m.malloy", "run: with_dupes"],
      ["an inline run target", "inline.malloy", "run: inline_target"],
      ["a two-stage query", "m.malloy", "run: two_stage"],
      ["a boolean measure", "m.malloy", "run: flagged"],
      ["a NULL boolean measure", "m.malloy", "run: null_flag"],
      [
         "a query that goes on past the query",
         "m.malloy",
         "run: by_carrier -> { aggregate: total is dist.sum() }",
      ],
      [
         "a query that goes on past it, colocated",
         "m.malloy",
         "run: top -> { select: carrier; where: n > 1; order_by: carrier }",
      ],
      ["a report tile naming it", "report.malloy", "run: by_carrier"],
      [
         "a query over an ungated source in another file",
         "other.malloy",
         "run: elsewhere",
      ],
   ];

   /** Shapes answered live, before and after the bind; the second element is the leg's skip reason, if it applies. */
   const answeredLive: [
      label: string,
      model: string,
      malloy: string,
      skip: string | undefined,
      givens?: Record<string, string>,
   ][] = [
      [
         "a refinement",
         "m.malloy",
         "run: by_carrier + { limit: 1 }",
         "MISSING_FIELD",
      ],
      [
         "a refinement that filters a limited query",
         "m.malloy",
         "run: top + { where: carrier = 'cc' }",
         "MISSING_FIELD",
      ],
      [
         "a use inside another definition of the model",
         "m.malloy",
         "run: over_by_carrier",
         undefined,
      ],
      [
         "a source over the query",
         "m.malloy",
         "source: s is by_carrier\nrun: s -> { select: carrier }",
         undefined,
      ],
      [
         "a query over the query in the caller's text",
         "m.malloy",
         "query: q2 is top -> { select: carrier }\nrun: q2",
         undefined,
      ],
      [
         "a nest of the query's source",
         "m.malloy",
         "run: fl -> { group_by: carrier; aggregate: n, dist }",
         undefined,
      ],
      // Shapes a regroup does not reproduce row for row (the view leg's
      // rule): refused at plan (a nest), or declined by the variant
      // (MISSING_FIELD): no dimension to regroup by.
      ["a nested result (refused)", "m.malloy", "run: nested", undefined],
      ["a select: nest (refused)", "m.malloy", "run: select_nest", undefined],
      [
         "an aggregate-only query whose having: keeps nothing",
         "m.malloy",
         "run: having_only",
         "MISSING_FIELD",
      ],
      ["an aggregate-only query", "m.malloy", "run: totals", "MISSING_FIELD"],
      [
         "a query over a gated source in another file (refused)",
         "other.malloy",
         "run: elsewhere_gated",
         undefined,
         { TENANT: "t1" },
      ],
   ];

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loaded = await loadFixture(FILES);
      for (const [, model, malloy] of served) {
         live.set(
            `${model}\u0000${malloy}`,
            answerOf(await loaded.run(model, malloy)),
         );
      }
      for (const [, model, malloy, , givens] of answeredLive) {
         live.set(
            `${model}\u0000${malloy}`,
            answerOf(await loaded.run(model, malloy, givens)),
         );
      }
      fixture = await loaded.bind();
   });
   afterAll(async () => {
      await loaded?.dispose();
      restore();
   });

   it("builds every persisted query the planner admits, and warns about the rest", () => {
      expect(fixture.buildErrors).toEqual({});
      expect(fixture.built.map((b) => `${b.name}:${b.tier}`).sort()).toEqual(
         [
            "by_carrier__query:storage",
            "by_carrier_colocated__query:colocated",
            "by_month__query:colocated",
            "by_month_stored__query:storage",
            "elsewhere__query:colocated",
            "flagged__query:colocated",
            "inline_target__query:colocated",
            "listing__query:colocated",
            "with_dupes__query:colocated",
            "having_only__query:colocated",
            "totals__query:colocated",
            "null_flag__query:colocated",
            "top__query:colocated",
            "two_stage__query:colocated",
         ].sort(),
      );
      const notBuilt = fixture.warnings
         .filter((w) => w.code === "persist-query-not-built")
         .map((w) => w.subject)
         .sort();
      expect(notBuilt).toEqual([
         "elsewhere_gated",
         "nested",
         "ops_by_carrier",
         "ops_by_tenant",
         "select_nest",
      ]);
      // Never the source warning a persisted query once raised.
      expect(
         fixture.warnings.some((w) => w.code === "persist-not-materializable"),
      ).toBe(false);
   });

   it("gives each built query a serve verdict as a persisted query", () => {
      expect(fixture.verdicts).toContain("by_carrier__query:serves");
      expect(fixture.verdicts).toContain("inline_target__query:serves");
      const verdict = fixture.pkg
         .getServeVerdicts()
         .find((v) => v.sourceName === "by_carrier__query");
      expect(verdict?.message).toContain("The persisted query `by_carrier`");
   });

   for (const [label, model, malloy] of served) {
      it(`serves ${label} from the query's table, equal to live`, async () => {
         const before = live.get(`${model}\u0000${malloy}`)!;
         const after = answerOf(await fixture.run(model, malloy));
         expect(before.servedBy).toBe("live");
         expect(after.servedBy).toBe("query");
         expect(rowDifference(before.rows, after.rows)).toBeUndefined();
         // Ordered as live orders it, column for column.
         expect(after.rows).toEqual(before.rows);
         expect(Object.keys(after.rows[0] ?? {})).toEqual(
            Object.keys(before.rows[0] ?? {}),
         );
         expect(metadata(after)).toEqual(metadata(before));
      });
   }

   it("keeps measures measures: live's #(malloy) calculation on each aggregate", async () => {
      const after = answerOf(await fixture.run("m.malloy", "run: by_carrier"));
      const calc = (name: string) =>
         after.fields
            .find((f) => f.name === name)
            ?.annotations.some((a) => a.includes("calculation"));
      expect(calc("n")).toBe(true);
      expect(calc("dist")).toBe(true);
      expect(calc("carrier")).toBe(false);
      expect(
         after.fields.find((f) => f.name === "dist")?.annotations,
      ).toContain("# currency");
   });

   it("keeps the query's own render tags on the result", async () => {
      const model = fixture.pkg.getModel("m.malloy")!;
      const run = (malloy: string) =>
         model.runQuery(
            unrecordedAudit(),
            { query: malloy } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      const after = await run("run: by_carrier");
      expect(after.path?.servedBy.kind).toBe("query");
      const notes = (r: typeof after) =>
         (
            (r.result as { annotations?: { value: string }[] } | undefined)
               ?.annotations ?? []
         ).map((a) => a.value.trim());
      expect(notes(after)).toContain("# bar_chart");
   });

   for (const [label, model] of [
      ["the model (REST and MCP queryName)", "m.malloy"],
      ["a single-query report", "report.malloy"],
   ] as const) {
      it(`serves queryName from ${label}, equal to live`, async () => {
         const m = fixture.pkg.getModel(model)!;
         const byName = await m.runQuery(
            unrecordedAudit(),
            { queryName: "by_carrier" } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         expect(byName.path?.servedBy.kind).toBe("query");
         expect(byName.servedFrom).toBe("storage");
         const before = live.get("m.malloy\u0000run: by_carrier")!;
         expect(byName.compactResult as unknown).toEqual(before.rows);
      });
   }

   it("reports the query's table as kind query, with its table id", async () => {
      const m = fixture.pkg.getModel("m.malloy")!;
      const result = await m.runQuery(
         unrecordedAudit(),
         { query: "run: top" } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      expect(result.path?.servedBy.kind).toBe("query");
      expect(result.path?.servedBy.tableId).toBeDefined();
      expect(result.path?.considered[0]).toMatchObject({
         kind: "query",
         outcome: "used",
      });
   });

   for (const [label, model, malloy, skip, givens] of answeredLive) {
      it(`answers ${label} live, equal to live`, async () => {
         const before = live.get(`${model}\u0000${malloy}`)!;
         const after = answerOf(await fixture.run(model, malloy, givens));
         expect(after.servedBy).not.toBe("query");
         expect(after.servedFrom).not.toBe("storage");
         expect(rowDifference(before.rows, after.rows)).toBeUndefined();
         const result = await fixture.pkg
            .getModel(model)!
            .runQuery(
               unrecordedAudit(),
               { query: malloy, ...(givens ? { givens } : {}) } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const considered = result.path?.considered.find(
            (c) => (c.kind as string) === "query",
         );
         // Ordered as live orders it, row for row.
         expect(after.rows).toEqual(before.rows);
         if (skip) {
            expect(considered?.outcome).toBe("skipped");
            expect(considered?.reason as string | undefined).toBe(skip);
         } else {
            expect(considered).toBeUndefined();
         }
      });
   }

   for (const gatedSwitch of [true, false]) {
      it(`answers a gated query live with gated sources ${gatedSwitch ? "on" : "off"}, per caller`, async () => {
         const inner = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gatedSwitch
                  ? "true"
                  : "false",
            },
         }).restore;
         try {
            for (const tenant of ["t1", "t2"]) {
               const malloy = "run: ops_by_tenant";
               const givens = { TENANT: tenant };
               const result = await fixture.pkg
                  .getModel("m.malloy")!
                  .runQuery(
                     unrecordedAudit(),
                     { query: malloy, givens } as never,
                     QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
                  );
               const before = answerOf(
                  await loaded.run("m.malloy", malloy, givens),
               );
               const rows = result.compactResult as unknown as Record<
                  string,
                  unknown
               >[];
               expect(rows.length).toBeGreaterThan(0);
               for (const row of rows) expect(row.tenant).toBe(tenant);
               expect(rows).toEqual(before.rows);
               expect(result.path?.servedBy.kind).toBe("live");
               expect(result.servedFrom ?? null).toBeNull();
               // Never planned, so the path report has no table to name;
               // the route counter records the leg's CONTRACT skip (below).
               expect(
                  result.path?.considered.find(
                     (c) => (c.kind as string) === "query",
                  ),
               ).toBeUndefined();
            }
         } finally {
            inner();
         }
      });
   }

   describe("publisher_query_route_total", () => {
      const counted = async (
         malloy: string,
         givens?: Record<string, string>,
      ) => {
         const harness = await startMetricsHarness();
         resetMaterializationTelemetryForTesting();
         try {
            await fixture.run("m.malloy", malloy, givens);
            const count = (leg: string, outcome: string, reason: string) =>
               harness.collectCounter("publisher_query_route_total", {
                  leg,
                  outcome,
                  reason,
               });
            return {
               viewUsed: await count("view", "used", "NONE"),
               viewMissingField: await count(
                  "view",
                  "skipped",
                  "MISSING_FIELD",
               ),
               viewContract: await count("view", "skipped", "CONTRACT"),
               liveUsed: await count("live", "used", "NONE"),
            };
         } finally {
            resetMaterializationTelemetryForTesting();
            await harness.shutdown();
         }
      };

      it("counts a served exact call as the view leg, used", async () => {
         expect(await counted("run: by_carrier")).toEqual({
            viewUsed: 1,
            viewMissingField: 0,
            viewContract: 0,
            liveUsed: 0,
         });
      });

      it("counts a refinement as the view leg skipped MISSING_FIELD, then live", async () => {
         expect(await counted("run: by_carrier + { limit: 1 }")).toEqual({
            viewUsed: 0,
            viewMissingField: 1,
            viewContract: 0,
            liveUsed: 1,
         });
      });

      it("counts a gated query as the view leg skipped CONTRACT, then live", async () => {
         expect(await counted("run: ops_by_tenant", { TENANT: "t1" })).toEqual({
            viewUsed: 0,
            viewMissingField: 0,
            viewContract: 1,
            liveUsed: 1,
         });
      });
   });

   it("answers the refused gated query live, per caller", async () => {
      for (const tenant of ["t1", "t2"]) {
         const outcome = answerOf(
            await fixture.run("m.malloy", "run: ops_by_carrier", {
               TENANT: tenant,
            }),
         );
         expect(outcome.servedBy).toBe("live");
         const before = answerOf(
            await loaded.run("m.malloy", "run: ops_by_carrier", {
               TENANT: tenant,
            }),
         );
         expect(rowDifference(before.rows, outcome.rows)).toBeUndefined();
      }
   });
});

describe("R4: a persisted named query with PUBLISHER_MATERIALIZE off", () => {
   let restore: () => void;
   let loaded: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "false" },
      }).restore;
      await freshLoadPool();
      loaded = await loadFixture({ "m.malloy": MODEL });
      fixture = await loaded.bind();
   });
   afterAll(async () => {
      await loaded?.dispose();
      restore();
   });

   it("builds nothing for it and warns persist-query-not-built", () => {
      expect(fixture.built.filter((b) => b.name.endsWith("__query"))).toEqual(
         [],
      );
      const warned = fixture.warnings.filter(
         (w) => w.code === "persist-query-not-built",
      );
      expect(warned.map((w) => w.subject)).toContain("by_carrier");
      expect(warned[0]?.message).toContain("answered live");
   });

   it("answers run: q and queryName live", async () => {
      const outcome = answerOf(
         await fixture.run("m.malloy", "run: by_carrier"),
      );
      expect(outcome.servedBy).toBe("live");
      expect(outcome.rows.length).toBe(3);
      const byName = await fixture.pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { queryName: "by_carrier" } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      expect(byName.path?.servedBy.kind ?? "live").toBe("live");
      expect(byName.compactResult as unknown).toEqual(outcome.rows);
   });
});
