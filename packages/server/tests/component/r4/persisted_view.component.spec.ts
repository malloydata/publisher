// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for `#@ persist` on a view (materialization v2, phase 4, P14): with
 * PUBLISHER_MATERIALIZE on, a persisted view's table answers a query only
 * when the query calls that view exactly or starts from it, and then its
 * answer equals live. A refinement, a nest, a hand-written look-alike and a
 * call on an extension are answered live, the path report saying why.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE fl AS SELECT * FROM (VALUES
      (1, 'aa', 't1', 10.5, TIMESTAMP '2026-01-01 10:00:00'),
      (2, 'aa', 't2', 20.0, TIMESTAMP '2026-02-02 11:00:00'),
      (3, 'bb', 't1', 5.5, TIMESTAMP '2026-01-02 12:00:00'),
      (4, 'bb', 't1', 11.0, TIMESTAMP '2026-03-03 13:00:00'),
      (5, 'cc', 't2', 2.0, TIMESTAMP '2026-03-04 14:00:00'),
      (6, 'aa', 't1', 7.0, TIMESTAMP '2026-03-05 15:00:00')
    ) AS t(id, carrier, tenant, d, ts)`,
];

const model = `source: flights is duckdb.table('fl') extend {
  measure:
    n is count()
    dist is d.sum()
  # bar_chart
  #@ persist storage="duckdb"
  view: by_carrier is { group_by: carrier; aggregate: n, dist }
  #@ persist storage="duckdb"
  view: by_month is { group_by: m is ts.month; aggregate: n }
  #@ persist storage="duckdb"
  view: two_stage is { group_by: carrier; aggregate: n } -> { select: carrier, n; where: n > 1 }
  #@ persist name="flights_nested"
  view: nested is { group_by: carrier; nest: by_tenant is { group_by: tenant; aggregate: n } }
  #@ persist storage="duckdb"
  view: recent is { where: ts > now - 30 days; group_by: carrier; aggregate: n }
  #@ persist storage="duckdb"
  view: top is { group_by: carrier; aggregate: n; limit: 2 }
  view: plain is { group_by: carrier; aggregate: n }
}

source: aa_flights is flights extend {
  where: carrier = 'aa'
}
`;

describe("R4: a persisted view answers its exact call, equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         persistedViews: true,
         colocated: ["flights__nested__view"],
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("refuses a view whose result reads the current time, with a warning", () => {
      expect(fixture.viewRefusals.some((r) => r.includes("current time"))).toBe(
         true,
      );
   });

   const served: [string, string, boolean][] = [
      ["an exact call", "run: flights -> by_carrier", true],
      ["a timeframe column", "run: flights -> by_month", true],
      ["a multi-stage view", "run: flights -> two_stage", true],
      ["a limited view", "run: flights -> top", true],
      [
         "a query that starts from the view",
         "run: flights -> by_carrier -> { select: carrier, n; where: n > 1 }",
         true,
      ],
      [
         "a query that starts from the view and aggregates",
         "run: flights -> by_carrier -> { aggregate: total is n.sum() }",
         true,
      ],
      ["a nested view, colocated", "run: flights -> nested", false],
   ];
   for (const [label, malloy, storage] of served) {
      it(`serves ${label} from the view's table, equal to live`, async () => {
         const run = await fixture.run({ name: label, malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         // Names, kinds and timeframes as live's; a stored number reads back
         // without its integer/decimal subtype, as from the served copy.
         const shape = (fields: typeof run.liveFields) =>
            fields.map((f) => {
               const t = f.type as { kind?: string; timeframe?: string };
               return [f.name, t?.kind, t?.timeframe];
            });
         expect(shape(run.servedFields)).toEqual(shape(run.liveFields));
         if (storage) expect(run.servedFrom).toBe("storage");
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.servedBy.kind).toBe("view");
         expect(path?.servedBy.tableId).toBeDefined();
      });
   }

   it("keeps the view's column order and ordering", async () => {
      fixture.setBindings(fixture.bindings);
      const served = await fixture.runAsIs("run: flights -> by_carrier");
      fixture.setBindings([]);
      const live = await fixture.runAsIs("run: flights -> by_carrier");
      expect(served.path?.servedBy.kind).toBe("view");
      expect(served.rows).toEqual(live.rows);
      expect(Object.keys(served.rows[0] ?? {})).toEqual(
         Object.keys(live.rows[0] ?? {}),
      );
   });

   const live: [string, string, string | undefined][] = [
      [
         "a refined call",
         "run: flights -> by_carrier + { limit: 1 }",
         "MISSING_FIELD",
      ],
      [
         "a hand-written look-alike with identical SQL",
         "run: flights -> { group_by: carrier; aggregate: n, dist }",
         undefined,
      ],
      ["a nest of the view", "run: flights -> { nest: by_carrier }", undefined],
      [
         "the view called on an extension that filters",
         "run: aa_flights -> by_carrier",
         undefined,
      ],
      ["a view that is not persisted", "run: flights -> plain", undefined],
      [
         "a caller's redefinition of the source",
         "source: flights2 is flights\nrun: flights2 -> by_carrier",
         undefined,
      ],
   ];
   for (const [label, malloy, reason] of live) {
      it(`answers ${label} live`, async () => {
         const run = await fixture.run({
            name: label,
            malloy,
            expectLive: true,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path, servedFrom } = await fixture.runAsIs(malloy);
         expect(path?.servedBy.kind).not.toBe("view");
         expect(servedFrom).not.toBe("storage");
         const viewSkip = path?.considered.find((c) => c.kind === "view");
         if (reason) {
            expect(viewSkip?.outcome).toBe("skipped");
            expect(viewSkip?.reason as string | undefined).toBe(reason);
            expect(viewSkip?.detail).toBeDefined();
         } else {
            expect(viewSkip).toBeUndefined();
         }
      });
   }

   it("reports an unbuilt view NOT_BUILT and answers live", async () => {
      fixture.setBindings(
         fixture.bindings.filter(
            (b) => b.sourceName !== "flights__by_carrier__view",
         ),
      );
      const { path, servedFrom } = await fixture.runAsIs(
         "run: flights -> by_carrier",
      );
      expect(servedFrom).not.toBe("storage");
      expect(path?.servedBy.kind).toBe("live");
      expect(path?.considered.find((c) => c.kind === "view")?.reason).toBe(
         "NOT_BUILT",
      );
   });

   it("reports a view past its freshness window STALE and answers live", async () => {
      fixture.setBindings(
         fixture.bindings.map((b) =>
            b.sourceName === "flights__by_carrier__view"
               ? {
                    ...b,
                    freshAsOf: "2020-01-01T00:00:00.000Z",
                    freshnessWindowSeconds: 60,
                    freshnessFallback: "live",
                 }
               : b,
         ),
      );
      const { path, servedFrom } = await fixture.runAsIs(
         "run: flights -> by_carrier",
      );
      expect(servedFrom).not.toBe("storage");
      expect(path?.servedBy.kind).toBe("live");
      expect(path?.considered.find((c) => c.kind === "view")?.reason).toBe(
         "STALE",
      );
   });
});

const gatedModel = `##! experimental { givens }
given: TENANT :: string is ""

#(access_filter) tenant = $TENANT
source: ops is duckdb.table('fl') extend {
  measure: n is count()
  #@ persist name="ops_by_tenant"
  view: by_tenant is { group_by: tenant, carrier; aggregate: n }
  #@ persist name="ops_by_carrier"
  view: by_carrier is { group_by: carrier; aggregate: n }
}
`;

for (const gatedSwitch of [true, false]) {
   describe(`R4: a persisted view on a gated source (gated sources ${gatedSwitch ? "on" : "off"})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gatedSwitch
                  ? "true"
                  : "false",
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": gatedModel },
            seedSql,
            persistedViews: true,
            colocated: ["ops__by_tenant__view"],
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("refuses a view that does not group by the access filter's field (question 3)", () => {
         expect(fixture.viewRefusals.some((r) => r.includes("`tenant`"))).toBe(
            true,
         );
      });

      for (const tenant of ["t1", "t2"]) {
         it(`grafts the gate on the view's table for ${tenant}, equal to live`, async () => {
            const malloy = "run: ops -> by_tenant";
            const run = await fixture.run({
               name: tenant,
               malloy,
               givens: { TENANT: tenant },
               expectLive: !gatedSwitch,
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            console.log(
               "ROWS",
               JSON.stringify(run.live),
               JSON.stringify(run.served),
               run.servedFrom,
            );
            expect(run.served.length).toBeGreaterThan(0);
            fixture.setBindings(fixture.bindings);
            const result = await fixture.model.runQuery(
               (
                  await import("../../../src/service/access_record")
               ).unrecordedAudit(),
               { query: malloy, filterParams: {}, givens: { TENANT: tenant } },
               (
                  await import("../../../src/service/model_query_request")
               ).QueryPrivileges.of({ bypassFilters: true }),
            );
            const view = result.path?.considered.find((c) => c.kind === "view");
            if (gatedSwitch) {
               expect(result.path?.servedBy.kind).toBe("view");
            } else {
               expect(view?.reason).toBe("CONTRACT");
            }
         });
      }

      it("answers the refused view live", async () => {
         const run = await fixture.run({
            name: "refused",
            malloy: "run: ops -> by_carrier",
            givens: { TENANT: "t1" },
            expectLive: true,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   });
}

// The routed re-proof compares the variant's whole gate set with live's:
// a view declared on an extension carries its base's lock (inherited) and
// its own access filter, both written on the variant, both re-proven.
const twoGateModel = `##! experimental { givens }
given: TENANT :: string is ""
given: ROLE :: string is ""

#(authorize) $ROLE = 'admin'
source: base is duckdb.table('fl') extend {
  measure: n is count()
}

#(access_filter) tenant = $TENANT
source: ops is base extend {
  #@ persist name="ops_by_tenant2"
  view: by_tenant is { group_by: tenant, carrier; aggregate: n }
}
`;

describe("R4: a persisted view on an extension with an inherited lock and its own filter", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": twoGateModel },
         seedSql,
         persistedViews: true,
         colocated: ["ops__by_tenant__view"],
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const tenant of ["t1", "t2"]) {
      it(`answers ${tenant} from the view's table, equal to live`, async () => {
         const malloy = "run: ops -> by_tenant";
         const givens = { TENANT: tenant, ROLE: "admin" };
         const run = await fixture.run({ name: tenant, malloy, givens });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.served.length).toBeGreaterThan(0);
         for (const row of run.served) expect(row.tenant).toBe(tenant);
         fixture.setBindings(fixture.bindings);
         const result = await fixture.model.runQuery(
            (
               await import("../../../src/service/access_record")
            ).unrecordedAudit(),
            { query: malloy, filterParams: {}, givens },
            (
               await import("../../../src/service/model_query_request")
            ).QueryPrivileges.of({ bypassFilters: true }),
         );
         expect(result.path?.servedBy.kind).toBe("view");
      });
   }

   it("denies a caller the inherited lock refuses", async () => {
      fixture.setBindings(fixture.bindings);
      const outcome = await fixture.model
         .runQuery(
            (
               await import("../../../src/service/access_record")
            ).unrecordedAudit(),
            {
               query: "run: ops -> by_tenant",
               filterParams: {},
               givens: { TENANT: "t1", ROLE: "viewer" },
            },
            (
               await import("../../../src/service/model_query_request")
            ).QueryPrivileges.of({ bypassFilters: true }),
         )
         .then(
            () => undefined,
            (e: unknown) => e,
         );
      expect((outcome as Error)?.constructor?.name).toBe("AccessDeniedError");
   });
});

// Which source declares a persisted view, end to end: only the statement that
// annotates it (declaredFieldNotes). A view an extension renames carries its
// base's notes and answers live; an `include:` re-annotation declares it on
// the including source, answered from that source's own table.
const declaringModel = `##! experimental.access_modifiers
source: flights is duckdb.table('fl') extend {
  measure: n is count()
  #@ persist storage="duckdb"
  view: by_carrier is { group_by: carrier; aggregate: n }
  view: by_tenant is { group_by: tenant; aggregate: n }
}

source: renamed is flights extend { rename: carriers is by_carrier }

source: public_flights is flights include {
  #@ persist storage="duckdb"
  public: by_tenant
  public: *
}
`;

describe("R4: a persisted view answers only on the source whose statement annotates it", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": declaringModel },
         seedSql,
         persistedViews: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds the base's view and the include's re-annotated view, not the renamed one", () => {
      expect(fixture.bindings.map((b) => b.sourceName).sort()).toEqual([
         "flights__by_carrier__view",
         "public_flights__by_tenant__view",
      ]);
   });

   it("answers a view an extension renames live", async () => {
      const malloy = "run: renamed -> carriers";
      const run = await fixture.run({
         name: "renamed",
         malloy,
         expectLive: true,
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.servedBy.kind).toBe("live");
      expect(path?.considered.find((c) => c.kind === "view")).toBeUndefined();
   });

   it("answers an include's re-annotated view from its own table, equal to live", async () => {
      const malloy = "run: public_flights -> by_tenant";
      const run = await fixture.run({ name: "include", malloy });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.servedBy.kind).toBe("view");
   });

   it("answers the base's unannotated view live", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs("run: flights -> by_tenant");
      expect(path?.servedBy.kind).toBe("live");
   });
});

// A row-level gate on a JOINED source the view reads is not the entry
// point's: gates are never walked through joins (docs/row-level-access.md),
// so live reads every row too, and the view's table answers, equal to live,
// with the gated switch on or off.
const joinGatedModel = `##! experimental { givens }
given: TENANT :: string is ""

#(access_filter) tenant = $TENANT
source: tenants is duckdb.sql("select distinct tenant from fl") extend {
  primary_key: tenant
}

source: ops is duckdb.table('fl') extend {
  join_one: t is tenants on tenant = t.tenant
  measure: n is count()
  #@ persist name="ops_by_joined_tenant"
  view: by_joined_tenant is { group_by: t.tenant; aggregate: n }
}
`;

for (const gatedSwitch of [true, false]) {
   describe(`R4: a persisted view reading a gated join (gated sources ${gatedSwitch ? "on" : "off"})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: gatedSwitch
                  ? "true"
                  : "false",
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": joinGatedModel },
            seedSql,
            persistedViews: true,
            colocated: ["ops__by_joined_tenant__view"],
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const tenant of ["t1", "t2"]) {
         it(`answers ${tenant} from the view's table, equal to live`, async () => {
            const malloy = "run: ops -> by_joined_tenant";
            const run = await fixture.run({
               name: tenant,
               malloy,
               givens: { TENANT: tenant },
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            fixture.setBindings(fixture.bindings);
            const result = await fixture.model.runQuery(
               (
                  await import("../../../src/service/access_record")
               ).unrecordedAudit(),
               { query: malloy, filterParams: {}, givens: { TENANT: tenant } },
               (
                  await import("../../../src/service/model_query_request")
               ).QueryPrivileges.of({ bypassFilters: true }),
            );
            expect(result.path?.servedBy.kind).toBe("view");
         });
      }
   });
}
