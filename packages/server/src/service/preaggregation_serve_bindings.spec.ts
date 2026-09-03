// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for turning a manifest's rollup entries into serve bindings.
//
// Two of these pin failure modes whose blast radius is the whole PACKAGE's
// storage serving rather than the rollup's, which is what makes them worth
// having as tests rather than as comments: the name-collision refusal, and the
// invariant that an unservable rollup leaves every other bound source alone.
// Both are the kind of thing a later change breaks silently, because the symptom
// is a tier quietly not being used rather than an error.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { bindingsAllowDegradeToLive } from "./model";
import {
   buildServeShapeModelForBindings,
   buildVirtualMap,
   deriveServeBindings,
   type ServeBinding,
} from "./materialization_serve_transform";
import {
   rollupServeBindings,
   rollupServeRefinements,
} from "./preaggregation_serve_bindings";
import type { RollupPlan } from "./preaggregation_synthesis";

function plan(
   rollupSourceName: string,
   grainDimensions: string[],
   measures: { name: string; reaggregate: "sum" | "min" | "max" }[] = [
      { name: "total", reaggregate: "sum" },
   ],
): RollupPlan {
   return {
      baseSourceName: "orders",
      rollupSourceName,
      grainDimensions,
      measures: measures.map((m) => ({
         name: m.name,
         partialName: `${m.name}__partial`,
         reaggregate: m.reaggregate,
      })),
   };
}

function binding(
   sourceName: string,
   origin: ServeBinding["origin"] = "preaggregate",
): ServeBinding {
   return {
      sourceName,
      origin,
      destinationName: "lake",
      virtualHandle: `h_${sourceName}`,
      tablePath: `lake.t_${sourceName}`,
      schema: [
         { name: "category", type: "VARCHAR" },
         { name: "total__partial", type: "HUGEINT" },
      ],
   };
}

describe("deriveServeBindings carries the entry's origin", () => {
   const entry = (
      sourceEntityId: string,
      origin?: "persist" | "preaggregate",
   ) => ({
      sourceEntityId,
      sourceName: `s_${sourceEntityId}`,
      storageDestinationName: "lake",
      physicalTableName: `t_${sourceEntityId}`,
      schema: [{ name: "a", type: "BIGINT" }],
      ...(origin ? { origin } : {}),
   });

   it("reads preaggregate through", () => {
      // No aliases: origin is a property of the entry, not of the names it is
      // bound under, so the alias map is irrelevant to what this asserts.
      const [b] = deriveServeBindings({ e1: entry("e1", "preaggregate") }, {});
      expect(b.origin).toBe("preaggregate");
   });

   it("an ABSENT origin means persist, per the wire default", () => {
      // An entry written before the field existed carries no origin, and it can
      // only ever have described an authored `#@ persist` source. Defaulting the
      // other way would send an ordinary binding down the rollup path, where the
      // author-model lookups it depends on are deliberately skipped.
      const [b] = deriveServeBindings({ e1: entry("e1") }, {});
      expect(b.origin).toBe("persist");
   });
});

describe("a rollup's measures come from its plan, not from the author model", () => {
   it("emits the merge over each stored partial, under the base's measure name", () => {
      // The measure keeps the BASE's name so a query asking for `total` finds it;
      // only the stored column is renamed. Getting this backwards would compile
      // and serve nothing, since no query names `total__partial`.
      expect(
         rollupServeRefinements(
            plan(
               "r",
               ["category"],
               [
                  { name: "total", reaggregate: "sum" },
                  { name: "biggest", reaggregate: "max" },
               ],
            ),
         ),
      ).toEqual([
         { kind: "measure", name: "total", code: "total__partial.sum()" },
         { kind: "measure", name: "biggest", code: "biggest__partial.max()" },
      ]);
   });

   it("attaches them to the binding, replacing whatever it arrived with", () => {
      const { groups } = rollupServeBindings(
         [binding("r_cat")],
         [plan("r_cat", ["category"])],
      );
      expect(groups).toHaveLength(1);
      expect(groups[0].baseSourceName).toBe("orders");
      expect(groups[0].members[0].refinements).toEqual([
         { kind: "measure", name: "total", code: "total__partial.sum()" },
      ]);
   });

   it("leaves the captured schema alone", () => {
      // A rollup's stored columns ARE its whole surface — synthesis chose them —
      // so there is no wider physical table to hide part of. Narrowing them
      // against an author source would strip the `__partial` columns its own
      // measures read, which is the failure this path exists to avoid.
      const { groups } = rollupServeBindings(
         [binding("r_cat")],
         [plan("r_cat", ["category"])],
      );
      expect(groups[0].members[0].schema.map((c) => c.name)).toEqual([
         "category",
         "total__partial",
      ]);
   });
});

describe("members are ordered coarsest-grain first", () => {
   it("matches the order synthesis emits compose() members in", () => {
      // The two legs must agree: the resolver takes the first member that covers,
      // so a different order here would route the same query differently than the
      // synthesized text does.
      const { groups } = rollupServeBindings(
         [binding("r_wide"), binding("r_narrow")],
         [
            plan("r_wide", ["category", "order_date"]),
            plan("r_narrow", ["category"]),
         ],
      );
      expect(groups[0].members.map((m) => m.sourceName)).toEqual([
         "r_narrow",
         "r_wide",
      ]);
   });
});

describe("one author name cannot rebind to two shapes", () => {
   it("refuses the base's rollups when the base is ITSELF stored", () => {
      // The collision this whole grouping exists to catch. Emitting both would
      // produce two `source: orders is …` declarations, failing the entire
      // package's serve shape — and `compileServeShape` treats base-only as its
      // guaranteed floor, so a duplicate name breaches a floor the escalation
      // ladder cannot recover from.
      const { groups, conflicts } = rollupServeBindings(
         [binding("orders", "persist"), binding("r_cat")],
         [plan("r_cat", ["category"])],
      );
      expect(groups).toEqual([]);
      expect(conflicts).toHaveLength(1);
      expect(conflicts[0].baseSourceName).toBe("orders");
      expect(conflicts[0].reason).toContain("`orders`");
   });

   it("THE INVARIANT, at the CONFLICT path: an unservable rollup leaves others served", () => {
      // The property a future change is most likely to break silently, because
      // the symptom is a tier quietly not being used rather than an error. A
      // conflict on one base must cost that base's rollups and nothing else —
      // every other binding stays exactly as it was, available to the ordinary
      // path.
      const other = binding("customers", "persist");
      const { groups, conflicts } = rollupServeBindings(
         [binding("orders", "persist"), other, binding("r_cat")],
         [plan("r_cat", ["category"])],
      );
      expect(conflicts.map((c) => c.baseSourceName)).toEqual(["orders"]);
      expect(groups).toEqual([]);
      // `customers` was never a rollup binding, so it is untouched and still
      // available to the ordinary serve path.
      expect(other.refinements).toBeUndefined();
      expect(other.sourceName).toBe("customers");
   });
});

describe("a rollup with no matching plan is skipped, not fatal", () => {
   it("drops it and keeps the rest of the base's rollups", () => {
      // The manifest outlives a model edit: an annotation removed or a grain
      // changed leaves a real table nothing can describe. Serving the base is
      // correct, and the next build drops the table.
      const { groups, conflicts } = rollupServeBindings(
         [binding("r_gone"), binding("r_cat")],
         [plan("r_cat", ["category"])],
      );
      expect(conflicts).toEqual([]);
      expect(groups[0].members.map((m) => m.sourceName)).toEqual(["r_cat"]);
   });

   it("a package with no rollup bindings produces nothing at all", () => {
      expect(rollupServeBindings([binding("orders", "persist")], [])).toEqual({
         groups: [],
         conflicts: [],
      });
   });
});

describe("the emitted shape compiles and routes against real tables", () => {
   // The generate -> compile -> bind -> run path for a rollup group, mirroring the
   // ordinary binding's own end-to-end test. The spike proved the compiler does
   // this against text written by hand; this proves the text we GENERATE is that
   // text. A green spike and a broken emitter look identical from the outside.
   let connections: FixedConnectionMap;
   let duckdb: DuckDBConnection;

   const NARROW = "lake_rollup_by_category";
   const WIDE = "lake_rollup_by_category_and_day";

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      const rows = `SELECT * FROM (VALUES
          (10, DATE '2024-01-01', 'A'),
          (20, DATE '2024-01-01', 'B'),
          (30, DATE '2024-01-02', 'A')
        ) AS t(amount, order_date, category)`;
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${NARROW} AS SELECT category,
             sum(amount) AS total__partial FROM (${rows}) GROUP BY category`,
      );
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${WIDE} AS SELECT category, order_date,
             sum(amount) AS total__partial FROM (${rows}) GROUP BY category, order_date`,
      );
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });

   const member = (sourceName: string, table: string, withDay: boolean) => ({
      sourceName,
      origin: "preaggregate" as const,
      destinationName: "duckdb",
      virtualHandle: `h_${sourceName}`,
      tablePath: table,
      schema: [
         { name: "category", type: "VARCHAR" },
         ...(withDay ? [{ name: "order_date", type: "DATE" }] : []),
         { name: "total__partial", type: "HUGEINT" },
      ],
   });

   /** Compile a query against the shape built from these bindings and plans. */
   async function runQuery(query: string, members: ServeBinding[]) {
      const { groups } = rollupServeBindings(members, [
         plan("r_narrow", ["category"]),
         plan("r_wide", ["category", "order_date"]),
      ]);
      const { modelText } = buildServeShapeModelForBindings([], groups);
      const root = "file:///rollupserve/";
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([[`${root}m.malloy`, modelText]]),
         ),
         connections,
      });
      const virtualMap = buildVirtualMap(groups.flatMap((g) => g.members));
      const runnable = runtime
         .loadModel(new URL(`${root}m.malloy`), {
            importBaseURL: new URL(root),
         })
         .loadQuery(query);
      return {
         sql: await runnable.getSQL({ virtualMap }),
         rows: (
            await runnable.run({
               virtualMap,
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
            } as any)
         ).data.toObject() as unknown as Record<string, unknown>[],
      };
   }

   const members = () => [
      member("r_narrow", NARROW, false),
      member("r_wide", WIDE, true),
   ];

   it("re-exposes the BASE's name, so an unchanged query routes", async () => {
      // The whole point: the author's query names `orders` and knows nothing about
      // any rollup. If the shape declared the rollup's synthesized name instead,
      // nothing would ever route to it.
      const { sql, rows } = await runQuery(
         "run: orders -> { group_by: category; aggregate: total }",
         members(),
      );
      expect(sql).toContain(`"${NARROW}"`);
      expect(
         Object.fromEntries(rows.map((r) => [r.category, Number(r.total)])),
      ).toEqual({ A: 40, B: 20 });
   });

   it("picks the coarser member, and the finer one when only it covers", async () => {
      const narrow = await runQuery(
         "run: orders -> { group_by: category; aggregate: total }",
         members(),
      );
      expect(narrow.sql).toContain(`"${NARROW}"`);
      expect(narrow.sql).not.toContain(`"${WIDE}"`);

      const wide = await runQuery(
         "run: orders -> { group_by: order_date; aggregate: total }",
         members(),
      );
      expect(wide.sql).toContain(`"${WIDE}"`);
   });

   it("a single-member group needs no special case", async () => {
      // compose() of one compiles and routes like any other, which is what makes
      // one grain and several one code path rather than two.
      const { sql } = await runQuery(
         "run: orders -> { group_by: category; aggregate: total }",
         [member("r_narrow", NARROW, false)],
      );
      expect(sql).toContain(`"${NARROW}"`);
   });

   it("an uncovered query does NOT compile — the fallback the serve path needs", async () => {
      // No base member, so the composite is not total. This must throw so the
      // caller falls back to live rather than being answered from rollup rows.
      await expect(
         runQuery(
            "run: orders -> { group_by: category; aggregate: total; where: amount > 15 }",
            members(),
         ),
      ).rejects.toThrow();
   });
});

describe("THE INVARIANT, at the COMPILE-FAILURE path", () => {
   // The conflict test above proves the invariant where a group is never emitted:
   // a conflict returns no groups, so no shape is built and nothing can break.
   // That leaves the case that actually threatens the package — a group that IS
   // emitted and then fails to compile — untested, which is how it went unnoticed
   // that no escalation tier could rescue it.
   //
   // Built from members that disagree about a grain column's captured type, so the
   // composite is invalid rather than merely unhelpful. The shape must still come
   // back compiling, with the ordinary binding intact and the group dropped —
   // otherwise one bad rollup takes every stored source in the package with it.
   let connections: FixedConnectionMap;

   beforeAll(async () => {
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE plain AS SELECT 'A' AS category, 10 AS amount",
      );
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE r_a AS SELECT 'A' AS category, 10 AS total__partial",
      );
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE r_b AS SELECT 1 AS category, 10 AS total__partial",
      );
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });

   it("drops the group, keeps the ordinary binding, and still compiles", async () => {
      const ordinary: ServeBinding = {
         sourceName: "plain",
         origin: "persist",
         destinationName: "duckdb",
         virtualHandle: "h_plain",
         tablePath: "plain",
         schema: [
            { name: "category", type: "VARCHAR" },
            { name: "amount", type: "BIGINT" },
         ],
      };
      // Same grain column, different declared types: a composite the compiler
      // cannot form.
      const broken = [
         {
            baseSourceName: "orders",
            members: [
               {
                  ...binding("r_a"),
                  destinationName: "duckdb",
                  tablePath: "r_a",
                  schema: [
                     { name: "category", type: "VARCHAR" },
                     { name: "total__partial", type: "BIGINT" },
                  ],
                  refinements: rollupServeRefinements(
                     plan("r_a", ["category"]),
                  ),
               },
               {
                  ...binding("r_b"),
                  destinationName: "duckdb",
                  tablePath: "r_b",
                  schema: [
                     { name: "category", type: "BIGINT" },
                     { name: "total__partial", type: "BIGINT" },
                  ],
                  refinements: rollupServeRefinements(
                     plan("r_b", ["category"]),
                  ),
               },
            ],
         },
      ];

      // The full shape, with the group, must NOT compile — otherwise this test
      // proves nothing about the escalation.
      const withGroup = buildServeShapeModelForBindings([ordinary], broken);
      const root = "file:///escalation/";
      const load = (text: string) =>
         new Runtime({
            urlReader: new InMemoryURLReader(
               new Map([[`${root}m.malloy`, text]]),
            ),
            connections,
         }).loadModel(new URL(`${root}m.malloy`), {
            importBaseURL: new URL(root),
         });
      await expect(load(withGroup.modelText).getModel()).rejects.toThrow();

      // The tier that drops the group is what the serve path falls back to, and it
      // must compile AND still carry the unrelated source.
      const withoutGroup = buildServeShapeModelForBindings([ordinary], []);
      await expect(
         load(withoutGroup.modelText).getModel(),
      ).resolves.toBeDefined();
      expect(withoutGroup.modelText).toContain("source: plain is");
      expect(withoutGroup.modelText).not.toContain("compose(");
   });
});

describe("a rollup neither vetoes nor enables degrade-to-live", () => {
   // This predicate has been wrong in both directions, which is why it is
   // extracted and tested rather than left inline. Counting rollups let one with
   // no declared fallback veto degradation for the authored sources beside it;
   // filtering them out then meant a package whose ONLY bindings are rollups —
   // the documented common shape, since a rollup's base is typically an
   // unpersisted table source — could never degrade at all, turning a store
   // hiccup into a user-facing error on a query that used to answer live.
   const b = (
      origin: ServeBinding["origin"],
      freshnessFallback?: ServeBinding["freshnessFallback"],
   ): ServeBinding => ({
      sourceName: "s",
      origin,
      destinationName: "lake",
      virtualHandle: "h",
      tablePath: "t",
      schema: [{ name: "a", type: "BIGINT" }],
      freshnessFallback,
   });

   it("a rollup-only shape CAN degrade, with no fallback declared", () => {
      expect(bindingsAllowDegradeToLive([b("preaggregate")])).toBe(true);
   });

   it("a rollup does not rescue an authored source that refuses", () => {
      // The `every` exists so one authored source declaring anything but `live`
      // keeps its neighbours from degrading. A rollup must not weaken that.
      expect(
         bindingsAllowDegradeToLive([b("preaggregate"), b("persist", "fail")]),
      ).toBe(false);
   });

   it("a rollup does not veto an authored source that permits", () => {
      expect(
         bindingsAllowDegradeToLive([b("preaggregate"), b("persist", "live")]),
      ).toBe(true);
   });

   it("authored-only behaviour is unchanged in both directions", () => {
      expect(bindingsAllowDegradeToLive([b("persist", "live")])).toBe(true);
      expect(bindingsAllowDegradeToLive([b("persist", "stale_ok")])).toBe(
         false,
      );
      expect(bindingsAllowDegradeToLive([b("persist")])).toBe(false);
   });

   it("nothing routed means nothing to degrade", () => {
      expect(bindingsAllowDegradeToLive([])).toBe(false);
   });
});
