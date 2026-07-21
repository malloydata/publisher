// Real-compiler + real-DuckDB contract for the virtual-source serve transform.
// The declared serve-shape schema is trusted on faith by the compiler (it does
// NOT type-check a virtual source's columns), so the generate -> compile ->
// bind -> run path is pinned end-to-end here against a live table: a drift in
// the user-type syntax, the virtualMap contract, or the type mapping must fail
// here rather than surface as a serve-time execution error in production.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { MaterializationEligibilityError } from "../errors";
import {
   assertServesInDuckDB,
   buildServeShapeModel,
   buildServeShapeModelForBindings,
   buildVirtualMap,
   deriveServeBindings,
   duckdbTypeToMalloy,
   extractJoins,
   extractRefinements,
   extractViews,
   sliceSourceRange,
   type ServeBinding,
} from "./materialization_serve_transform";

describe("duckdbTypeToMalloy", () => {
   it.each([
      ["BIGINT", "number"],
      ["INTEGER", "number"],
      ["HUGEINT", "number"],
      ["UBIGINT", "number"],
      ["DOUBLE", "number"],
      ["DECIMAL(18,2)", "number"],
      ["NUMERIC", "number"],
      ["VARCHAR", "string"],
      ["VARCHAR(255)", "string"],
      ["TEXT", "string"],
      ["UUID", "string"],
      ["BOOLEAN", "boolean"],
      ["BOOL", "boolean"],
      ["DATE", "date"],
      ["TIMESTAMP", "timestamp"],
      ["TIMESTAMP WITH TIME ZONE", "timestamp"],
      ["TIMESTAMPTZ", "timestamp"],
      ["timestamp", "timestamp"],
      ["INTEGER[]", "json"],
      ["STRUCT(a INTEGER)", "json"],
      ["BLOB", "json"],
   ])("maps %s -> %s", (duck, malloy) => {
      expect(duckdbTypeToMalloy(duck)).toBe(malloy);
   });
});

describe("buildServeShapeModel", () => {
   it("emits the flag, a double-colon type shape, and the virtual source line", () => {
      const binding: ServeBinding = {
         sourceName: "mz_orders",
         connectionName: "lake",
         virtualHandle: "mz_orders__g1",
         tablePath: "analytics.mz_orders",
         schema: [
            { name: "amount", type: "BIGINT" },
            { name: "region", type: "VARCHAR" },
            { name: "ts", type: "TIMESTAMP WITH TIME ZONE" },
         ],
      };
      const { modelText, shapeTypeName } = buildServeShapeModel(
         "mz_orders",
         binding,
      );
      expect(shapeTypeName).toBe("mz_orders__shape");
      expect(modelText).toContain("##! experimental.virtual_source");
      expect(modelText).toContain("type: mz_orders__shape is {");
      expect(modelText).toContain("amount::number");
      expect(modelText).toContain("region::string");
      expect(modelText).toContain("ts::timestamp");
      expect(modelText).toContain(
         "source: mz_orders is lake.virtual('mz_orders__g1')::mz_orders__shape",
      );
   });

   it("backtick-quotes a field name that is not a bare identifier", () => {
      const { modelText } = buildServeShapeModel("s", {
         sourceName: "s",
         connectionName: "lake",
         virtualHandle: "h",
         tablePath: "t",
         schema: [{ name: "odd name", type: "VARCHAR" }],
      });
      expect(modelText).toContain("`odd name`::string");
   });
});

describe("buildVirtualMap", () => {
   it("groups handles by connection and quotes the table path for DuckDB", () => {
      const map = buildVirtualMap([
         {
            sourceName: "a",
            connectionName: "lake",
            virtualHandle: "h1",
            tablePath: "analytics.a",
            schema: [],
         },
         {
            sourceName: "b",
            connectionName: "lake",
            virtualHandle: "h2",
            tablePath: "b",
            schema: [],
         },
      ]);
      expect(map.get("lake")?.get("h1")).toBe('"analytics"."a"');
      expect(map.get("lake")?.get("h2")).toBe('"b"');
   });
});

describe("serve transform end-to-end (generate -> compile -> bind -> run)", () => {
   let connections: FixedConnectionMap;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE TABLE mz_physical AS " +
            "SELECT 10 AS amount, 'US' AS region " +
            "UNION ALL SELECT 20, 'EU' UNION ALL SELECT 30, 'US'",
      );
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });

   /** DESCRIBE the physical table to build a binding from its real schema. */
   async function bindingFromLiveTable(): Promise<ServeBinding> {
      const described = await duckdb.runSQL("DESCRIBE mz_physical");
      const rows = Array.isArray(described) ? described : described.rows;
      const schema = (rows as Record<string, unknown>[]).map((r) => ({
         name: String(r.column_name),
         type: String(r.column_type),
      }));
      return {
         sourceName: "mz",
         connectionName: "duckdb",
         virtualHandle: "mz_handle",
         tablePath: "mz_physical",
         schema,
      };
   }

   it("runs a query against the virtual source bound to the live table", async () => {
      const binding = await bindingFromLiveTable();
      const { modelText } = buildServeShapeModel("mz", binding);
      const root = "file:///e2e/";
      const urlReader = new InMemoryURLReader(
         new Map([[`${root}m.malloy`, modelText]]),
      );
      const runtime = new Runtime({ urlReader, connections });
      const query = runtime
         .loadModel(new URL(`${root}m.malloy`), {
            importBaseURL: new URL(root),
         })
         .loadQuery("run: mz -> { aggregate: total is amount.sum() }");

      const virtualMap = buildVirtualMap([binding]);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await query.run({ virtualMap } as any);
      const out = result.data.toObject() as { total: number }[];
      expect(out[0].total).toBe(60);
   });

   it("assertServesInDuckDB passes for a well-formed captured schema", async () => {
      const binding = await bindingFromLiveTable();
      await expect(
         assertServesInDuckDB("mz", binding, connections),
      ).resolves.toBeUndefined();
   });

   it("assertServesInDuckDB refuses a serve shape that cannot compile", async () => {
      // A field named after a reserved token with no valid mapping path: force a
      // compile failure by declaring an empty shape name collision is hard, so
      // use a connection name that does not resolve — the virtual source's
      // connection must exist, so an unknown connection fails compilation.
      const binding: ServeBinding = {
         sourceName: "mz",
         connectionName: "does_not_exist",
         virtualHandle: "h",
         tablePath: "t",
         schema: [{ name: "amount", type: "BIGINT" }],
      };
      await expect(
         assertServesInDuckDB("mz", binding, connections),
      ).rejects.toThrow(MaterializationEligibilityError);
   });
});

describe("join serve end-to-end (two virtual sources, join runs in DuckDB)", () => {
   let connections: FixedConnectionMap;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE TABLE orders_phys AS " +
            "SELECT 10 AS amount, 'r1' AS region_id " +
            "UNION ALL SELECT 20, 'r2' UNION ALL SELECT 30, 'r1'",
      );
      await duckdb.runSQL(
         "CREATE TABLE regions_phys AS " +
            "SELECT 'r1' AS region_id, 'North' AS region_name " +
            "UNION ALL SELECT 'r2', 'South'",
      );
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });

   it("serves a query that traverses a join from the materialized tables", async () => {
      const bindings: ServeBinding[] = [
         {
            sourceName: "regions",
            connectionName: "duckdb",
            virtualHandle: "regions_h",
            tablePath: "regions_phys",
            schema: [
               { name: "region_id", type: "VARCHAR" },
               { name: "region_name", type: "VARCHAR" },
            ],
         },
         {
            sourceName: "orders",
            connectionName: "duckdb",
            virtualHandle: "orders_h",
            tablePath: "orders_phys",
            schema: [
               { name: "amount", type: "BIGINT" },
               { name: "region_id", type: "VARCHAR" },
            ],
            refinements: [
               {
                  kind: "join",
                  name: "regions",
                  keyword: "join_one",
                  text: "regions is regions on region_id = regions.region_id",
                  dependsOn: "regions",
               },
            ],
         },
      ];
      const { modelText } = buildServeShapeModelForBindings(bindings);
      const root = "file:///join-e2e/";
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([[`${root}m.malloy`, modelText]]),
         ),
         connections,
      });
      const query = runtime
         .loadModel(new URL(`${root}m.malloy`), {
            importBaseURL: new URL(root),
         })
         .loadQuery(
            "run: orders -> { group_by: regions.region_name; aggregate: total is amount.sum() }",
         );
      const virtualMap = buildVirtualMap(bindings);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await query.run({ virtualMap } as any);
      const out = result.data.toObject() as {
         region_name: string;
         total: number;
      }[];
      const byRegion = Object.fromEntries(
         out.map((r) => [r.region_name, r.total]),
      );
      expect(byRegion).toEqual({ North: 40, South: 20 });
   });
});

describe("deriveServeBindings", () => {
   it("binds only storage entries, keying the handle on sourceEntityId", () => {
      const bindings = deriveServeBindings({
         se_storage: {
            sourceEntityId: "se_storage",
            sourceName: "mz",
            physicalTableName: "mz_g003",
            connectionName: "wh",
            storageConnectionName: "lake",
            schema: [{ name: "amount", type: "BIGINT" }],
            dataAsOf: "2026-07-20T00:00:00Z",
            realization: "COPY",
            rowCount: null,
         },
         se_pathC: {
            // In-warehouse (no storage): served via the manifest, not the
            // transform — must NOT produce a binding.
            sourceEntityId: "se_pathC",
            sourceName: "orders",
            physicalTableName: "orders_v1",
            connectionName: "wh",
            realization: "COPY",
            rowCount: null,
         },
         se_noschema: {
            // Storage but no captured schema — skipped (can't declare a shape).
            sourceEntityId: "se_noschema",
            sourceName: "x",
            physicalTableName: "lake.x",
            connectionName: "wh",
            storageConnectionName: "lake",
            schema: [],
            realization: "COPY",
            rowCount: null,
         },
      });
      expect(bindings).toEqual([
         {
            sourceName: "mz",
            connectionName: "lake",
            virtualHandle: "se_storage",
            tablePath: "lake.mz_g003",
            schema: [{ name: "amount", type: "BIGINT" }],
            freshAsOf: "2026-07-20T00:00:00Z",
         },
      ]);
      // The table path is qualified with the destination catalog (attach alias)
      // so the serve reads <store>.<table>, not an unqualified name.
      expect(bindings[0].tablePath).toBe("lake.mz_g003");
   });
});

describe("extractRefinements", () => {
   it("maps derived fields to dimensions/measures and skips raw columns + joins", () => {
      const fields = [
         { name: "order_date", type: "date", expressionType: "scalar" }, // raw col (no code)
         { name: "total_amount", type: "number", expressionType: "scalar" }, // raw col
         {
            name: "avg_order_value",
            type: "number",
            expressionType: "scalar",
            code: "total_amount / order_count",
         },
         {
            name: "grand_total",
            type: "number",
            expressionType: "aggregate",
            code: "total_amount.sum()",
         },
         // analytic / window -> skipped (falls back)
         {
            name: "running",
            type: "number",
            expressionType: "analytic",
            code: "sum(total_amount)",
         },
         // join -> no code -> skipped
         { name: "region_dim", type: "join", join: "one" },
      ];
      expect(extractRefinements(fields)).toEqual([
         {
            kind: "dimension",
            name: "avg_order_value",
            code: "total_amount / order_count",
         },
         { kind: "measure", name: "grand_total", code: "total_amount.sum()" },
      ]);
   });

   it("returns [] for undefined/empty fields", () => {
      expect(extractRefinements(undefined)).toEqual([]);
      expect(extractRefinements([])).toEqual([]);
   });
});

describe("buildServeShapeModelForBindings with refinements", () => {
   it("re-declares dimensions/measures as an extend on the virtual base", () => {
      const { modelText } = buildServeShapeModelForBindings([
         {
            sourceName: "daily",
            connectionName: "lake",
            virtualHandle: "h",
            tablePath: "lake.daily",
            schema: [
               { name: "total_amount", type: "BIGINT" },
               { name: "order_count", type: "BIGINT" },
            ],
            refinements: [
               {
                  kind: "dimension",
                  name: "avg_order_value",
                  code: "total_amount / order_count",
               },
            ],
         },
      ]);
      expect(modelText).toContain(
         "source: daily is lake.virtual('h')::daily__shape extend {",
      );
      expect(modelText).toContain(
         "dimension: avg_order_value is total_amount / order_count",
      );
   });

   it("emits joins (verbatim, keyword-prefixed) before dimensions/measures", () => {
      const { modelText } = buildServeShapeModelForBindings([
         {
            sourceName: "orders",
            connectionName: "lake",
            virtualHandle: "h",
            tablePath: "lake.orders",
            schema: [
               { name: "amount", type: "BIGINT" },
               { name: "region_id", type: "VARCHAR" },
            ],
            refinements: [
               {
                  kind: "join",
                  name: "regions",
                  keyword: "join_one",
                  text: "regions is regions on region_id = regions.region_id",
                  dependsOn: "regions",
               },
               {
                  kind: "measure",
                  name: "total",
                  code: "amount.sum()",
               },
            ],
         },
      ]);
      expect(modelText).toContain(
         "join_one: regions is regions on region_id = regions.region_id",
      );
      expect(modelText).toContain("measure: total is amount.sum()");
      // Join must precede the measure so the measure can reference joined fields.
      expect(modelText.indexOf("join_one:")).toBeLessThan(
         modelText.indexOf("measure: total"),
      );
   });

   it("declares a joined source before the source that joins it (dependency order)", () => {
      // `orders` (joins `regions`) is listed FIRST, but must be emitted after
      // `regions` so the join reference resolves.
      const { modelText } = buildServeShapeModelForBindings([
         {
            sourceName: "orders",
            connectionName: "lake",
            virtualHandle: "o",
            tablePath: "lake.orders",
            schema: [{ name: "region_id", type: "VARCHAR" }],
            refinements: [
               {
                  kind: "join",
                  name: "regions",
                  keyword: "join_one",
                  text: "regions is regions on region_id = regions.region_id",
                  dependsOn: "regions",
               },
            ],
         },
         {
            sourceName: "regions",
            connectionName: "lake",
            virtualHandle: "r",
            tablePath: "lake.regions",
            schema: [{ name: "region_id", type: "VARCHAR" }],
         },
      ]);
      expect(modelText.indexOf("source: regions is")).toBeLessThan(
         modelText.indexOf("source: orders is"),
      );
   });
});

describe("sliceSourceRange", () => {
   const src =
      "line0\nsource: orders is x extend {\n  join_one: r is regions on a = r.a\n}\n";
   it("slices a single-line range (the join declaration)", () => {
      // Recover `r is regions on a = r.a` from line 2.
      expect(
         sliceSourceRange(src, {
            start: { line: 2, character: 12 },
            end: { line: 2, character: 35 },
         }),
      ).toBe("r is regions on a = r.a");
   });
   it("slices a multi-line range", () => {
      expect(
         sliceSourceRange(src, {
            start: { line: 1, character: 8 },
            end: { line: 3, character: 1 },
         }),
      ).toBe("orders is x extend {\n  join_one: r is regions on a = r.a\n}");
   });
   it("returns undefined for an out-of-bounds range (stale source)", () => {
      expect(
         sliceSourceRange("short", {
            start: { line: 0, character: 0 },
            end: { line: 9, character: 0 },
         }),
      ).toBeUndefined();
   });
});

describe("extractJoins", () => {
   const loc = (line: number) => ({
      url: "file:///m.malloy",
      range: {
         start: { line, character: 0 },
         end: { line, character: 20 },
      },
   });
   const ctx = (overrides?: Partial<Parameters<typeof extractJoins>[1]>) => ({
      sourceNameById: new Map([
         ["regions@f", "regions"],
         ["inline@f", "inline_only"],
      ]),
      materializedSourceNames: new Set(["orders", "regions"]),
      liftText: () => "r is regions on region_id = r.region_id",
      ...overrides,
   });

   it("carries a join whose target is materialized, keyword and text set", () => {
      const fields = [
         {
            as: "r",
            name: "duckdb:regions",
            join: "one",
            sourceID: "regions@f",
            location: loc(3),
         },
      ];
      expect(extractJoins(fields, ctx())).toEqual([
         {
            kind: "join",
            name: "r",
            keyword: "join_one",
            text: "r is regions on region_id = r.region_id",
            dependsOn: "regions",
         },
      ]);
   });

   it("skips a join whose target source is not materialized (the gate)", () => {
      const fields = [
         { as: "u", join: "one", sourceID: "unmat@f", location: loc(3) },
      ];
      expect(extractJoins(fields, ctx())).toEqual([]);
   });

   it("skips a join to an anonymous/inline source not in the name map", () => {
      const fields = [
         { as: "z", join: "one", sourceID: "not_in_map@f", location: loc(3) },
      ];
      expect(extractJoins(fields, ctx())).toEqual([]);
   });

   it("skips a join whose declaration text cannot be recovered", () => {
      const fields = [
         { as: "r", join: "one", sourceID: "regions@f", location: loc(3) },
      ];
      expect(extractJoins(fields, ctx({ liftText: () => undefined }))).toEqual(
         [],
      );
   });

   it("maps join relationships to keywords and skips raw fields", () => {
      const map = new Map([["regions@f", "regions"]]);
      const shared = {
         sourceNameById: map,
         materializedSourceNames: new Set(["regions"]),
         liftText: () => "x",
      };
      expect(
         extractJoins(
            [{ join: "many", sourceID: "regions@f", location: loc(1) }],
            shared,
         )[0].keyword,
      ).toBe("join_many");
      expect(
         extractJoins(
            [{ join: "cross", sourceID: "regions@f", location: loc(1) }],
            shared,
         )[0].keyword,
      ).toBe("join_cross");
      // A non-join field (a dimension) is ignored.
      expect(
         extractJoins(
            [{ name: "d", expressionType: "scalar", code: "1+1" }],
            shared,
         ),
      ).toEqual([]);
   });
});

describe("extractViews", () => {
   const liftText = () =>
      "by_region is { group_by: region; aggregate: c is count() }";

   it("carries a turtle field, lifting its declaration text", () => {
      const fields = [
         {
            type: "turtle",
            name: "by_region",
            location: { url: "file:///m", range: {} },
         },
      ];
      expect(extractViews(fields, liftText)).toEqual([
         {
            kind: "view",
            name: "by_region",
            text: "by_region is { group_by: region; aggregate: c is count() }",
         },
      ]);
   });

   it("skips non-turtle fields and unliftable turtles", () => {
      expect(
         extractViews(
            [{ name: "amount", type: "number", expressionType: "scalar" }],
            liftText,
         ),
      ).toEqual([]);
      expect(
         extractViews(
            [
               {
                  type: "turtle",
                  name: "v",
                  location: { url: "file:///m", range: {} },
               },
            ],
            () => undefined,
         ),
      ).toEqual([]);
   });

   it("returns [] for undefined fields", () => {
      expect(extractViews(undefined, liftText)).toEqual([]);
   });
});

describe("buildServeShapeModelForBindings with a view", () => {
   it("emits the view (verbatim, view: prefixed) after joins and measures", () => {
      const { modelText } = buildServeShapeModelForBindings([
         {
            sourceName: "orders",
            connectionName: "lake",
            virtualHandle: "h",
            tablePath: "lake.orders",
            schema: [{ name: "amount", type: "BIGINT" }],
            refinements: [
               { kind: "measure", name: "total", code: "amount.sum()" },
               {
                  kind: "view",
                  name: "by_amount",
                  text: "by_amount is { group_by: amount; aggregate: total }",
               },
            ],
         },
      ]);
      expect(modelText).toContain(
         "view: by_amount is { group_by: amount; aggregate: total }",
      );
      // The view must come after the measure it references.
      expect(modelText.indexOf("measure: total")).toBeLessThan(
         modelText.indexOf("view: by_amount"),
      );
   });
});

describe("view serve end-to-end (view over a join runs in DuckDB)", () => {
   let connections: FixedConnectionMap;
   let duckdb: DuckDBConnection;

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE v_orders AS " +
            "SELECT 10 AS amount, 'r1' AS region_id " +
            "UNION ALL SELECT 20, 'r2' UNION ALL SELECT 30, 'r1'",
      );
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE v_regions AS " +
            "SELECT 'r1' AS region_id, 'North' AS region_name " +
            "UNION ALL SELECT 'r2', 'South'",
      );
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });

   it("invokes a named view that groups by a joined field, served from storage", async () => {
      const bindings: ServeBinding[] = [
         {
            sourceName: "regions",
            connectionName: "duckdb",
            virtualHandle: "vr",
            tablePath: "v_regions",
            schema: [
               { name: "region_id", type: "VARCHAR" },
               { name: "region_name", type: "VARCHAR" },
            ],
         },
         {
            sourceName: "orders",
            connectionName: "duckdb",
            virtualHandle: "vo",
            tablePath: "v_orders",
            schema: [
               { name: "amount", type: "BIGINT" },
               { name: "region_id", type: "VARCHAR" },
            ],
            refinements: [
               {
                  kind: "join",
                  name: "regions",
                  keyword: "join_one",
                  text: "regions is regions on region_id = regions.region_id",
                  dependsOn: "regions",
               },
               { kind: "measure", name: "total", code: "amount.sum()" },
               {
                  kind: "view",
                  name: "by_region",
                  text: "by_region is { group_by: regions.region_name; aggregate: total }",
               },
            ],
         },
      ];
      const { modelText } = buildServeShapeModelForBindings(bindings);
      const root = "file:///view-e2e/";
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([[`${root}m.malloy`, modelText]]),
         ),
         connections,
      });
      const query = runtime
         .loadModel(new URL(`${root}m.malloy`), {
            importBaseURL: new URL(root),
         })
         .loadQuery("run: orders -> by_region");
      const virtualMap = buildVirtualMap(bindings);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await query.run({ virtualMap } as any);
      const out = result.data.toObject() as {
         region_name: string;
         total: number;
      }[];
      expect(
         Object.fromEntries(out.map((r) => [r.region_name, r.total])),
      ).toEqual({ North: 40, South: 20 });
   });
});
