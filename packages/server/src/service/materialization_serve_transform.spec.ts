// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
   buildChainedStorageBuildModel,
   documentFlagLines,
   liftDerivedSources,
   missingPersistedTables,
   reachedPersistedSources,
   documentFlagsForLifts,
   type DerivedSourceLift,
   buildServeShapeModel,
   buildServeShapeModelForBindings,
   extractSourceFilters,
   buildServeShapeTiers,
   NEVER_THINNED,
   type RollupShapeGroup,
   UNREPRODUCIBLE_FILTER,
   buildVirtualMap,
   deriveServeBindings,
   duckdbTypeToMalloy,
   groupAliasesByName,
   extractJoins,
   extractRefinements,
   extractViews,
   narrowSchemaToPublic,
   serveShapeDiagnostics,
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
         destinationName: "lake",
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
         destinationName: "lake",
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
            destinationName: "lake",
            virtualHandle: "h1",
            tablePath: "analytics.a",
            schema: [],
         },
         {
            sourceName: "b",
            destinationName: "lake",
            virtualHandle: "h2",
            tablePath: "b",
            schema: [],
         },
      ]);
      expect(map.get("lake")?.get("h1")).toBe('"analytics"."a"');
      expect(map.get("lake")?.get("h2")).toBe('"b"');
   });

   it("passes an already-quoted path through unchanged (no double-quoting)", () => {
      // Mirrors #904's quoteManifestTablePath: an author-quoted `name=` is
      // canonical SQL and must not be re-quoted into `""foo""`.
      const map = buildVirtualMap([
         {
            sourceName: "a",
            destinationName: "lake",
            virtualHandle: "h",
            tablePath: '"My Table"',
            schema: [],
         },
      ]);
      expect(map.get("lake")?.get("h")).toBe('"My Table"');
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
         destinationName: "duckdb",
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
         destinationName: "does_not_exist",
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
            destinationName: "duckdb",
            virtualHandle: "regions_h",
            tablePath: "regions_phys",
            schema: [
               { name: "region_id", type: "VARCHAR" },
               { name: "region_name", type: "VARCHAR" },
            ],
         },
         {
            sourceName: "orders",
            destinationName: "duckdb",
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

describe("groupAliasesByName", () => {
   const plan = (
      ...pairs: [name: string, address: string][]
   ): { name?: string; sourceEntityId?: string }[] =>
      pairs.map(([name, sourceEntityId]) => ({ name, sourceEntityId }));

   it("groups the sources that share one address, keyed by each of their names", () => {
      const byName = groupAliasesByName(
         plan(["daily", "X"], ["daily_with_avg", "X"], ["monthly", "Y"]),
      );
      expect(byName["daily"]).toEqual(["daily", "daily_with_avg"]);
      expect(byName["daily_with_avg"]).toEqual(["daily", "daily_with_avg"]);
      expect(byName["monthly"]).toEqual(["monthly"]);
   });

   it("drops a name declared at more than one address", () => {
      // Source names are not unique in a package — the wire plan is keyed by
      // sourceID for exactly this reason. `ext` here could mean either table, so
      // it cannot be resolved by name and must alias nothing; picking one by map
      // order would eventually bind one name to two tables, put two
      // `source: ext` declarations in one serve shape, and take the storage tier
      // out for every model in the package.
      const byName = groupAliasesByName(
         plan(["daily", "X"], ["ext", "X"], ["other", "Z"], ["ext", "Z"]),
      );
      expect(byName["ext"]).toBeUndefined();
      // The unambiguous names keep their (now narrower) groups.
      expect(byName["daily"]).toEqual(["daily"]);
      expect(byName["other"]).toEqual(["other"]);
   });

   it("is independent of the order sources arrive in", () => {
      const forward = groupAliasesByName(
         plan(["daily", "X"], ["ext", "X"], ["other", "Z"], ["ext", "Z"]),
      );
      const reversed = groupAliasesByName(
         plan(["ext", "Z"], ["other", "Z"], ["ext", "X"], ["daily", "X"]),
      );
      expect(reversed).toEqual(forward);
   });

   it("skips sources missing a name or an address", () => {
      const byName = groupAliasesByName([
         { name: "daily", sourceEntityId: "X" },
         { name: undefined, sourceEntityId: "X" },
         { name: "nameless", sourceEntityId: undefined },
      ]);
      expect(byName).toEqual({ daily: ["daily"] });
   });
});

describe("deriveServeBindings", () => {
   it("binds only storage entries, keying the handle on sourceEntityId", () => {
      const bindings = deriveServeBindings(
         {
            se_storage: {
               sourceEntityId: "se_storage",
               sourceName: "mz",
               physicalTableName: "mz_g003",
               connectionName: "wh",
               storageDestinationName: "lake",
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
               storageDestinationName: "lake",
               schema: [],
               realization: "COPY",
               rowCount: null,
            },
         },
         {},
      );
      expect(bindings).toEqual([
         {
            sourceName: "mz",
            // An entry with no `origin` describes an authored `#@ persist`
            // source, which is what the wire default means and what every entry
            // written before the field existed can only have been.
            origin: "persist",
            destinationName: "lake",
            virtualHandle: "se_storage",
            tablePath: "lake.mz_g003",
            schema: [{ name: "amount", type: "BIGINT" }],
            freshAsOf: "2026-07-20T00:00:00Z",
            freshnessWindowSeconds: undefined,
            freshnessFallback: undefined,
         },
      ]);
      // The table path is qualified with the destination catalog (attach alias)
      // so the serve reads <store>.<table>, not an unqualified name.
      expect(bindings[0].tablePath).toBe("lake.mz_g003");
   });

   it("binds every source sharing an address, not just the one that built it", () => {
      // A base and its `extend` share a content address and therefore one entry
      // and one table. The extension must not get a table of its own, but it must
      // READ the base's — so both names bind, to the same virtual handle. Binding
      // only `entry.sourceName` left the other silently serving live.
      const entry = {
         sourceEntityId: "se_shared",
         sourceName: "daily",
         physicalTableName: "daily_g001",
         connectionName: "wh",
         storageDestinationName: "lake",
         schema: [{ name: "total_amount", type: "BIGINT" }],
         realization: "COPY" as const,
         rowCount: null,
      };

      const bindings = deriveServeBindings(
         { se_shared: entry },
         { daily: ["daily", "daily_with_avg"] },
      );

      expect(bindings.map((b) => b.sourceName)).toEqual([
         "daily",
         "daily_with_avg",
      ]);
      // One table, one handle: several sources resolving to one virtual table is
      // what the identity-scoped handle is for.
      expect(new Set(bindings.map((b) => b.virtualHandle))).toEqual(
         new Set(["se_shared"]),
      );
      expect(new Set(bindings.map((b) => b.tablePath))).toEqual(
         new Set(["lake.daily_g001"]),
      );
   });

   it("does not duplicate the builder's own name", () => {
      // The builder's name is normally in the address group too, so the naive
      // concatenation would bind it twice and push a duplicate source declaration
      // into the serve model.
      const bindings = deriveServeBindings(
         {
            se_shared: {
               sourceEntityId: "se_shared",
               sourceName: "daily",
               physicalTableName: "daily_g001",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "total_amount", type: "BIGINT" }],
               realization: "COPY",
               rowCount: null,
            },
         },
         { daily: ["daily"] },
      );

      expect(bindings.map((b) => b.sourceName)).toEqual(["daily"]);
   });
   it("binds aliases even when the entry carries a HOST-assigned identity", () => {
      // An instructed build stamps the CALLER's sourceEntityId on its entry
      // (`executeInstructedBuild` treats it as opaque, so a host may derive it any
      // way it likes) while `entries` is keyed by the publisher's content address.
      // Grouping aliases by the entry's id would therefore work only for a host
      // that hashes exactly as the publisher does, and would silently fall back to
      // one-alias routing for any other — the bug this pins. The group is keyed by
      // NAME, which both sides mean the same thing by.
      const bindings = deriveServeBindings(
         {
            // map key = publisher content address; entry id = host's scheme
            "publisher-content-address": {
               sourceEntityId: "host-opaque-id-zzz",
               sourceName: "daily",
               physicalTableName: "daily__shared__g007__tok",
               connectionName: "wh",
               storageDestinationName: "lake",
               schema: [{ name: "total_amount", type: "BIGINT" }],
               realization: "COPY",
               rowCount: null,
            },
         },
         { daily: ["daily", "daily_with_avg"] },
      );

      expect(bindings.map((b) => b.sourceName)).toEqual([
         "daily",
         "daily_with_avg",
      ]);
      // The handle still comes from the entry, so it keeps agreeing with whatever
      // the build wrote into the virtual map.
      expect(new Set(bindings.map((b) => b.virtualHandle))).toEqual(
         new Set(["host-opaque-id-zzz"]),
      );
   });
   it("never lets an alias claim a name another entry OWNS", () => {
      // Two tables. `ext` is an alias of `daily`'s table, and also the source
      // that built its own. Binding it for both would put two `source: ext`
      // declarations in one serve shape — which fails to compile and drops the
      // storage tier for every model in the package, silently, since base-only is
      // the tier the ladder trusts without probing. The owner wins its name.
      const entry = (
         sourceEntityId: string,
         sourceName: string,
         table: string,
      ) => ({
         sourceEntityId,
         sourceName,
         physicalTableName: table,
         connectionName: "wh",
         storageDestinationName: "lake",
         schema: [{ name: "total_amount", type: "BIGINT" }],
         realization: "COPY" as const,
         rowCount: null,
      });

      const bindings = deriveServeBindings(
         {
            X: entry("X", "daily", "daily_t"),
            Z: entry("Z", "ext", "ext_t"),
         },
         // A stale/ambiguous group that still offers `ext` as an alias of X.
         { daily: ["daily", "ext"], ext: ["ext"] },
      );

      expect(bindings.map((b) => b.sourceName).sort()).toEqual([
         "daily",
         "ext",
      ]);
      // `ext` resolves to the table it OWNS, not to daily's.
      const ext = bindings.filter((b) => b.sourceName === "ext");
      expect(ext).toHaveLength(1);
      expect(ext[0].tablePath).toBe("lake.ext_t");
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

   it("skips access-restricted (private/internal) fields — never re-emit a hidden field", () => {
      // The served virtual source carries no access modifiers, so re-declaring a
      // private/internal field would expose over the stored table a field the
      // live path refuses. It must be dropped (the query falls back to live,
      // where the modifier is enforced). The compiler carries accessModifier on
      // the field right beside `code`/`expressionType`.
      const fields = [
         {
            name: "public_dim",
            expressionType: "scalar",
            code: "upper(region)",
         },
         {
            name: "secret_flag",
            expressionType: "scalar",
            code: "amount * 2",
            accessModifier: "private",
         },
         {
            name: "internal_total",
            expressionType: "aggregate",
            code: "amount.sum()",
            accessModifier: "internal",
         },
      ];
      expect(extractRefinements(fields)).toEqual([
         { kind: "dimension", name: "public_dim", code: "upper(region)" },
      ]);
   });
});

describe("buildServeShapeModelForBindings with refinements", () => {
   it("re-declares dimensions/measures as an extend on the virtual base", () => {
      const { modelText } = buildServeShapeModelForBindings([
         {
            sourceName: "daily",
            destinationName: "lake",
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
            destinationName: "lake",
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
            destinationName: "lake",
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
            destinationName: "lake",
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

describe("narrowSchemaToPublic", () => {
   const schema = [
      { name: "id", type: "BIGINT" },
      { name: "ssn", type: "VARCHAR" },
      { name: "amount", type: "BIGINT" },
   ];

   it("drops a captured column the source does not publicly expose (except:)", () => {
      // `ssn` is materialized into the table (getSQL projects it) and captured by
      // DESCRIBE, but the source `except:`s it, so it is absent from the field
      // list and must not be declared on the serve shape.
      const fields = [
         { name: "id" },
         { name: "amount" },
         { name: "amount_x2", code: "amount * 2", expressionType: "scalar" },
      ];
      expect(narrowSchemaToPublic(schema, fields)).toEqual([
         { name: "id", type: "BIGINT" },
         { name: "amount", type: "BIGINT" },
      ]);
   });

   it("drops a captured column whose field carries a non-public access modifier", () => {
      const fields = [
         { name: "id" },
         { name: "ssn", accessModifier: "private" },
         { name: "amount" },
      ];
      expect(narrowSchemaToPublic(schema, fields).map((c) => c.name)).toEqual([
         "id",
         "amount",
      ]);
   });

   it("keeps every column when all are public (no-op for a plain rollup)", () => {
      const fields = [{ name: "id" }, { name: "ssn" }, { name: "amount" }];
      expect(narrowSchemaToPublic(schema, fields)).toEqual(schema);
   });

   it("fails closed to an empty shape when the field list is unavailable", () => {
      // Can't determine the public surface → declare nothing → queries fall back
      // to live rather than risk exposing a hidden column.
      expect(narrowSchemaToPublic(schema, undefined)).toEqual([]);
      expect(narrowSchemaToPublic(schema, [])).toEqual([]);
   });

   it("matches an UPPERCASE captured name and emits the author's spelling", () => {
      // What an upper-folding warehouse (Snowflake, Oracle, Redshift, Teradata)
      // reports from DESCRIBE. An exact match intersects to NOTHING here, and the
      // caller drops an empty-schema binding — so the source leaves the serve
      // shape and every query on it falls back live.
      const captured = [
         { name: "ID", type: "BIGINT" },
         { name: "SSN", type: "VARCHAR" },
         { name: "AMOUNT", type: "BIGINT" },
      ];
      const fields = [{ name: "id" }, { name: "amount" }];
      expect(narrowSchemaToPublic(captured, fields)).toEqual([
         { name: "id", type: "BIGINT" },
         { name: "amount", type: "BIGINT" },
      ]);
   });

   it("still hides a non-public column when the captured name differs in case", () => {
      // The security property must not ride on the case matching: folding decides
      // which PHYSICAL column can match an author field, never which author fields
      // exist. `ssn` is access-restricted, so `SSN` has nothing public to match.
      const captured = [
         { name: "ID", type: "BIGINT" },
         { name: "SSN", type: "VARCHAR" },
      ];
      const fields = [
         { name: "id" },
         { name: "ssn", accessModifier: "private" },
      ];
      expect(narrowSchemaToPublic(captured, fields).map((c) => c.name)).toEqual(
         ["id"],
      );
   });

   it("prefers an EXACT hit over a fold, so a folding dimension cannot steal a column", () => {
      // Malloy permits two fields differing only in case: a `TitleCase` dimension
      // over a `snake_case` column. On a case-PRESERVING warehouse the captured
      // name already is the author's, so folding first let the dimension's name
      // win the physical column — the shape declared the stored column as `Total`,
      // the duplicate against the re-emitted dimension failed the shape down to
      // base-only, and `group_by: Total` served the raw column (1) instead of the
      // computed one (other * 10 = 20). Wrong rows, reported `servedFrom: storage`.
      const captured = [
         { name: "total", type: "BIGINT" },
         { name: "other", type: "BIGINT" },
      ];
      const fields = [
         { name: "total" },
         { name: "other" },
         { name: "Total", code: "other * 10", expressionType: "scalar" },
      ];
      expect(narrowSchemaToPublic(captured, fields)).toEqual([
         { name: "total", type: "BIGINT" },
         { name: "other", type: "BIGINT" },
      ]);
   });

   it("drops a captured column that several author fields fold onto", () => {
      // The upper-folded version of the case above: nothing here can tell which of
      // `total`/`Total` the warehouse's `TOTAL` came from, and guessing is the bug
      // the test above pins. Dropping sends a query touching it to live, where the
      // author's own names still distinguish the two.
      const captured = [{ name: "TOTAL", type: "BIGINT" }];
      const fields = [
         { name: "total" },
         { name: "Total", code: "other * 10", expressionType: "scalar" },
      ];
      expect(narrowSchemaToPublic(captured, fields)).toEqual([]);
   });

   it("emits a case-colliding author field once rather than failing the shape", () => {
      // Two physical columns folding onto one author field: declaring the name
      // twice is a duplicate that fails the whole shape model, which would cost
      // storage serving for every source in the model. First wins.
      const captured = [
         { name: "AMOUNT", type: "BIGINT" },
         { name: "amount", type: "VARCHAR" },
      ];
      expect(narrowSchemaToPublic(captured, [{ name: "amount" }])).toEqual([
         { name: "amount", type: "BIGINT" },
      ]);
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

   it("skips an access-restricted (private/internal) join", () => {
      const fields = [
         {
            as: "r",
            join: "one",
            sourceID: "regions@f",
            location: loc(3),
            accessModifier: "private",
         },
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

   it("skips an access-restricted (private/internal) view", () => {
      const fields = [
         {
            type: "turtle",
            name: "secret_view",
            location: { url: "file:///m", range: {} },
            accessModifier: "internal",
         },
      ];
      expect(extractViews(fields, liftText)).toEqual([]);
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
            destinationName: "lake",
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
            destinationName: "duckdb",
            virtualHandle: "vr",
            tablePath: "v_regions",
            schema: [
               { name: "region_id", type: "VARCHAR" },
               { name: "region_name", type: "VARCHAR" },
            ],
         },
         {
            sourceName: "orders",
            destinationName: "duckdb",
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

describe("buildChainedStorageBuildModel (stack-on-parent transient build model)", () => {
   const parent: ServeBinding = {
      sourceName: "daily_orders",
      destinationName: "lake",
      virtualHandle: "daily_h",
      tablePath: "lake.daily_orders__mabc",
      schema: [
         { name: "order_date", type: "DATE" },
         { name: "total", type: "DOUBLE" },
      ],
   };
   const downstreamDefText =
      "monthly_orders is daily_orders -> {\n" +
      "  group_by: order_month is order_date.month\n" +
      "  aggregate: monthly_total is total.sum()\n}";

   it("rebinds the parent as a virtual source and re-declares the persist-annotated downstream over it", () => {
      const model = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly_orders",
         downstreamDefText,
         destinationName: "lake",
      });
      // Both experiments are enabled (persist + virtual_source).
      expect(model).toContain(
         "##! experimental { persistence virtual_source }",
      );
      // The parent is rebound to a virtual source carrying its captured schema.
      expect(model).toContain("type: daily_orders__shape is {");
      expect(model).toContain(
         "source: daily_orders is lake.virtual('daily_h')::daily_orders__shape",
      );
      // The downstream is persist-annotated (so it surfaces in the transient
      // build plan) and re-declared over the rebound parent with `source: `
      // prepended to the lifted RHS.
      expect(model).toContain("#@ persist storage=lake");
      expect(model).toContain("source: monthly_orders is daily_orders ->");
   });

   it("compiles against DuckDB and its downstream getSQL reads the parent's mapped table", async () => {
      const conn = new DuckDBConnection("lake", ":memory:");
      const model = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly_orders",
         downstreamDefText,
         destinationName: "lake",
      });
      const root = "file:///t3-assemble/";
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, model]])),
         connections: new FixedConnectionMap(new Map([["lake", conn]]), "lake"),
      });
      const compiled = await runtime
         .loadModel(new URL(url), { importBaseURL: new URL(root) })
         .getModel();
      const plan = compiled.getBuildPlan();
      const names = Object.values(plan.sources).map((s) => s.name);
      // Only the downstream is a persist source; the rebound parent is virtual.
      expect(names).toEqual(["monthly_orders"]);
      const downstream = Object.values(plan.sources).find(
         (s) => s.name === "monthly_orders",
      )!;
      const virtualMap = buildVirtualMap([parent]);
      const sql = downstream.getSQL({ virtualMap });
      // The generated SQL reads the parent's content-addressed lake table, not
      // a re-scan of raw — the whole point of stacking on the parent.
      expect(sql).toContain("daily_orders__mabc");
   });

   it("runs the downstream over the parent's stored rows and computes the frozen roll-up", async () => {
      // The end-to-end proof of the mechanism, entirely in-memory (no file, so
      // it is deterministic under full-suite load): a live DuckDB holds the
      // parent's STORED rows, the transient model rebinds it, and running the
      // downstream over it yields Jan = 150+225 = 375, Feb = 99 — a pure
      // function of the parent's rows, never a re-scan of raw.
      const conn = new DuckDBConnection("lake", ":memory:");
      await conn.runSQL(
         'CREATE TABLE "daily_orders__mabc" AS ' +
            "SELECT CAST('2026-01-01' AS DATE) AS order_date, 150.0 AS total " +
            "UNION ALL SELECT CAST('2026-01-02' AS DATE), 225.0 " +
            "UNION ALL SELECT CAST('2026-02-01' AS DATE), 99.0",
      );
      // Bare (unqualified) table path so the virtualMap resolves against the
      // in-memory connection's default catalog rather than a `lake.` catalog.
      const memParent: ServeBinding = {
         ...parent,
         tablePath: "daily_orders__mabc",
      };
      const model = buildChainedStorageBuildModel({
         upstreams: [memParent],
         downstreamName: "monthly_orders",
         downstreamDefText,
         destinationName: "lake",
      });
      const root = "file:///t3-run/";
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, model]])),
         connections: new FixedConnectionMap(new Map([["lake", conn]]), "lake"),
      });
      const query = runtime
         .loadModel(new URL(url), { importBaseURL: new URL(root) })
         .loadQuery(
            "run: monthly_orders -> { select: order_month, monthly_total }",
         );
      const virtualMap = buildVirtualMap([memParent]);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await query.run({ virtualMap } as any);
      const out = result.data.toObject() as {
         order_month: { toISOString(): string } | string;
         monthly_total: number;
      }[];
      const byMonth = Object.fromEntries(
         out.map((r) => [
            typeof r.order_month === "string"
               ? r.order_month.slice(0, 7)
               : r.order_month.toISOString().slice(0, 7),
            Number(r.monthly_total),
         ]),
      );
      expect(byMonth).toEqual({ "2026-01": 375, "2026-02": 99 });
   });
});

describe("buildChainedStorageBuildModel with intermediates and the author's flags", () => {
   const parent: ServeBinding = {
      sourceName: "daily",
      destinationName: "lake",
      virtualHandle: "daily_h",
      tablePath: "daily__mabc",
      schema: [
         { name: "order_date", type: "DATE" },
         { name: "total", type: "DOUBLE" },
      ],
   };
   // A non-persisted source between the downstream and its stored parent: the
   // downstream never names `daily`, so without this in the model it does not
   // compile.
   const wide: DerivedSourceLift = {
      sourceName: "daily_wide",
      base: "daily",
      refinements: [],
      text: "daily_wide is daily -> { select: * } extend {\n  dimension: order_month is order_date.month\n}",
   };
   const downstreamDefText =
      "monthly is daily_wide -> {\n" +
      "  group_by: order_month\n" +
      "  aggregate: monthly_total is total.sum()\n}";

   function compile(model: string, tag: string) {
      const conn = new DuckDBConnection("lake", ":memory:");
      const root = `file:///${tag}/`;
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, model]])),
         connections: new FixedConnectionMap(new Map([["lake", conn]]), "lake"),
      });
      return runtime
         .loadModel(new URL(url), { importBaseURL: new URL(root) })
         .getModel();
   }

   it("emits the intermediates after the parents and before the downstream", () => {
      const model = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText,
         destinationName: "lake",
         derived: [wide],
      });
      const parentAt = model.indexOf("source: daily is lake.virtual");
      const wideAt = model.indexOf("source: daily_wide is daily ->");
      const downstreamAt = model.indexOf("source: monthly is daily_wide ->");
      expect(parentAt).toBeGreaterThan(-1);
      expect(wideAt).toBeGreaterThan(parentAt);
      expect(downstreamAt).toBeGreaterThan(wideAt);
   });

   it("a downstream reading a stored parent only through an intermediate compiles, and its SQL reads the parent's table", async () => {
      const model = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText,
         destinationName: "lake",
         derived: [wide],
      });
      const compiled = await compile(model, "t4-intermediate");
      const downstream = Object.values(compiled.getBuildPlan().sources).find(
         (s) => s.name === "monthly",
      );
      expect(downstream).toBeDefined();
      const sql = downstream!.getSQL({ virtualMap: buildVirtualMap([parent]) });
      expect(sql).toContain("daily__mabc");
   });

   it("without the intermediate the same downstream does not compile — the shape miss the lift exists to remove", async () => {
      const model = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText,
         destinationName: "lake",
      });
      await expect(compile(model, "t4-no-intermediate")).rejects.toThrow(
         /undefined object 'daily_wide'/,
      );
   });

   it("carries the author file's ##! flags, so a declaration compiled under them compiles here too", async () => {
      // `include {}` needs `access_modifiers`; the author's file enabled it.
      const gated: DerivedSourceLift = {
         sourceName: "daily_public",
         base: "daily",
         refinements: [],
         text: "daily_public is daily -> { select: * } include { public: order_date }",
      };
      const def =
         "monthly is daily_public -> { group_by: order_date; aggregate: n is count() }";
      const withFlags = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText: def,
         destinationName: "lake",
         derived: [gated],
         documentFlags: ["##! experimental { persistence, access_modifiers }"],
      });
      expect(
         withFlags.startsWith(
            "##! experimental { persistence, access_modifiers }\n##! experimental { persistence virtual_source }\n",
         ),
      ).toBe(true);
      await compile(withFlags, "t4-flags");
      const withoutFlags = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText: def,
         destinationName: "lake",
         derived: [gated],
      });
      await expect(compile(withoutFlags, "t4-no-flags")).rejects.toThrow(
         /access_modifiers/,
      );
   });
});

describe("buildChainedStorageBuildModel declares the author's givens", () => {
   const parent: ServeBinding = {
      sourceName: "daily",
      destinationName: "lake",
      virtualHandle: "daily_h",
      tablePath: "daily__mabc",
      schema: [
         { name: "order_date", type: "DATE" },
         { name: "region", type: "VARCHAR" },
         { name: "total", type: "DOUBLE" },
      ],
   };
   // The intermediate's text reads a given the downstream never touches.
   const wide: DerivedSourceLift = {
      sourceName: "daily_wide",
      base: "daily",
      refinements: [],
      text:
         "daily_wide is daily -> { select: * } extend {\n" +
         "  dimension: is_focus is pick 'yes' when region = $REGION else 'no'\n}",
   };
   const def =
      "monthly is daily_wide -> { aggregate: grand_total is total.sum() }";

   function compile(model: string, tag: string) {
      const conn = new DuckDBConnection("lake", ":memory:");
      const root = `file:///${tag}/`;
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, model]])),
         connections: new FixedConnectionMap(new Map([["lake", conn]]), "lake"),
      });
      return runtime
         .loadModel(new URL(url), { importBaseURL: new URL(root) })
         .getModel();
   }

   it("a carried intermediate that reads a given compiles when the given is declared, and not otherwise", async () => {
      const declared = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText: def,
         destinationName: "lake",
         derived: [wide],
         givens: [{ name: "REGION", type: "string" }],
      });
      expect(declared).toContain(
         "##! experimental { persistence virtual_source givens }\n",
      );
      expect(declared).toContain("given:\n  REGION :: string\n");
      await compile(declared, "t5-givens");
      const undeclared = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText: def,
         destinationName: "lake",
         derived: [wide],
      });
      await expect(compile(undeclared, "t5-no-givens")).rejects.toThrow(
         /REGION/,
      );
   });

   it("carries the author's default as a source literal", () => {
      const model = buildChainedStorageBuildModel({
         upstreams: [parent],
         downstreamName: "monthly",
         downstreamDefText: def,
         destinationName: "lake",
         givens: [{ name: "ORG_ID", type: "number", defaultText: "1" }],
      });
      expect(model).toContain("given:\n  ORG_ID :: number is 1\n");
   });
});

describe("buildServeShapeModelForBindings carries the author files' flags", () => {
   const binding: ServeBinding = {
      sourceName: "daily",
      destinationName: "lake",
      virtualHandle: "daily_h",
      tablePath: "daily__mabc",
      schema: [
         { name: "order_date", type: "DATE" },
         { name: "total_amount", type: "DOUBLE" },
      ],
   };
   const wrapper: DerivedSourceLift = {
      sourceName: "daily_public",
      base: "daily",
      refinements: [],
      text: "daily_public is daily -> { select: * } include {\n  public:\n    order_date\n    total_amount\n}",
   };

   function compile(model: string, tag: string) {
      const conn = new DuckDBConnection("lake", ":memory:");
      const root = `file:///${tag}/`;
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, model]])),
         connections: new FixedConnectionMap(new Map([["lake", conn]]), "lake"),
      });
      return runtime
         .loadModel(new URL(url), { importBaseURL: new URL(root) })
         .getModel();
   }

   it("a lifted include {} wrapper compiles under its file's flags, and not without them", async () => {
      const flagged = buildServeShapeModelForBindings(
         [binding],
         [],
         [],
         [wrapper],
         ["##! experimental { persistence, access_modifiers }"],
      ).modelText;
      expect(
         flagged.startsWith(
            "##! experimental { persistence, access_modifiers }\n##! experimental.virtual_source\n",
         ),
      ).toBe(true);
      await compile(flagged, "s1-flags");
      const bare = buildServeShapeModelForBindings(
         [binding],
         [],
         [],
         [wrapper],
      ).modelText;
      await expect(compile(bare, "s1-no-flags")).rejects.toThrow(
         /access_modifiers/,
      );
   });

   it("with no flags the text is what it was", () => {
      const { modelText } = buildServeShapeModelForBindings([binding]);
      expect(modelText.startsWith("##! experimental.virtual_source\n")).toBe(
         true,
      );
   });
});

describe("documentFlagsForLifts", () => {
   it("collects each carried file's flags once, in first-seen order", () => {
      const files: Record<string, string> = {
         "file:///a.malloy":
            "##! experimental { persistence, access_modifiers }\nsource: x is y",
         "file:///b.malloy":
            "##! experimental.persistence\n##! experimental { persistence, access_modifiers }\n",
      };
      const flags = documentFlagsForLifts(
         [{ sourceName: "p" }, { sourceName: "q" }, { sourceName: "r" }],
         {
            contents: {
               p: { location: { url: "file:///a.malloy" } },
               q: { location: { url: "file:///b.malloy" } },
               r: undefined,
            },
            fileText: (url) => files[url],
         },
      );
      expect(flags).toEqual([
         "##! experimental { persistence, access_modifiers }",
         "##! experimental.persistence",
      ]);
   });
});

describe("extractRefinements carries ungrouped aggregates as measures", () => {
   it("all(...) over a measure is a measure", () => {
      expect(
         extractRefinements([
            {
               name: "share",
               code: "total.sum() / all(total.sum())",
               expressionType: "ungrouped_aggregate",
            },
            {
               name: "rk",
               code: "rank()",
               expressionType: "aggregate_analytic",
            },
         ]),
      ).toEqual([
         {
            kind: "measure",
            name: "share",
            code: "total.sum() / all(total.sum())",
         },
      ]);
   });
});

describe("documentFlagLines", () => {
   it("returns the ##! lines in order and nothing else", () => {
      const text =
         "// header\n##! experimental.persistence\n\n##! experimental { givens, access_modifiers }\n## materialization.freshness.window = 1h\nsource: a is b\n";
      expect(documentFlagLines(text)).toEqual([
         "##! experimental.persistence",
         "##! experimental { givens, access_modifiers }",
      ]);
   });
});

// A compiled model's `contents`, reduced to what the walk and the lift read.
// sourceIDs are the names with an `@` suffix so a wrong lookup is visible in a
// failure. Which names are persist sources is the PLAN's to say (the
// `isPersisted` argument), never a definition's: `#@ persist` is inherited, a
// plain extension or rename of a persisted source is a persist source sharing
// the base's table, and the compiler copies the base's note onto an extension,
// so no definition can tell an inheritor from the source that wrote the
// annotation — and the design does not ask it to.
const modelId = (name: string) => `${name}@m`;
const tableDef = (name: string) => ({ sourceID: modelId(name), type: "table" });
const persistedDef = (name: string, base: string) => ({
   sourceID: modelId(name),
   type: "query_source",
   persistent: true,
   query: { structRef: modelId(base) },
});
const queryDef = (name: string, base: string, joins: string[] = []) => ({
   sourceID: modelId(name),
   type: "query_source",
   query: {
      structRef: modelId(base),
      pipeline: joins.map((j) => ({ join: "one", sourceID: modelId(j) })),
   },
});
const joinField = (j: string) => ({
   name: j,
   join: "one",
   sourceID: modelId(j),
});
/**
 * An `extend`. Of a persisted base it inherits `persistent: true` and the
 * base's table; `joins` are the ones it adds, `inherited` the base's it carries
 * again on its own field list, as the compiler lays them out.
 */
const extendDef = (
   name: string,
   base: string,
   opts: { joins?: string[]; inherited?: string[]; persistent?: boolean } = {},
) => ({
   sourceID: modelId(name),
   type: "query_source",
   extends: modelId(base),
   ...(opts.persistent ? { persistent: true } : {}),
   fields: [...(opts.inherited ?? []), ...(opts.joins ?? [])].map(joinField),
});
function modelCtx(contents: Record<string, unknown>) {
   const sourceNameById = new Map<string, string>();
   for (const [name, def] of Object.entries(contents)) {
      sourceNameById.set((def as { sourceID: string }).sourceID, name);
   }
   return {
      contents: contents as Parameters<
         typeof reachedPersistedSources
      >[0]["contents"],
      sourceNameById,
   };
}
// The walk's predicate sees a definition's `sourceID`; these fixtures use
// `modelId(name)`, so a name list is the set of ids the plan would hold.
const persistedIn =
   (...names: string[]) =>
   (_name: string, sourceID: string) =>
      names.map(modelId).includes(sourceID);
/** The walk's report of a stop: the model's name for it and its id. */
const stop = (name: string) => ({ name, sourceID: modelId(name) });

describe("liftDerivedSources through an inline parenthesized extend", () => {
   // `hits is (daily extend { join_one: r is regions_kept … }) -> { … }`: the
   // compiler embeds the parenthesized source as the structRef, with `extends`
   // and its join fields but no sourceID of its own.
   const inlineExtend = (base: string, joins: string[]) => ({
      type: "query_source",
      extends: modelId(base),
      fields: joins.map((j) => ({ as: j, join: "one", sourceID: modelId(j) })),
   });
   const hitsDef = (joins: string[]) => ({
      sourceID: modelId("hits"),
      type: "query_source",
      location: {
         url: "file:///m.malloy",
         range: {
            start: { line: 0, character: 0 },
            end: { line: 0, character: 4 },
         },
      },
      query: { structRef: inlineExtend("daily", joins), pipeline: [] },
   });
   const ctx = (contents: Record<string, unknown>, shape: string[]) => ({
      ...modelCtx(contents),
      shapeSourceNames: new Set(shape),
      liftText: () =>
         "hits is (daily extend { join_one: r is regions_kept on true }) -> { group_by: x }",
   });

   it("is carried when the inline base and its joins are all on the shape", () => {
      const lifts = liftDerivedSources(
         ctx(
            {
               daily: persistedDef("daily", "orders"),
               regions_kept: persistedDef("regions_kept", "regions"),
               hits: hitsDef(["regions_kept"]),
            },
            ["daily", "regions_kept"],
         ) as Parameters<typeof liftDerivedSources>[0],
      );
      expect(lifts.map((l) => l.sourceName)).toEqual(["hits"]);
      expect(lifts[0]?.base).toBe("daily");
   });

   it("is left off when a join the inline base declares is not on the shape", () => {
      const lifts = liftDerivedSources(
         ctx(
            {
               daily: persistedDef("daily", "orders"),
               regions: tableDef("regions"),
               hits: hitsDef(["regions"]),
            },
            ["daily"],
         ) as Parameters<typeof liftDerivedSources>[0],
      );
      expect(lifts).toEqual([]);
   });
});

describe("reachedPersistedSources", () => {
   it("stops at the first persist source on each path, through any number of intermediates", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         daily_wide: queryDef("daily_wide", "daily"),
         daily_wider: extendDef("daily_wider", "daily_wide"),
         monthly: persistedDef("monthly", "daily_wider"),
      });
      expect(
         reachedPersistedSources(c, "monthly", persistedIn("daily", "monthly")),
      ).toEqual({
         persisted: [stop("daily")],
         raw: false,
         rawLeaves: [],
         rawVia: [],
         visited: ["daily_wider", "daily_wide", "daily"],
      });
   });

   it("stops at an extension of a persisted source under its own name, leaving the table to the caller", () => {
      // `daily_regional is daily extend { … }` is a persist source in the plan
      // (it inherits the annotation) and shares `daily`'s table. The walk
      // records the name it reached; which table that is, and whether it is
      // present, is answered from the plan's address groups.
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         daily_regional: extendDef("daily_regional", "daily", {
            persistent: true,
         }),
         monthly: persistedDef("monthly", "daily_regional"),
      });
      expect(
         reachedPersistedSources(
            c,
            "monthly",
            persistedIn("daily", "daily_regional", "monthly"),
         ),
      ).toMatchObject({ persisted: [stop("daily_regional")], raw: false });
   });

   it("follows the joins an extension adds, which its table does not hold, and reports raw by them", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         regions: tableDef("regions"),
         daily: persistedDef("daily", "orders"),
         daily_regional: extendDef("daily_regional", "daily", {
            joins: ["regions"],
            persistent: true,
         }),
         monthly: persistedDef("monthly", "daily_regional"),
      });
      expect(
         reachedPersistedSources(
            c,
            "monthly",
            persistedIn("daily", "daily_regional", "monthly"),
         ),
      ).toEqual({
         persisted: [stop("daily_regional")],
         raw: true,
         rawLeaves: ["regions"],
         rawVia: ["daily_regional"],
         visited: ["daily_regional", "regions"],
      });
   });

   it("does not follow a join the extension inherits, which its base's table already answers for", () => {
      // The base's join appears on the extension's field list too; it is the
      // base's to account for, and the base is a stored table.
      const c = modelCtx({
         orders: tableDef("orders"),
         regions: tableDef("regions"),
         daily: {
            ...persistedDef("daily", "orders"),
            fields: [joinField("regions")],
         },
         ext: extendDef("ext", "daily", {
            inherited: ["regions"],
            persistent: true,
         }),
         monthly: persistedDef("monthly", "ext"),
      });
      expect(
         reachedPersistedSources(
            c,
            "monthly",
            persistedIn("daily", "ext", "monthly"),
         ),
      ).toMatchObject({ persisted: [stop("ext")], raw: false });
   });

   it("collects every persist source an intermediate joins", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         sites: persistedDef("sites", "orders"),
         joined: extendDef("joined", "daily", { joins: ["sites"] }),
         monthly: persistedDef("monthly", "joined"),
      });
      expect(
         reachedPersistedSources(
            c,
            "monthly",
            persistedIn("daily", "sites", "monthly"),
         ),
      ).toMatchObject({
         persisted: [stop("daily"), stop("sites")],
         raw: false,
      });
   });

   it("walks into a reference carried as the embedded definition", () => {
      // `monthly is (daily extend { … }) -> { … }`: the inline extension has no
      // name of its own, but its definition is embedded whole and reads `daily`.
      const inline = extendDef("daily_inline", "daily", { persistent: true });
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         monthly: {
            ...persistedDef("monthly", "daily"),
            query: { structRef: { ...inline, sourceID: "anon" } },
         },
      });
      expect(
         reachedPersistedSources(c, "monthly", persistedIn("daily", "monthly")),
      ).toMatchObject({ persisted: [stop("daily")], raw: false });
   });

   it("does not stop at the root, which is persisted by definition", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
      });
      expect(
         reachedPersistedSources(c, "daily", persistedIn("daily")),
      ).toMatchObject({ persisted: [], raw: true, rawLeaves: ["orders"] });
   });

   it("treats a reference with no identity at all as raw", () => {
      const c = modelCtx({
         daily: persistedDef("daily", "orders"),
         monthly: {
            ...persistedDef("monthly", "daily"),
            query: { structRef: 42 },
         },
      });
      expect(
         reachedPersistedSources(c, "monthly", persistedIn("daily", "monthly")),
      ).toMatchObject({ persisted: [], raw: true });
   });
});

describe("missingPersistedTables", () => {
   const groups = {
      daily: ["daily", "daily_regional", "renamed"],
      daily_regional: ["daily", "daily_regional", "renamed"],
      renamed: ["daily", "daily_regional", "renamed"],
   };
   it("a name's table is present when any name sharing it is bound", () => {
      expect(
         missingPersistedTables(["daily_regional"], groups, new Set(["daily"])),
      ).toEqual([]);
   });
   it("and missing when none is", () => {
      expect(
         missingPersistedTables(["daily_regional", "sites"], groups, new Set()),
      ).toEqual(["daily_regional", "sites"]);
   });
   it("a name in no group is its own table", () => {
      expect(
         missingPersistedTables(["sites"], groups, new Set(["sites"])),
      ).toEqual([]);
   });
});

describe("liftDerivedSources carrying a persist source that extends another", () => {
   const contents = {
      orders: tableDef("orders"),
      daily: persistedDef("daily", "orders"),
      daily_regional: extendDef("daily_regional", "daily", {
         persistent: true,
      }),
   };
   const ctx = {
      ...modelCtx(contents),
      shapeSourceNames: new Set(["daily"]),
      liftText: () => "unused",
   };

   it("is left off by default, as the serve path binds it as an alias", () => {
      expect(liftDerivedSources(ctx).map((l) => l.sourceName)).toEqual([]);
   });

   it("is carried over its base when asked, as the base plus what it adds", () => {
      const lifts = liftDerivedSources({
         ...ctx,
         carryPersistExtensions: true,
      });
      expect(lifts.map((l) => [l.sourceName, l.base])).toEqual([
         ["daily_regional", "daily"],
      ]);
   });
});

describe("serveShapeDiagnostics", () => {
   const binding = (sourceName: string): ServeBinding => ({
      sourceName,
      destinationName: "credible",
      virtualHandle: `${sourceName}#h`,
      tablePath: `main.${sourceName}`,
      schema: [{ name: "n", type: "BIGINT" }],
   });

   // Both live failures this exists for produce the SAME compiler error, so the
   // sets are what separate them. Asserted as a pair for that reason: either one
   // alone leaves the two indistinguishable, which is the state this replaces.
   it("distinguishes a source that is not materialized from one withheld as stale", () => {
      const all = [binding("_fact"), binding("rollup")];

      // Nothing stale: everything bound is in the shape. A query naming
      // `wrapper` finds it in neither set -- it carries no `#@ persist`.
      const allFresh = serveShapeDiagnostics(all, all);
      expect(allFresh.shapeSources).toEqual(["_fact", "rollup"]);
      expect(allFresh.staleSources).toEqual([]);

      // `rollup` is bound but past its freshness window, so it is absent from
      // the shape for a reason the compiler's message cannot express.
      const oneStale = serveShapeDiagnostics(all, [binding("_fact")]);
      expect(oneStale.shapeSources).toEqual(["_fact"]);
      expect(oneStale.staleSources).toEqual(["rollup"]);
   });

   it("reports every bound source as stale when the gate withholds them all", () => {
      const all = [binding("a"), binding("b")];
      expect(serveShapeDiagnostics(all, [])).toEqual({
         shapeSources: [],
         staleSources: ["a", "b"],
      });
   });
});

describe("a source's own filters on the serve shape", () => {
   const base = {
      sourceName: "deals",
      destinationName: "lake",
      virtualHandle: "h",
      tablePath: "lake.deals",
      schema: [
         { name: "amount", type: "BIGINT" },
         { name: "is_deleted", type: "BOOLEAN" },
         { name: "is_open", type: "BOOLEAN" },
      ],
   };

   it("emits each filterList entry as its own where:, never combined with and", () => {
      // Combining would reassociate: `(a or b) and c` is not `a or b and c`.
      const { modelText } = buildServeShapeModelForBindings([
         {
            ...base,
            refinements: [
               { kind: "filter", code: "is_open or is_deleted" },
               { kind: "filter", code: "amount > 10" },
            ],
         },
      ]);
      expect(modelText).toContain("where: is_open or is_deleted");
      expect(modelText).toContain("where: amount > 10");
      expect(modelText).not.toContain("and amount > 10");
   });

   it("emits filters after joins and fields, which a where: may reference", () => {
      const { modelText } = buildServeShapeModelForBindings([
         {
            ...base,
            refinements: [
               {
                  kind: "filter",
                  code: "big and regions.region_name = 'North'",
               },
               { kind: "dimension", name: "big", code: "amount > 100" },
               {
                  kind: "join",
                  name: "regions",
                  keyword: "join_one",
                  text: "regions on region_id = regions.region_id",
                  dependsOn: "regions",
               },
            ],
         },
      ]);
      const joinAt = modelText.indexOf("join_one: regions");
      const dimAt = modelText.indexOf("dimension: big");
      const whereAt = modelText.indexOf("where: big and");
      for (const at of [joinAt, dimAt, whereAt]) expect(at).toBeGreaterThan(-1);
      expect(joinAt).toBeLessThan(dimAt);
      expect(dimAt).toBeLessThan(whereAt);
   });

   it("emits filters before views, which are emitted last", () => {
      const { modelText } = buildServeShapeModelForBindings([
         {
            ...base,
            refinements: [
               {
                  kind: "view",
                  name: "by_month",
                  text: "by_month is { group_by: m }",
               },
               { kind: "filter", code: "not is_deleted" },
            ],
         },
      ]);
      const whereAt = modelText.indexOf("where: not is_deleted");
      const viewAt = modelText.indexOf("view: by_month");
      // Assert presence before order: indexOf returns -1 for an absent filter,
      // which would satisfy a bare `toBeLessThan` and let this pass with the
      // filter dropped entirely.
      expect(whereAt).toBeGreaterThan(-1);
      expect(viewAt).toBeGreaterThan(-1);
      expect(whereAt).toBeLessThan(viewAt);
   });

   it("declares no extend block when a source has no filters and no refinements", () => {
      // The no-filter shape must stay byte-identical to what it was, so a package
      // that declares none reads exactly as before.
      const { modelText } = buildServeShapeModelForBindings([base]);
      expect(modelText).not.toContain("extend {");
      expect(modelText).not.toContain("where:");
   });
});

describe("extractSourceFilters", () => {
   it("carries one refinement per entry, in order, with the author's text", () => {
      expect(
         extractSourceFilters([
            { code: "not is_deleted" },
            { code: "is_open" },
         ]),
      ).toEqual([
         { kind: "filter", code: "not is_deleted" },
         { kind: "filter", code: "is_open" },
      ]);
   });

   it("treats an absent filterList as no filters", () => {
      expect(extractSourceFilters(undefined)).toEqual([]);
      expect(extractSourceFilters([])).toEqual([]);
   });

   it("fails closed on an entry whose code is unreadable", () => {
      // Dropping the entry would serve the unfiltered relation. Emitting
      // something that cannot compile withholds the binding instead, and the
      // query serves live.
      const out = extractSourceFilters([{ code: 42 }, {}, null]);
      expect(out).toHaveLength(3);
      expect(out.every((f) => f.code === UNREPRODUCIBLE_FILTER)).toBe(true);
      const { modelText } = buildServeShapeModelForBindings([
         {
            sourceName: "deals",
            destinationName: "lake",
            virtualHandle: "h",
            tablePath: "lake.deals",
            schema: [{ name: "amount", type: "BIGINT" }],
            refinements: out,
         },
      ]);
      expect(modelText).toContain(UNREPRODUCIBLE_FILTER);
   });
});

describe("buildServeShapeTiers", () => {
   // The invariant the whole filter fix rests on. A tier that thins `filter`
   // answers with rows the source excludes — and the floor tier, which is
   // assembled separately from the thinning ladder, is exactly where that
   // reappears when someone adds a rung.
   const oneGroup: RollupShapeGroup[] = [
      { baseSourceName: "orders", members: [] },
   ];
   for (const [label, groups] of [
      ["no rollup groups", [] as RollupShapeGroup[]],
      ["with a rollup group", oneGroup],
   ] as const) {
      it(`keeps every never-thinned kind at every tier (${label})`, () => {
         const tiers = buildServeShapeTiers([...groups]);
         expect(tiers.length).toBeGreaterThan(0);
         for (const [i, tier] of tiers.entries()) {
            for (const kind of NEVER_THINNED) {
               expect(`tier ${i} keeps ${kind}: ${tier.keep.has(kind)}`).toBe(
                  `tier ${i} keeps ${kind}: true`,
               );
            }
         }
      });
   }

   it("adds the group-dropping rungs only when there is a group", () => {
      const without = buildServeShapeTiers([]);
      const with_ = buildServeShapeTiers([
         { baseSourceName: "orders", members: [] },
      ]);
      expect(with_.length).toBe(without.length + 2);
      // The floor drops the groups AND keeps the never-thinned kinds.
      const floor = with_[with_.length - 1];
      expect(floor.groups).toEqual([]);
      for (const kind of NEVER_THINNED) expect(floor.keep.has(kind)).toBe(true);
   });

   it("thins the optional kinds monotonically", () => {
      const tiers = buildServeShapeTiers([]);
      const optional = tiers.map(
         (t) => [...t.keep].filter((k) => !NEVER_THINNED.includes(k)).length,
      );
      for (let i = 1; i < optional.length; i++) {
         expect(optional[i]).toBeLessThanOrEqual(optional[i - 1]);
      }
      expect(optional[optional.length - 1]).toBe(0);
   });
});
