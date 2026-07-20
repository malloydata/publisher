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
   buildVirtualMap,
   deriveServeBindings,
   duckdbTypeToMalloy,
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
            connectionName: "lake",
            virtualHandle: "h1",
            tablePath: "analytics.a",
            schema: [],
         },
         {
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

describe("deriveServeBindings", () => {
   it("binds only storage entries, keying the handle on sourceEntityId", () => {
      const bindings = deriveServeBindings({
         se_storage: {
            sourceEntityId: "se_storage",
            physicalTableName: "lake.mz_g003",
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
            physicalTableName: "orders_v1",
            connectionName: "wh",
            realization: "COPY",
            rowCount: null,
         },
         se_noschema: {
            // Storage but no captured schema — skipped (can't declare a shape).
            sourceEntityId: "se_noschema",
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
            connectionName: "lake",
            virtualHandle: "se_storage",
            tablePath: "lake.mz_g003",
            schema: [{ name: "amount", type: "BIGINT" }],
            freshAsOf: "2026-07-20T00:00:00Z",
         },
      ]);
   });
});
