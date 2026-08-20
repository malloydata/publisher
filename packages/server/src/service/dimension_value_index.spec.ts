import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../storage/duckdb/schema";
import {
   collectIndexableDimensionsAsync,
   hasIndexAnnotation,
   malloyIdent,
   refreshDimensionValueIndex,
   searchDimensionValues,
   sourceIsProtected,
} from "./dimension_value_index";

describe("dimension value index helpers", () => {
   it("detects an #(index) tag", () => {
      expect(hasIndexAnnotation(["#(index)"])).toBe(true);
      expect(hasIndexAnnotation(["#( index )"])).toBe(true);
      expect(hasIndexAnnotation(["#(doc) category"])).toBe(false);
      expect(hasIndexAnnotation([])).toBe(false);
   });

   it("treats authorize and filter sources as protected", () => {
      expect(sourceIsProtected({ authorize: ["$ROLE = 'x'"] })).toBe(true);
      expect(sourceIsProtected({ filters: [{ name: "org" }] })).toBe(true);
      expect(
         sourceIsProtected({
            annotations: ['#(authorize) "$ROLE = \'x\'"'],
         }),
      ).toBe(true);
      expect(
         sourceIsProtected({
            annotations: ["#(filter) dimension=org type=equal required"],
         }),
      ).toBe(true);
      expect(sourceIsProtected({ annotations: ["#(doc) open"] })).toBe(false);
   });

   it("quotes Malloy identifiers that need it", () => {
      expect(malloyIdent("state")).toBe("state");
      expect(malloyIdent("order status")).toBe("`order status`");
   });
});

describe("dimension value index store", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeEach(async () => {
      process.env.PUBLISHER_DIMENSION_VALUE_INDEX = "lexical";
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dim-value-index-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await initializeSchema(db);
   });

   afterEach(async () => {
      delete process.env.PUBLISHER_DIMENSION_VALUE_INDEX;
      delete process.env.PUBLISHER_DIMENSION_VALUE_CAP;
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("defaults off so search returns nothing without the flag", async () => {
      delete process.env.PUBLISHER_DIMENSION_VALUE_INDEX;
      const hits = await searchDimensionValues({
         db,
         environmentName: "env",
         packageName: "pkg",
         searchText: "ca",
         limit: 10,
      });
      expect(hits).toEqual([]);
   });

   it("caps values and marks truncation, then refreshes by generation", async () => {
      const dim = {
         modelPath: "m.malloy",
         sourceName: "orders",
         dimensionName: "state",
      };
      const first = await refreshDimensionValueIndex({
         db,
         environmentName: "env",
         packageName: "pkg",
         servedRevision: "rev-1",
         dimensions: [dim],
         cap: 2,
         fetchValues: async () => ["CA", "NY", "TX"],
      });
      expect(first.valueCount).toBe(2);
      expect(first.truncatedCount).toBe(1);
      expect(first.generation).toBe(1);

      const hits = await searchDimensionValues({
         db,
         environmentName: "env",
         packageName: "pkg",
         searchText: "c",
         limit: 10,
      });
      expect(hits.map((h) => h.name)).toEqual(["CA"]);
      expect(hits[0].entityId).toBe("dimensional_value:orders:state:CA");
      expect(hits[0].rank).toBe(1);

      const second = await refreshDimensionValueIndex({
         db,
         environmentName: "env",
         packageName: "pkg",
         servedRevision: "rev-2",
         dimensions: [dim],
         cap: 10,
         fetchValues: async () => ["OR"],
      });
      expect(second.generation).toBe(2);
      const after = await searchDimensionValues({
         db,
         environmentName: "env",
         packageName: "pkg",
         searchText: null,
         limit: 10,
      });
      expect(after.map((h) => h.name)).toEqual(["OR"]);
      const leftover = await db.get<{ n: number }>(
         `SELECT count(*) AS n FROM dimension_values WHERE generation = 1`,
      );
      expect(Number(leftover?.n)).toBe(0);
   });

   it("skips protected sources and untagged dimensions when collecting", async () => {
      const pkg = {
         listModels: async () => [{ path: "m.malloy" }],
         getModel: () => ({
            getSources: () => [
               { name: "open", authorize: undefined, filters: undefined },
               { name: "gated", authorize: ["$ROLE = 'x'"] },
            ],
            getSourceInfos: () => [
               {
                  name: "open",
                  annotations: [],
                  schema: {
                     fields: [
                        {
                           kind: "dimension",
                           name: "state",
                           annotations: ["#(index)"],
                        },
                        {
                           kind: "dimension",
                           name: "city",
                           annotations: [],
                        },
                     ],
                  },
               },
               {
                  name: "gated",
                  annotations: ['#(authorize) "$ROLE = \'x\'"'],
                  schema: {
                     fields: [
                        {
                           kind: "dimension",
                           name: "tenant",
                           annotations: ["#(index)"],
                        },
                     ],
                  },
               },
            ],
         }),
      };
      const found = await collectIndexableDimensionsAsync(pkg as never);
      expect(found).toEqual([
         {
            modelPath: "m.malloy",
            sourceName: "open",
            dimensionName: "state",
         },
      ]);
   });
});
