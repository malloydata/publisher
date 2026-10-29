// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The drill metadata the browser receives, after the server corrects it, on
// the real compiler. The rows each code names are checked end to end, on
// DuckDB, by the SDK's `RowsDialog.drill.compile.spec.ts`.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { API, SingleConnectionRuntime } from "@malloydata/malloy";
import { parseAnnotation } from "@malloydata/malloy-tag";
import { afterAll, describe, expect, it } from "bun:test";
import {
   DRILL_METADATA_CORRECTED,
   wrapWithCorrectDrillMetadata,
} from "./drill_metadata";

const MODEL = `
source: users is duckdb.sql("""
  select 'West' as region_key, TIMESTAMP '2024-03-01 00:00:00' as created_at, 60.0 as amount
""") extend {
  dimension: signup_year is created_at.year
  dimension: big is amount > 50
}
source: orders is duckdb.sql("""
  select 'West' as region, TIMESTAMP '2026-01-15 10:00:00' as created_at, 10.0 as amount
""") extend {
  measure: order_count is count()
  join_one: users is users on region = users.region_key
  view: by_user is { group_by: users.signup_year, users.big, users.region_key; aggregate: order_count }
  view: by_month is { group_by: created_at.month; aggregate: order_count }
  view: staged is { group_by: region; aggregate: order_count } -> { select: region }
}
`;

const connection = new DuckDBConnection("duckdb", ":memory:");
const model = new SingleConnectionRuntime({ connection } as never).loadModel(
   MODEL,
);
afterAll(() => connection.close());

/**
 * The `#(malloy)` tag of the root and of each top-level field, as the server
 * sends them, or as Malloy writes them with `asMalloyWrites`.
 */
async function tags(query: string, asMalloyWrites = false) {
   const result = await model.loadQuery(query).run();
   const wrapped = asMalloyWrites
      ? API.util.wrapResult(result)
      : wrapWithCorrectDrillMetadata(result);
   const read = (annotations?: { value: string }[]) =>
      parseAnnotation(
         (annotations ?? [])
            .map((a) => a.value)
            .filter((v) => v.startsWith("#(malloy) ")),
      ).tag;
   return {
      schema: wrapped.schema,
      root: read(wrapped.annotations),
      field: (name: string) =>
         read(wrapped.schema.fields.find((f) => f.name === name)?.annotations),
   };
}

describe("correctDrillMetadata", () => {
   it("writes a joined source's computed dimension by its joined name", async () => {
      const { field } = await tags("run: orders -> by_user");
      expect(field("signup_year").text("drill_expression", "code")).toBe(
         "users.signup_year",
      );
      expect(field("big").text("drill_expression", "code")).toBe("users.big");
      // A joined column already had its path.
      expect(field("region_key").text("drill_expression", "code")).toBe(
         "users.region_key",
      );
   });

   it("writes an unnamed truncation from what the result says it was", async () => {
      const { field } = await tags(
         "run: orders -> { group_by: users.created_at.year; aggregate: order_count }",
      );
      expect(field("created_at").text("drill_expression", "code")).toBe(
         "users.created_at.year",
      );
   });

   it("writes no code for an unnamed truncation in a view, whose truncation is lost", async () => {
      const { field } = await tags("run: orders -> by_month");
      expect(field("created_at").text("drill_expression", "code")).toBe(
         undefined,
      );
      // Still a dimension to the renderer: only the code is withheld.
      expect(field("created_at").has("calculation")).toBe(false);
      expect(
         (await tags("run: orders -> by_month")).schema.fields.find(
            (f) => f.name === "created_at",
         )?.kind,
      ).toBe("dimension");
   });

   it("leaves the root of a pipeline of more than one stage undrillable", async () => {
      expect((await tags("run: orders -> staged")).root.has("drillable")).toBe(
         false,
      );
      expect(
         (await tags("run: orders -> by_month")).root.has("drillable"),
      ).toBe(true);
   });
});

describe("the stamp a client requires before it offers rows", () => {
   it("is on every result the server sends, and only on those", async () => {
      expect(
         (await tags("run: orders -> by_month")).root.has(
            DRILL_METADATA_CORRECTED,
         ),
      ).toBe(true);
      expect(
         (await tags("run: orders -> staged")).root.has(
            DRILL_METADATA_CORRECTED,
         ),
      ).toBe(true);
      expect(
         (await tags("run: orders -> by_month", true)).root.has(
            DRILL_METADATA_CORRECTED,
         ),
      ).toBe(false);
   });
});

// Each rule in drill_metadata.ts exists because Malloy writes this metadata
// wrong. These pin that it still does. When one fails after a Malloy bump,
// Malloy is fixed: delete the rule it names, and this test with it.
describe("Malloy still writes the metadata the rules correct", () => {
   it("marks the root of a multi-stage pipeline drillable (rule 1)", async () => {
      expect(
         (await tags("run: orders -> staged", true)).root.has("drillable"),
      ).toBe(true);
   });

   it("writes a joined computed dimension's declaration text as its code (rule 2)", async () => {
      const { field } = await tags("run: orders -> by_user", true);
      expect(field("signup_year").text("drill_expression", "code")).toBe(
         "created_at.year",
      );
   });

   it("writes the bare field as an unnamed truncation's code (rule 3)", async () => {
      const { field } = await tags("run: orders -> by_month", true);
      expect(field("created_at").text("drill_expression", "code")).toBe(
         "created_at",
      );
   });
});
