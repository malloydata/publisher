// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { BadRequestError } from "../errors";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "givens-integration-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

const SEED_SQL = `
CREATE TABLE IF NOT EXISTS orders (
   order_id INTEGER,
   region VARCHAR,
   order_date DATE
);
INSERT INTO orders VALUES
   (1, 'US', '2024-01-15'),
   (2, 'EU', '2024-02-10'),
   (3, 'APAC', '2024-03-05');
`;

const MODEL_WITH_GIVENS = `
##! experimental.givens

given: region_filter :: string is 'US'
given: cutoff_date :: date is @2024-02-01

source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure: order_count is count()
}
`;

const MODEL_WITHOUT_GIVENS = `
source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure: order_count is count()
}
`;

const MODEL_WITH_ANNOTATED_GIVEN = `
##! experimental.givens

#(doc) Region code, e.g. US, EU
#(label) Region
given: region_filter :: string is 'US'

source: orders is duckdb.table('orders') extend {
   primary_key: order_id
}
`;

const MODEL_WITH_FILTER_GIVENS = `
##! experimental.givens

given: BIG :: filter<boolean> is f''
given: MIN_ID :: filter<number> is f''
given: SINCE :: filter<date> is f''

source: orders is duckdb.table('orders') extend {
   primary_key: order_id
   dimension: big is order_id > 1
   measure: order_count is count()
   view: filtered is {
      where: big ~ $BIG and order_id ~ $MIN_ID and order_date ~ $SINCE
      aggregate: order_count
   }
}
`;

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
   // Each fixture lives in its own file. Tests share `beforeAll` for harness
   // setup but never edit these files at runtime, so no `beforeEach` /
   // `afterEach` cleanup is needed.
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders.malloy"),
      MODEL_WITH_GIVENS,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders_no_givens.malloy"),
      MODEL_WITHOUT_GIVENS,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders_annotated.malloy"),
      MODEL_WITH_ANNOTATED_GIVEN,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders_filter_givens.malloy"),
      MODEL_WITH_FILTER_GIVENS,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "filter_givens.malloynb"),
      `>>>malloy\nimport "orders_filter_givens.malloy"\n>>>malloy\nrun: orders -> filtered`,
      "utf-8",
   );
});

afterAll(async () => {
   try {
      await duckdbConnection.close();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // Ignore cleanup errors
   }
});

function getConnections(): Map<string, Connection> {
   const map = new Map<string, Connection>();
   map.set("duckdb", duckdbConnection);
   return map;
}

describe("givens introspection", () => {
   it("surfaces declared givens on the compiled-model response", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders.malloy",
         getConnections(),
      );

      const compiledModel = await model.getModel();

      expect(compiledModel.givens).toBeDefined();
      expect(compiledModel.givens).toHaveLength(2);

      const byName = new Map(
         (compiledModel.givens ?? []).map((g) => [g.name, g]),
      );
      const region = byName.get("region_filter");
      const cutoff = byName.get("cutoff_date");

      expect(region).toBeDefined();
      expect(region?.type).toBe("string");
      expect(region?.default).toBe("'US'");
      expect(cutoff).toBeDefined();
      expect(cutoff?.type).toBe("date");
      expect(cutoff?.default).toBe("@2024-02-01");
   });

   it("omits default for a given declared without one", async () => {
      await fs.writeFile(
         path.join(TEST_PKG_DIR, "mixed_defaults.malloy"),
         `##! experimental.givens

given: with_default :: string is 'WN'
given: no_default :: string

source: orders is duckdb.table('orders') extend {
   primary_key: order_id
}
`,
      );
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "mixed_defaults.malloy",
         getConnections(),
      );
      const byName = new Map(
         ((await model.getModel()).givens ?? []).map((g) => [g.name, g]),
      );
      expect(byName.get("with_default")?.default).toBe("'WN'");
      expect(byName.get("no_default")?.default).toBeUndefined();
   });

   it("attaches the model-level givens list to every source", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders.malloy",
         getConnections(),
      );

      const sources = model.getSources();
      expect(sources).toBeDefined();
      expect(sources).toHaveLength(1);

      const ordersSource = sources?.[0];
      expect(ordersSource?.name).toBe("orders");
      expect(ordersSource?.givens).toBeDefined();
      expect(ordersSource?.givens).toHaveLength(2);

      const names = (ordersSource?.givens ?? []).map((g) => g.name).sort();
      expect(names).toEqual(["cutoff_date", "region_filter"]);
   });

   it("returns undefined for givens when the model declares none", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders_no_givens.malloy",
         getConnections(),
      );

      const compiledModel = await model.getModel();

      // Absent rather than empty: matches how `sources`/`queries` behave when
      // there are none, and lets OpenAPI clients distinguish "feature
      // unsupported" from "supported but no declarations."
      expect(compiledModel.givens).toBeUndefined();
      expect(model.getSources()?.[0]?.givens).toBeUndefined();
   });

   it("surfaces only `#(...)` annotations, not pragmas or doc comments", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders_annotated.malloy",
         getConnections(),
      );

      const compiledModel = await model.getModel();

      expect(compiledModel.givens).toHaveLength(1);
      const region = compiledModel.givens?.[0];
      expect(region?.name).toBe("region_filter");

      // The given declares two app-route annotations (`#(doc)`, `#(label)`).
      // Only app routes land on the wire; Malloy-reserved routes — the
      // model-level `##!` pragma, plain `#` tags, `#"` doc strings — must
      // not leak onto the given's surface.
      const annotations = region?.annotations ?? [];
      expect(annotations.length).toBeGreaterThanOrEqual(2);
      expect(annotations.some((a) => a.startsWith("##"))).toBe(false);
      expect(annotations.some((a) => a.startsWith('#"'))).toBe(false);
   });
});

describe("filter given values are checked on arrival", () => {
   const run = async (
      givens: Record<string, string>,
      shape: "full" | "compact" = "full",
   ) => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders_filter_givens.malloy",
         getConnections(),
      );
      return model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> filtered",
         undefined,
         undefined,
         givens,
         undefined,
         undefined,
         shape,
      );
   };
   const count = async (givens: Record<string, string>) =>
      (
         JSON.parse((await run(givens, "compact")).serializedResult) as {
            order_count: number;
         }[]
      )[0].order_count;

   it("refuses a value its filter type cannot read, with the parser's reason", async () => {
      await expect(run({ BIG: "asdf" })).rejects.toThrow(
         "Invalid value for given BIG (filter<boolean>): Illegal boolean " +
            "filter 'asdf'. Must be one of true,=true,false,=false,null,none. " +
            "Fix: send a filter<boolean> expression, or leave BIG unset to " +
            "use its default.",
      );
      await expect(run({ MIN_ID: "abc" })).rejects.toThrow(
         /^Invalid value for given MIN_ID \(filter<number>\): Expected /,
      );
      await expect(run({ SINCE: "notadate" })).rejects.toThrow(
         /^Invalid value for given SINCE \(filter<date>\): Expected /,
      );
   });

   it("is a 400, not a compile error", async () => {
      const error = await run({ BIG: "asdf" }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(error).toBeInstanceOf(BadRequestError);
   });

   it("runs every value the filter grammar accepts, and the empty filter", async () => {
      // Orders 1, 2 and 3; `big` is order_id > 1.
      expect(await count({ BIG: "true" })).toBe(2);
      expect(await count({ BIG: "=false" })).toBe(1);
      expect(await count({ BIG: "not true" })).toBe(1);
      expect(await count({ BIG: "" })).toBe(3);
      expect(await count({ MIN_ID: ">= 2" })).toBe(2);
      expect(await count({ SINCE: "2024-02" })).toBe(1);
   });

   it("checks a notebook cell's givens the same way", async () => {
      const notebook = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "filter_givens.malloynb",
         getConnections(),
      );
      await expect(
         notebook.executeNotebookCell(1, undefined, undefined, { BIG: "asdf" }),
      ).rejects.toThrow("Invalid value for given BIG (filter<boolean>)");
   });
});
