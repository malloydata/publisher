// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { Model } from "./model";
import { Package } from "./package";

const SEED_SQL = `
CREATE OR REPLACE TABLE orders_daily (customer_id INTEGER, amount INTEGER);
INSERT INTO orders_daily VALUES (1, 10), (1, 20), (2, 30);

CREATE OR REPLACE TABLE orders_monthly (customer_id INTEGER, amount INTEGER);
INSERT INTO orders_monthly VALUES (1, 100), (2, 200), (2, 300), (2, 400);

CREATE OR REPLACE TABLE customers (customer_id INTEGER, segment VARCHAR);
INSERT INTO customers VALUES (1, 'retail'), (2, 'wholesale');
`;

const MODEL_TEXT = `##! experimental { composite_sources }

source: customers is duckdb.table('customers') extend { primary_key: customer_id }

source: orders is compose(
   duckdb.table('orders_daily') extend { dimension: is_daily is true },
   duckdb.table('orders_monthly') extend {
      dimension:
         is_monthly is true
         is_retail_customer is customer_id = 1
   }
) extend {
   join_one: customer is customers on customer_id = customer.customer_id
   measure: n is count()
}

source: orders_monthly_only is orders extend { where: is_monthly }
source: orders_retail is orders extend { where: customer.segment = 'retail' }
source: orders_monthly_retail is orders_monthly_only extend {
   where: customer.segment = 'retail'
}
source: orders_retail_customer is orders extend { where: is_retail_customer }

source: by_size is compose(
   duckdb.table('orders_daily') extend { dimension: is_any is true },
   duckdb.table('orders_daily') extend {
      where: amount > 15
      dimension: is_large is true
   }
) extend {
   measure: n is count()
}
source: large_only is by_size extend { where: is_large }
source: large_fake is large_only extend {
   rename: raw_large is is_large
   dimension: is_large is true
}

source: orders_fake_monthly is orders_monthly_only extend {
   rename: raw_monthly is is_monthly
   dimension: is_monthly is true
}
source: orders_shifted_customer is orders_retail extend {
   rename: raw_customer_id is customer_id
   dimension: customer_id is 3 - raw_customer_id
}
source: orders_rebound_dependency is orders_retail_customer extend {
   rename: raw_customer_id is customer_id
   dimension: customer_id is 1
}
`;

async function newDuckdb(): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const stmt of SEED_SQL.trim()
      .split(";")
      .filter((s) => s.trim())) {
      await duckdb.runSQL(stmt.trim() + ";");
   }
   return duckdb;
}

async function withModel(run: (model: Model) => Promise<void>): Promise<void> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fbg-composite-"));
   try {
      fs.writeFileSync(path.join(dir, "m.malloy"), MODEL_TEXT);
      const model = await Model.create(
         "test-pkg",
         dir,
         "m.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      expect(
         (model as unknown as { compilationError?: Error }).compilationError,
      ).toBeUndefined();
      await run(model);
   } finally {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

async function count(model: Model, queryText: string): Promise<unknown> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      queryText,
      {},
      true,
      {},
   );
   const rows = result.compactResult as unknown as Record<string, unknown>[];
   return rows[0]?.n;
}

async function expectDenied(model: Model, queryText: string): Promise<void> {
   let served: unknown = "<not served>";
   try {
      served = await count(model, queryText);
   } catch (error) {
      expect(error).toBeInstanceOf(AccessDeniedError);
      return;
   }
   throw new Error(`expected a denial, got n = ${String(served)}`);
}

describe("filter binding: an inherited where: on a composite source", () => {
   it("serves a where: on a field only the resolved member declares", async () => {
      await withModel(async (model) => {
         expect(
            await count(model, "run: orders_monthly_only -> { aggregate: n }"),
         ).toBe(4);
      });
   });

   it("serves a where: through a join declared on the composite", async () => {
      await withModel(async (model) => {
         expect(
            await count(model, "run: orders_retail -> { aggregate: n }"),
         ).toBe(2);
      });
   });

   it("serves both filters stacked, resolved to the member that has both", async () => {
      await withModel(async (model) => {
         expect(
            await count(
               model,
               "run: orders_monthly_retail -> { aggregate: n }",
            ),
         ).toBe(1);
      });
   });

   it("serves a where: on a member dimension that reads a member column", async () => {
      await withModel(async (model) => {
         expect(
            await count(
               model,
               "run: orders_retail_customer -> { aggregate: n }",
            ),
         ).toBe(1);
      });
   });

   it("still refuses a model extension that rebinds the filtered field", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: orders_fake_monthly -> { aggregate: n }",
         );
      });
   });

   it("still refuses a caller query that rebinds the filtered field", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: orders_monthly_only extend { rename: raw_monthly is is_monthly; dimension: is_monthly is true } -> { aggregate: n }",
         );
      });
   });

   it("still refuses a rebinding of the column a composite join's ON reads", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: orders_shifted_customer -> { aggregate: n }",
         );
      });
   });

   it("still refuses a rebinding of a column the member's dimension reads", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: orders_rebound_dependency -> { aggregate: n }",
         );
      });
   });
});

describe("filter binding: a where: declared on a composite member", () => {
   it("serves a query resolved to a member that filters its own rows", async () => {
      await withModel(async (model) => {
         expect(
            await count(
               model,
               "run: by_size -> { group_by: is_large; aggregate: n }",
            ),
         ).toBe(2);
      });
   });

   it("serves a composite where: over a member that filters its own rows", async () => {
      await withModel(async (model) => {
         expect(await count(model, "run: large_only -> { aggregate: n }")).toBe(
            2,
         );
      });
   });

   it("still refuses a rebinding that moves the query onto the other member", async () => {
      await withModel(async (model) => {
         await expectDenied(model, "run: large_fake -> { aggregate: n }");
      });
   });
});

// The package-load worker compiles with its own DuckDB, so rows are inline.
const DAILY_SQL =
   "select 1 as customer_id, 10 as amount union all select 1, 20 union all select 2, 30";
const MONTHLY_SQL =
   "select 1 as customer_id, 100 as amount union all select 2, 200 " +
   "union all select 2, 300 union all select 2, 400";
const CUSTOMERS_SQL =
   "select 1 as customer_id, 'retail' as segment union all select 2, 'wholesale'";

const PACKAGE_FILES = {
   "facts.malloy": `
source: orders_daily is duckdb.sql("${DAILY_SQL}")
source: orders_monthly is duckdb.sql("${MONTHLY_SQL}")
source: customers is duckdb.sql("${CUSTOMERS_SQL}") extend { primary_key: customer_id }
`,
   "composites.malloy": `##! experimental { composite_sources }
import "facts.malloy"

source: orders is compose(
   orders_daily extend { dimension: is_daily is true },
   orders_monthly extend { dimension: is_monthly is true }
) extend {
   join_one: customer is customers on customer_id = customer.customer_id
   measure: n is count()
}

source: by_size is compose(
   orders_daily extend { dimension: is_any is true },
   orders_daily extend {
      where: amount > 15
      dimension: is_large is true
   }
) extend {
   measure: n is count()
}
`,
   "grains.malloy": `##! experimental { composite_sources }
import "composites.malloy"

source: orders_monthly_only is orders extend { where: is_monthly }
source: orders_retail is orders extend { where: customer.segment = 'retail' }
source: large_only is by_size extend { where: is_large }
`,
   "m.malloy": `
import "grains.malloy"
`,
};

async function withPackage(
   run: (model: Model) => Promise<void>,
): Promise<void> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fbg-composite-pkg-"));
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "test-pkg" }),
   );
   for (const [fileName, text] of Object.entries(PACKAGE_FILES)) {
      fs.writeFileSync(path.join(dir, fileName), text);
   }
   const connections = new FixedConnectionMap(
      new Map<string, Connection>([["duckdb", duckdb]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   const pkg = await Package.create("env", "test-pkg", dir, malloyConfig);
   try {
      const model = pkg.getModel("m.malloy");
      expect(model).toBeDefined();
      await run(model!);
   } finally {
      await pkg.getMalloyConfig().releaseConnections();
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

describe("filter binding: a composite imported from another file", () => {
   it("serves a where: on a field only the resolved member declares", async () => {
      await withPackage(async (model) => {
         expect(
            await count(model, "run: orders_monthly_only -> { aggregate: n }"),
         ).toBe(4);
      });
   });

   it("serves a where: through a join declared on the composite", async () => {
      await withPackage(async (model) => {
         expect(
            await count(model, "run: orders_retail -> { aggregate: n }"),
         ).toBe(2);
      });
   });

   it("serves a composite where: over a member that filters its own rows", async () => {
      await withPackage(async (model) => {
         expect(await count(model, "run: large_only -> { aggregate: n }")).toBe(
            2,
         );
      });
   });

   it("still refuses a caller query that rebinds the filtered field", async () => {
      await withPackage(async (model) => {
         await expectDenied(
            model,
            "run: orders_monthly_only extend { rename: raw_monthly is is_monthly; dimension: is_monthly is true } -> { aggregate: n }",
         );
      });
   });

   it("still refuses a rebinding of the column a composite join's ON reads", async () => {
      await withPackage(async (model) => {
         await expectDenied(
            model,
            "run: orders_retail extend { rename: raw_customer_id is customer_id; dimension: customer_id is 3 - raw_customer_id } -> { aggregate: n }",
         );
      });
   });

   it("still refuses a rebinding that moves the query onto the other member", async () => {
      await withPackage(async (model) => {
         await expectDenied(
            model,
            "run: large_only extend { rename: raw_large is is_large; dimension: is_large is true } -> { aggregate: n }",
         );
      });
   });
});
