// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
} from "@malloydata/malloy";
import { describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { logger } from "../logger";
import { Model } from "./model";
import { Package } from "./package";

const SEED_SQL = `
CREATE OR REPLACE TABLE orders_daily (customer_id INTEGER, amount INTEGER);
INSERT INTO orders_daily VALUES (1, 10), (1, 20), (2, 30);

CREATE OR REPLACE TABLE orders_monthly (customer_id INTEGER, amount INTEGER);
INSERT INTO orders_monthly VALUES (1, 100), (2, 200), (2, 300), (2, 400);

CREATE OR REPLACE TABLE customers (customer_id INTEGER, segment VARCHAR);
INSERT INTO customers VALUES (1, 'retail'), (2, 'wholesale');

CREATE OR REPLACE TABLE orgs (org_id INTEGER);
INSERT INTO orgs VALUES (1), (1), (2), (2), (2);
`;

const MODEL_TEXT = `##! experimental { composite_sources parameters }

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

source: c3 is compose(
   duckdb.table('orgs') extend { dimension: is_plain is true },
   duckdb.table('orgs') extend { dimension: allowed is org_id = 1 },
   duckdb.table('orgs') extend { dimension: allowed is true }
) extend {
   measure: n is count()
}
source: f3 is c3 extend { where: allowed }

source: by_flag is compose(
   duckdb.table('orders_daily') extend { dimension: is_any is true },
   duckdb.table('orders_daily') extend { dimension: is_large is amount > 15 }
) extend {
   measure: n is count()
}
source: large_flagged is by_flag extend { where: is_large }

source: by_size_unfiltered is compose(
   duckdb.table('orders_daily') extend { dimension: is_any is true },
   duckdb.table('orders_daily') extend { dimension: is_large is true }
) extend {
   measure: n is count()
}
source: large_unfiltered is by_size_unfiltered extend { where: is_large }
source: large_unfiltered_fake is large_unfiltered extend {
   rename: raw_large is is_large
   dimension: is_large is true
}

source: nested is compose(
   compose(
      duckdb.table('orders_daily') extend { dimension: is_daily is true },
      duckdb.table('orders_monthly') extend { dimension: is_monthly is true }
   ),
   duckdb.table('orders_daily') extend { dimension: is_other is true }
) extend {
   measure: n is count()
}
source: nested_monthly is nested extend { where: is_monthly }

source: customer_orders is customers extend {
   join_many: o is by_size on customer_id = o.customer_id
}

source: pt(x::number) is duckdb.table('orders_daily') extend { where: amount > x }
source: pc is compose(
   pt(x is 5) extend { dimension: is_low is true },
   pt(x is 15) extend { dimension: is_high is true }
) extend {
   measure: n is count()
}
source: pc_high is pc extend { where: is_high }
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
   const debug = spyOn(logger, "debug");
   try {
      let served: unknown = "<not served>";
      try {
         served = await count(model, queryText);
      } catch (error) {
         expect(error).toBeInstanceOf(AccessDeniedError);
         expect(
            debug.mock.calls.some(
               (call) =>
                  String(call[0]) ===
                  "Inherited source filter binding check failed; denying",
            ),
         ).toBe(true);
         return;
      }
      throw new Error(`expected a denial, got n = ${String(served)}`);
   } finally {
      debug.mockRestore();
   }
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

   it("still refuses a caller compose(...) around a rebinding", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: compose(orders_fake_monthly, orders_monthly_only) -> { aggregate: n }",
         );
      });
   });

   it("serves a join_one to a composite with a where:", async () => {
      await withModel(async (model) => {
         expect(
            await count(
               model,
               "run: customers extend { join_one: o is orders_monthly_only on customer_id = o.customer_id } -> { aggregate: n is o.amount.sum() }",
            ),
         ).toBe(1000);
      });
   });

   it("still refuses a join_one to a caller compose(...) around a rebinding", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: customers extend { join_one: o is compose(orders_monthly_only extend { rename: raw_monthly is is_monthly; dimension: is_monthly is true }, orders_monthly_only) on customer_id = o.customer_id } -> { aggregate: n is o.amount.sum() }",
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

describe("filter binding: members that define the filtered field differently", () => {
   it("serves the definition of the member the query resolves to", async () => {
      await withModel(async (model) => {
         expect(await count(model, "run: f3 -> { aggregate: n }")).toBe(2);
      });
   });

   it("still refuses a rebinding that copies another member's definition", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: f3 extend { rename: raw_allowed is allowed; dimension: allowed is true } -> { aggregate: n }",
         );
      });
   });

   it("serves a member-defined filter when no member filters its own rows", async () => {
      await withModel(async (model) => {
         expect(
            await count(model, "run: large_flagged -> { aggregate: n }"),
         ).toBe(2);
      });
   });

   it("still refuses a rebinding when no member filters its own rows", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: large_unfiltered_fake -> { aggregate: n }",
         );
      });
   });
});

describe("filter binding: a nested composite", () => {
   it("serves a where: on a field a nested member declares", async () => {
      await withModel(async (model) => {
         expect(
            await count(model, "run: nested_monthly -> { aggregate: n }"),
         ).toBe(4);
      });
   });

   it("still refuses a rebinding of that field", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: nested_monthly extend { rename: raw_monthly is is_monthly; dimension: is_monthly is true } -> { aggregate: n }",
         );
      });
   });
});

describe("filter binding: a composite reached through a join", () => {
   it("serves a member that filters its own rows", async () => {
      await withModel(async (model) => {
         expect(
            await count(
               model,
               "run: customer_orders -> { group_by: o.is_large; aggregate: n is o.count() }",
            ),
         ).toBe(2);
      });
   });

   it("still refuses a caller join that rebinds the column a member's where: reads", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: customers extend { join_many: o is by_size extend { rename: raw_amount is amount; dimension: amount is 100 } on customer_id = o.customer_id } -> { group_by: o.is_large; aggregate: n is o.count() }",
         );
      });
   });
});

describe("filter binding: a composite of parameterized members", () => {
   it("is not served: Malloy drops a member's arguments when resolving it", async () => {
      await withModel(async (model) => {
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: pc_high -> { aggregate: n }",
               {},
               true,
               {},
            ),
         ).rejects.toThrow();
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

   it("names the queried source in the denial, not the member's table", async () => {
      await withModel(async (model) => {
         await expect(
            count(model, "run: large_fake -> { aggregate: n }"),
         ).rejects.toThrow('Access denied for source "large_fake".');
      });
   });

   it("still refuses a rebinding that copies the member's own where:", async () => {
      await withModel(async (model) => {
         await expectDenied(
            model,
            "run: large_only extend { rename: raw_large is is_large; dimension: is_large is true; where: amount > 15 } -> { aggregate: n }",
         );
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
   "public.malloy": `##! experimental { composite_sources }
import "grains.malloy"

source: monthly_public is orders_monthly_only extend { measure: total is amount.sum() }
`,
   "top.malloy": `
import "public.malloy"
`,
};

async function withPackage(
   run: (model: Model) => Promise<void>,
   modelPath = "m.malloy",
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
      const model = pkg.getModel(modelPath);
      expect(model).toBeDefined();
      await run(model!);
   } finally {
      await pkg.getMalloyConfig().releaseConnections();
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

describe("filter binding: a composite imported from another file", () => {
   it("serves a where: declared in a file the queried model only imports indirectly", async () => {
      await withPackage(async (model) => {
         expect(
            await count(model, "run: monthly_public -> { aggregate: n }"),
         ).toBe(4);
      }, "top.malloy");
   });

   it("still refuses a rebinding through that indirect import", async () => {
      await withPackage(async (model) => {
         await expectDenied(
            model,
            "run: monthly_public extend { rename: raw_monthly is is_monthly; dimension: is_monthly is true } -> { aggregate: n }",
         );
      }, "top.malloy");
   });

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
