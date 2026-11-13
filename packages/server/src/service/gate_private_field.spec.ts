// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A row-level gate over a `private` column. The gate is the author's, read
 * where it was declared, so it reads the source's own private fields; the
 * caller's query is compiled against the source as the author hid it, so the
 * caller reads none of them.
 *
 * Every case runs for two callers with different `#(secure)` givens over the
 * same private tenant column, and checks each gets exactly its own rows.
 * Before this change the gate's probe compiled as a caller-level `extend`,
 * which Malloy refuses a private field, so each of these models failed to
 * load (an own gate) or denied every caller (an inherited one).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection, GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { unrecordedAudit, type QueryAudit } from "./access_record";
import { gateOwnScopeModelDef, gateProbeCompiler } from "./authorize";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), `gate-private-field-${process.pid}`);
const PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdb: DuckDBConnection;
const connections = (): Map<string, Connection> =>
   new Map<string, Connection>([["duckdb", duckdb]]);

const SEED = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'acme', 'east', 10, 'c1'), (2, 'acme', 'west', 20, 'c1'),
      (3, 'globex', 'east', 30, 'c2'), (4, 'globex', 'west', 40, 'c2'),
      (5, 'initech', 'east', 50, 'c3')
    ) AS t(id, org_id, region, amount, customer_id)`,
   `CREATE TABLE customers AS SELECT * FROM (VALUES
      ('c1', 'acme', 'Acme'), ('c2', 'globex', 'Globex'), ('c3', 'initech', 'Initech')
    ) AS t(id, org_id, name)`,
];

const VIEWS = `measure: n is count(), total is amount.sum()
  view: by_region is { group_by: region; aggregate: n, total; order_by: region }`;
const PUBLIC = "id, region, amount, customer_id, n, total, by_region";

const MODEL = `##! experimental { givens access_modifiers }

#(secure)
given:
  GROUPS :: string[]
  ROLE :: string

// Own gate; the column is private on the source's own head.
#(access_filter) org_id in $GROUPS
source: orders is duckdb.table('orders') include { private: org_id; public: * } extend {
  ${VIEWS}
}

// Own gate over a private computed dimension, itself over a private column.
#(access_filter) tenant in $GROUPS
source: orders_tenant is duckdb.table('orders') include { private: org_id; public: * } extend {
  private dimension: tenant is lower(org_id)
  ${VIEWS}
}

// Inherited gate: the base's column is public, the extension hides it.
#(access_filter) org_id in $GROUPS
source: orders_open is duckdb.table('orders') extend {
  ${VIEWS}
}
source: orders_curated is orders_open include { public: ${PUBLIC}; private: * } extend {}

// The documented idiom: a locked base, a curated extension with its own lock
// and its own row filter over a column \`private: *\` hides.
#(authorize) false
source: orders_raw is duckdb.table('orders') extend {
  ${VIEWS}
}
#(authorize) 'analyst' = $ROLE
#(access_filter) org_id in $GROUPS
source: orders_analyst is orders_raw include { public: ${PUBLIC}; private: * } extend {}

// A gate through a join the source keeps private.
#(access_filter) customer.org_id in $GROUPS
source: orders_by_customer is duckdb.table('orders') extend {
  private join_one: customer is duckdb.table('customers') on customer_id = customer.id
  ${VIEWS}
}

// An extension's own gate over a column its base made private (see the
// decision in the PR): read, since the base and the gate are both the
// author's, and the caller still reads nothing.
source: orders_hidden is duckdb.table('orders') include { private: org_id; public: * } extend {
  ${VIEWS}
}
#(access_filter) org_id in $GROUPS
source: orders_ext is orders_hidden extend {}

source: customers_open is duckdb.table('customers') extend {}
`;

/** A joined source's own private field: not the gated source's to read. */
const JOINED_PRIVATE = `##! experimental { givens access_modifiers }

#(secure)
given: GROUPS :: string[]

source: customers is duckdb.table('customers') include { private: org_id; public: * }

#(access_filter) customer.org_id in $GROUPS
source: o is duckdb.table('orders') extend {
  join_one: customer is customers on customer_id = customer.id
  measure: n is count()
}
`;

type Outcome = { rows: unknown[] } | { refused: string };

let model: Model;

async function run(
   query: string,
   givens?: Record<string, GivenValue>,
   audit: QueryAudit = unrecordedAudit(),
): Promise<Outcome> {
   try {
      const { compactResult } = await model.runQuery(audit, {
         query,
         givens,
      });
      return { rows: compactResult as unknown[] };
   } catch (error) {
      return { refused: (error as Error).constructor.name };
   }
}

beforeAll(async () => {
   await fs.mkdir(PKG_DIR, { recursive: true });
   duckdb = new DuckDBConnection(
      "duckdb",
      path.join(TEST_DIR, "test.duckdb"),
      TEST_DIR,
   );
   for (const statement of SEED) await duckdb.runSQL(statement);
   await fs.writeFile(path.join(PKG_DIR, "model.malloy"), MODEL);
   await fs.writeFile(path.join(PKG_DIR, "joined.malloy"), JOINED_PRIVATE);
   model = await Model.create("pkg", PKG_DIR, "model.malloy", connections());
});

afterAll(async () => {
   try {
      await duckdb.close();
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // ignore cleanup errors
   }
});

const ACME = { GROUPS: ["acme"], ROLE: "analyst" };
const OTHERS = { GROUPS: ["globex", "initech"], ROLE: "analyst" };
const ACME_ROWS = [
   { region: "east", n: 1, total: 10 },
   { region: "west", n: 1, total: 20 },
];
const OTHER_ROWS = [
   { region: "east", n: 2, total: 80 },
   { region: "west", n: 1, total: 40 },
];
const DENIED = { refused: "AccessDeniedError" };

const GATED = [
   "orders",
   "orders_tenant",
   "orders_curated",
   "orders_analyst",
   "orders_by_customer",
   "orders_ext",
];

describe("a row-level gate over a private column", () => {
   for (const source of GATED) {
      it(`gives each caller only its own rows through ${source}`, async () => {
         expect(await run(`run: ${source} -> by_region`, ACME)).toEqual({
            rows: ACME_ROWS,
         });
         expect(await run(`run: ${source} -> by_region`, OTHERS)).toEqual({
            rows: OTHER_ROWS,
         });
      });

      it(`denies a caller who supplies no group through ${source}`, async () => {
         expect(
            await run(`run: ${source} -> by_region`, { ROLE: "analyst" }),
         ).toEqual(DENIED);
         expect(await run(`run: ${source} -> by_region`)).toEqual(DENIED);
      });
   }

   it("still decides the lock before the filter", async () => {
      expect(
         await run("run: orders_analyst -> by_region", {
            GROUPS: ["acme"],
            ROLE: "viewer",
         }),
      ).toEqual(DENIED);
      expect(await run("run: orders_raw -> by_region", ACME)).toEqual(DENIED);
   });

   it("gates a caller's join into the source by the caller's own rows", async () => {
      const query = `run: customers_open extend {
           join_many: o is orders on o.customer_id = id
         } -> { where: o.id != null; group_by: o.region; aggregate: c is o.count(); order_by: region }`;
      expect(await run(query, ACME)).toEqual({
         rows: [
            { region: "east", c: 1 },
            { region: "west", c: 1 },
         ],
      });
      expect(await run(query, OTHERS)).toEqual({
         rows: [
            { region: "east", c: 2 },
            { region: "west", c: 1 },
         ],
      });
   });

   it("writes the access record as for a public column", async () => {
      const audit = unrecordedAudit();
      expect(await run("run: orders -> by_region", ACME, audit)).toEqual({
         rows: ACME_ROWS,
      });
      expect(audit.summary).toMatchObject({
         source: "orders",
         decision: "filtered",
         gates: { authorize: [], accessFilter: ["org_id in $GROUPS"] },
         gateGivenNames: ["GROUPS"],
         secureGivens: { GROUPS: ["acme"] },
      });
   });
});

describe("the gate stays bound to the column it was written against", () => {
   // A caller frees the name and defines its own `org_id` that admits every
   // row; reading the caller's field instead of the author's would leak.
   const redirects = [
      "orders extend { except: org_id; dimension: org_id is 'acme' }",
      "orders extend { except: org_id; private dimension: org_id is 'acme' }",
      "orders extend { except: org_id; internal dimension: org_id is 'acme' }",
      "orders_curated extend { except: org_id; dimension: org_id is 'acme' }",
      "orders_ext extend { except: org_id; dimension: org_id is 'acme' }",
      "orders_tenant extend { except: tenant; dimension: tenant is 'acme' }",
   ];
   for (const shape of redirects) {
      it(`denies ${shape}`, async () => {
         expect(await run(`run: ${shape} -> by_region`, ACME)).toEqual(DENIED);
      });
   }

   it("denies the same redirect through a source the request declares", async () => {
      expect(
         await run(
            `source: mine is orders extend { except: org_id; dimension: org_id is 'acme' }
run: mine -> by_region`,
            ACME,
         ),
      ).toEqual(DENIED);
   });

   it("refuses a rename of the private column outright", async () => {
      const outcome = await run(
         "run: orders extend { rename: o2 is org_id } -> by_region",
         ACME,
      );
      expect(outcome).not.toHaveProperty("rows");
   });
});

describe("a caller still reads no private field", () => {
   const reads: [string, string][] = [
      ["select", "run: orders -> { select: org_id }"],
      ["group_by", "run: orders -> { group_by: org_id }"],
      ["where", "run: orders -> { where: org_id = 'globex'; aggregate: n }"],
      ["an aggregate", "run: orders -> { aggregate: c is count(org_id) }"],
      [
         "nest",
         "run: orders -> { group_by: region; nest: x is { group_by: org_id } }",
      ],
      [
         "an extend dimension",
         "run: orders extend { dimension: o is org_id } -> { group_by: o }",
      ],
      [
         "an extend view",
         "run: orders extend { view: v is { group_by: org_id } } -> v",
      ],
      [
         "an include that widens it",
         "run: orders include { public: org_id } -> { group_by: org_id }",
      ],
      [
         "a caller join",
         "run: customers_open extend { join_many: o is orders on o.customer_id = id } -> { group_by: o.org_id }",
      ],
      [
         "the computed private dimension",
         "run: orders_tenant -> { group_by: tenant }",
      ],
      ["the column under it", "run: orders_tenant -> { group_by: org_id }"],
      [
         "the inherited gate's column",
         "run: orders_curated -> { group_by: org_id }",
      ],
      [
         "the curated idiom's column",
         "run: orders_analyst -> { group_by: org_id }",
      ],
      [
         "the private join",
         "run: orders_by_customer -> { group_by: customer.org_id }",
      ],
      [
         "a public field of the private join",
         "run: orders_by_customer -> { group_by: customer.name }",
      ],
      [
         "the ancestor-private column",
         "run: orders_ext -> { group_by: org_id }",
      ],
      ["raw SQL", "run: duckdb.sql('select org_id from orders')"],
      ["a raw table", "run: duckdb.table('orders') -> { group_by: org_id }"],
   ];
   for (const [name, query] of reads) {
      it(`refuses ${name}`, async () => {
         const outcome = await run(query, ACME);
         expect(outcome).not.toHaveProperty("rows");
      });
   }

   it("leaves the private column out of a wildcard", async () => {
      const outcome = await run(
         "run: orders -> { select: *; order_by: id }",
         ACME,
      );
      expect(outcome).toEqual({
         rows: [
            { id: 1, region: "east", amount: 10, customer_id: "c1" },
            { id: 2, region: "west", amount: 20, customer_id: "c1" },
         ],
      });
   });
});

describe("a private field the gate's own source cannot read", () => {
   it("refuses the model at load: a joined source's private field is that source's own", async () => {
      // The load error surfaces on the first use of the model.
      const joined = await Model.create(
         "pkg",
         PKG_DIR,
         "joined.malloy",
         connections(),
      );
      await expect(
         joined.runQuery(unrecordedAudit(), {
            query: "run: o -> { aggregate: n }",
            givens: { GROUPS: ["acme"] },
         }),
      ).rejects.toThrow(/'org_id' is private/);
   });
});

describe("gateOwnScopeModelDef", () => {
   const def = (fields: object[]) =>
      ({
         contents: {
            s: { type: "table", name: "s", fields },
            other: { type: "table", name: "other", fields: [] },
         },
      }) as never;

   it("makes only the named source's own private fields readable, without touching the model", () => {
      const privateField = {
         name: "org_id",
         type: "string",
         accessModifier: "private",
      };
      const internalField = {
         name: "k",
         type: "string",
         accessModifier: "internal",
      };
      const join = {
         name: "j",
         type: "table",
         join: "one",
         accessModifier: "private",
         fields: [{ name: "x", type: "string", accessModifier: "private" }],
      };
      const modelDef = def([privateField, internalField, join]);
      const before = JSON.stringify(modelDef);
      const lifted = gateOwnScopeModelDef(modelDef, "s") as unknown as {
         contents: Record<string, { fields: Record<string, unknown>[] }>;
      };
      expect(JSON.stringify(modelDef)).toBe(before);
      const [o, k, j] = lifted.contents.s.fields;
      expect(o.accessModifier).toBeUndefined();
      // `internal` is already readable to the probe, and stays as written.
      expect(k.accessModifier).toBe("internal");
      // The join hop is the source's own; what the joined source hides is not.
      expect(j.accessModifier).toBeUndefined();
      expect(
         (j.fields as { accessModifier?: string }[])[0].accessModifier,
      ).toBe("private");
      expect(lifted.contents.other as unknown).toBe(
         (modelDef as { contents: Record<string, unknown> }).contents.other,
      );
   });

   it("is undefined for a source with no private field, or no source", () => {
      expect(
         gateOwnScopeModelDef(def([{ name: "a", type: "string" }]), "s"),
      ).toBeUndefined();
      expect(gateOwnScopeModelDef(def([]), "missing")).toBeUndefined();
   });

   it("falls back to the plain compiler when the runtime cannot be read", () => {
      const compiler = {
         loadQuery: () => ({ getPreparedQuery: async () => 1 }),
      };
      const modelDef = def([
         { name: "org_id", type: "string", accessModifier: "private" },
      ]);
      expect(gateProbeCompiler(compiler, modelDef, "s")).toBe(compiler);
      expect(gateProbeCompiler(compiler, undefined, "s")).toBe(compiler);
   });
});
