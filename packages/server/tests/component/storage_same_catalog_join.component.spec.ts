// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored table joined to an unstored source on the same DuckLake
 * catalog is served, not answered live.
 *
 * On a managed database the host configures each catalog twice: as a
 * storage destination and as a `managed` DuckLake connection, both named
 * `credible`. A `#(materialize)` source on the connection is built into the
 * destination (the same-catalog build), and its served copy reads it from
 * there; a `join_one` to a source left on the connection crossed
 * connections, so the copy left the join out and every query through it
 * answered live. When the destination and the joined source's connection
 * are one catalog (the build's own check), the copy now reads the joined
 * source through the destination too, as the build does: only while every
 * table it moves passes the managed connection's scope check, is a base
 * table, and nothing it moves carries author SQL. Otherwise the decline
 * stays, naming the join.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the DuckLake catalogs and
 * the Postgres warehouse.
 */

import { DuckDBInstance } from "@duckdb/node-api";
import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

/** Opt-in: a Postgres server the test may create and drop databases on. */
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

/** The Credible shape: a stored source joined to an unstored one, one catalog. */
const SAME = `##! experimental.persistence
source: customer is credible.table('customers') extend {
  primary_key: customer_id
}

#(materialize) storage="credible"
source: kept_orders is credible.table('orders') extend {
  where: status = 'kept'
  join_one: customer with customer_id
}
`;

/**
 * The same, the joined source scoped by its own `where:` and gated by an
 * `#(access_filter)` on a given, which applies where it is entered. The join
 * is an extension's: a build refuses a stored source that joins a gated one.
 */
const FILTERED = `##! experimental { persistence givens }
given: IDS :: number[]

#(access_filter) customer_id in $IDS
source: active_customer is credible.table('customers') extend {
  primary_key: customer_id
  where: active
}

#(materialize) storage="credible"
source: active_orders is credible.table('orders') extend {
  where: status = 'kept' and order_id > 1
}

source: active_orders_x is active_orders extend {
  join_one: active_customer with customer_id
}
`;

/**
 * The joined source gated by an `#(access_filter)` and nothing else, so the
 * join is served: the served answer must be live's under the same givens.
 */
const GATED = `##! experimental { persistence givens }
given: IDS :: number[]

#(access_filter) customer_id in $IDS
source: gated_customer is credible.table('customers') extend {
  primary_key: customer_id
}

#(materialize) storage="credible"
source: gated_orders is credible.table('orders') extend {
  where: status = 'kept'
}

source: gated_orders_x is gated_orders extend {
  join_one: gated_customer with customer_id
}
`;

/** The same join over a Postgres warehouse: another catalog than the destination. */
const CROSS = `##! experimental.persistence
source: wh_customer is wh.table('public.customers') extend {
  primary_key: customer_id
}

#(materialize) storage="lake"
source: wh_orders is wh.table('public.orders') extend {
  where: status = 'kept'
  join_one: wh_customer with customer_id
}
`;

/** A joined source on the catalog that is a view over a schema the connection may not read. */
const VIEW = `##! experimental.persistence
source: viewed_customer is credible.table('customers_v') extend {
  primary_key: customer_id
}

#(materialize) storage="credible"
source: viewed_orders is credible.table('orders') extend {
  where: status = 'kept' and order_id < 6
  join_one: viewed_customer with customer_id
}
`;

/**
 * A join to a source on the catalog that carries a sql_* expression, declared
 * on an extension of the stored source: a build refuses a stored source whose
 * own join carries author SQL, so the extension is how a served copy meets one.
 */
const SQL_EXPR = `##! experimental { persistence sql_functions }
source: tagged_customer is credible.table('customers') extend {
  primary_key: customer_id
  dimension: doubled is sql_number("\${customer_id} * 2")
}

#(materialize) storage="credible"
source: tagged_orders is credible.table('orders') extend {
  where: status = 'kept' and order_id < 5
}

source: tagged_orders_x is tagged_orders extend {
  join_one: tagged_customer with customer_id
}
`;

/** The same, the joined source an .sql() source. */
const SQL_SOURCE = `##! experimental.persistence
source: sql_customer is credible.sql("SELECT customer_id, name FROM customers") extend {
  primary_key: customer_id
}

#(materialize) storage="credible"
source: sql_orders is credible.table('orders') extend {
  where: status = 'kept' and order_id < 4
}

source: sql_orders_x is sql_orders extend {
  join_one: sql_customer with customer_id
}
`;

type Answer = {
   rows: Record<string, unknown>[];
   servedFrom: string | null;
   path: unknown;
};

describe.skipIf(!url)("a stored table joined across one catalog", () => {
   const token = Math.random().toString(36).slice(2, 8);
   const catalogDb = `same_catalog_${token}`;
   const warehouseDb = `same_catalog_wh_${token}`;
   let admin: PostgresConnection;
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let pkg: Package;
   let service: MaterializationService;
   let restore: () => void;
   let pg: {
      host: string;
      port: number;
      userName: string;
      password: string;
   };

   /** The host's own writer on the catalog: seeds main and crm. */
   async function seedCatalog(dataPath: string): Promise<void> {
      const instance = await DuckDBInstance.create(":memory:");
      const w = await instance.connect();
      try {
         await w.run(
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
         );
         await w.run(
            `ATTACH 'ducklake:postgres:host=${pg.host} port=${pg.port} dbname=${catalogDb} user=${pg.userName} password=${pg.password}' AS credible (DATA_PATH '${dataPath}', METADATA_SCHEMA 'lake_meta')`,
         );
         await w.run(
            `CREATE TABLE credible.main.orders AS SELECT * FROM (VALUES
               (1, 10, 'kept'), (2, 10, 'kept'), (3, 20, 'kept'),
               (4, 30, 'kept'), (5, 30, 'returned'), (6, 40, 'kept')
             ) AS t(order_id, customer_id, status)`,
         );
         await w.run(
            `CREATE TABLE credible.main.customers AS SELECT * FROM (VALUES
               (10, 'ann', true), (20, 'bo', true), (30, 'cy', false), (40, 'di', true)
             ) AS t(customer_id, name, active)`,
         );
         // The destination's schema, as the host creates it.
         await w.run("CREATE SCHEMA credible.materialized");
         await w.run("CREATE SCHEMA credible.crm");
         await w.run(
            `CREATE TABLE credible.crm.customers AS SELECT * FROM (VALUES
               (10, 'secret-ann'), (20, 'secret-bo')
             ) AS t(customer_id, name)`,
         );
         await w.run(
            "CREATE VIEW credible.main.customers_v AS SELECT customer_id, name FROM credible.crm.customers",
         );
      } finally {
         w.closeSync();
         instance.closeSync();
      }
   }

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      ({ root, envPath, db, repo } = await openTempStorage("same-catalog-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, PKG),
         {
            "same.malloy": SAME,
            "filtered.malloy": FILTERED,
            "gated.malloy": GATED,
            "cross.malloy": CROSS,
            "view.malloy": VIEW,
            "sql_expr.malloy": SQL_EXPR,
            "sql_source.malloy": SQL_SOURCE,
         },
         { manifest: { name: PKG, description: "fixture" } },
      );
      admin = new PostgresConnection({ name: "admin", connectionString: url! });
      await admin.runSQL(`CREATE DATABASE ${catalogDb}`);
      await admin.runSQL(`CREATE DATABASE ${warehouseDb}`);
      const parsed = new URL(url!);
      pg = {
         host: parsed.hostname,
         port: Number(parsed.port || 5432),
         userName: decodeURIComponent(parsed.username),
         password: decodeURIComponent(parsed.password),
      };
      const warehouse = new PostgresConnection({
         name: "seed",
         connectionString: `postgres://${encodeURIComponent(pg.userName)}:${encodeURIComponent(pg.password)}@${pg.host}:${pg.port}/${warehouseDb}`,
      });
      await warehouse.runSQL(
         `CREATE TABLE public.orders AS SELECT * FROM (VALUES
            (1, 10, 'kept'), (2, 10, 'kept'), (3, 20, 'returned')
          ) AS t(order_id, customer_id, status)`,
      );
      await warehouse.runSQL(
         `CREATE TABLE public.customers AS SELECT * FROM (VALUES
            (10, 'ann'), (20, 'bo')
          ) AS t(customer_id, name)`,
      );
      await warehouse.close();
      const dataPath = path.join(root, "bucket") + "/";
      fs.mkdirSync(dataPath, { recursive: true });
      const lakePath = path.join(root, "lake") + "/";
      fs.mkdirSync(lakePath, { recursive: true });
      await seedCatalog(dataPath);
      const catalog = {
         postgresConnection: { ...pg, databaseName: catalogDb },
         metadataSchema: "lake_meta",
      };
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "credible",
               type: "ducklake",
               managed: true,
               ducklakeConnection: {
                  catalog,
                  storage: { bucketUrl: dataPath },
               },
            } as never,
            {
               name: "wh",
               type: "postgres",
               postgresConnection: { ...pg, databaseName: warehouseDb },
            } as never,
         ],
         [
            {
               name: "credible",
               type: "ducklake",
               ducklakeConnection: {
                  catalog,
                  storage: { bucketUrl: dataPath },
                  destinationSchema: "materialized",
               },
            } as never,
            {
               name: "lake",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: { ...pg, databaseName: catalogDb },
                     metadataSchema: "other_lake_meta",
                  },
                  storage: { bucketUrl: lakePath },
               },
            } as never,
         ],
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   });

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      for (const name of [catalogDb, warehouseDb]) {
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
            .catch(() => undefined);
      }
      await admin?.close().catch(() => undefined);
      if (root) fs.rmSync(root, { recursive: true, force: true });
      restore?.();
   });

   /** Start a run and wait until it is terminal and its worker is gone. */
   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id))!;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   async function ask(
      modelPath: string,
      malloy: string,
      givens?: Record<string, unknown>,
   ): Promise<Answer> {
      const model = pkg.getModel(modelPath)!;
      const result = await model.runQuery(
         unrecordedAudit(),
         { query: malloy, ...(givens ? { givens } : {}) } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      return {
         rows: (result.compactResult as Record<string, unknown>[])
            .slice()
            .sort((x, y) => JSON.stringify(x).localeCompare(JSON.stringify(y))),
         servedFrom: result.servedFrom ?? null,
         path: result.path,
      };
   }

   const JOINED = {
      "filtered.malloy":
         "run: active_orders_x -> { group_by: active_customer.customer_id, active_customer.name; aggregate: n is count() }",
      "same.malloy":
         "run: kept_orders -> { group_by: customer.customer_id, customer.name; aggregate: n is count() }",
      "cross.malloy":
         "run: wh_orders -> { group_by: wh_customer.customer_id, wh_customer.name; aggregate: n is count() }",
      "view.malloy":
         "run: viewed_orders -> { group_by: viewed_customer.customer_id, viewed_customer.name; aggregate: n is count() }",
      "sql_expr.malloy":
         "run: tagged_orders_x -> { group_by: tagged_customer.customer_id, tagged_customer.doubled; aggregate: n is count() }",
      "sql_source.malloy":
         "run: sql_orders_x -> { group_by: sql_customer.customer_id, sql_customer.name; aggregate: n is count() }",
   } as const;
   const PLAIN = {
      "filtered.malloy": "run: active_orders -> { aggregate: n is count() }",
      "same.malloy": "run: kept_orders -> { aggregate: n is count() }",
      "cross.malloy": "run: wh_orders -> { aggregate: n is count() }",
      "view.malloy": "run: viewed_orders -> { aggregate: n is count() }",
      "sql_expr.malloy": "run: tagged_orders_x -> { aggregate: n is count() }",
      "sql_source.malloy": "run: sql_orders_x -> { aggregate: n is count() }",
   } as const;
   type ModelPath = keyof typeof JOINED;
   const live = new Map<string, Answer["rows"]>();
   const GATED_JOIN =
      "run: gated_orders_x -> { group_by: gated_customer.customer_id, gated_customer.name; aggregate: n is count() }";
   const GATED_GIVENS = { IDS: [10, 20] };

   it("answers every query live before anything is built", async () => {
      for (const m of Object.keys(JOINED) as ModelPath[]) {
         for (const q of [JOINED[m], PLAIN[m]]) {
            const answer = await ask(m, q);
            expect([m, answer.servedFrom]).toEqual([m, null]);
            live.set(q, answer.rows);
         }
      }
      const gated = await ask("gated.malloy", GATED_JOIN, GATED_GIVENS);
      expect(gated.servedFrom).toBe(null);
      live.set(GATED_JOIN, gated.rows);
      expect(live.get(JOINED["same.malloy"])).toEqual([
         { customer_id: 10, name: "ann", n: 2 },
         { customer_id: 20, name: "bo", n: 1 },
         { customer_id: 30, name: "cy", n: 1 },
         { customer_id: 40, name: "di", n: 1 },
      ]);
      // The joined source's own `where:` is applied live: customer 30 is
      // inactive, so its kept order joins no customer row.
      expect(live.get(JOINED["filtered.malloy"])).toEqual([
         { customer_id: 10, name: "ann", n: 1 },
         { customer_id: 20, name: "bo", n: 1 },
         { customer_id: 40, name: "di", n: 1 },
         { customer_id: null, name: null, n: 1 },
      ]);
   });

   it("builds every stored source", async () => {
      const run = await build();
      expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
   });

   it("serves a plain query on every stored source", async () => {
      for (const m of Object.keys(PLAIN) as ModelPath[]) {
         const answer = await ask(m, PLAIN[m]);
         expect([m, answer.servedFrom]).toEqual([m, "storage"]);
         expect(answer.rows).toEqual(live.get(PLAIN[m])!);
      }
   });

   it("serves a query through a join to an unstored source on the same catalog, equal to live", async () => {
      const answer = await ask("same.malloy", JOINED["same.malloy"]);
      expect(answer.servedFrom).toBe("storage");
      expect(answer.rows).toEqual(live.get(JOINED["same.malloy"])!);
      // No served-copy warning for the join.
      const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) =>
            w.code === "served-copy-not-served" &&
            (w.message ?? "").includes("`customer`"),
      );
      expect(warnings).toEqual([]);
   });

   it("applies the joined source's own where: and keeps its gate, as live does", async () => {
      // The joined source's own `where:` reads a column no stored table
      // holds, so the row-scope rule answers the query live, as it did; the
      // same-catalog read changes only that the copy compiles the join. The
      // rows are live's.
      const answer = await ask("filtered.malloy", JOINED["filtered.malloy"]);
      expect(answer.rows).toEqual(live.get(JOINED["filtered.malloy"])!);
      // Its gate is its own, as live: entered directly, its access filter
      // applies.
      const gated = await ask(
         "filtered.malloy",
         "run: active_customer -> { group_by: customer_id }",
         { IDS: [20, 30, 40] },
      );
      expect(gated.rows.map((r) => r.customer_id)).toEqual([20, 40]);
   });

   it("serves a join to a gated source on the same catalog, equal to live under the same givens", async () => {
      const answer = await ask("gated.malloy", GATED_JOIN, GATED_GIVENS);
      expect(answer.servedFrom).toBe("storage");
      expect(answer.rows).toEqual(live.get(GATED_JOIN)!);
   });

   it("answers a join to another catalog live, the decline naming the join", async () => {
      const answer = await ask("cross.malloy", JOINED["cross.malloy"]);
      expect(answer.servedFrom).toBe(null);
      expect(answer.rows).toEqual(live.get(JOINED["cross.malloy"])!);
      expect(JSON.stringify(answer.path)).toContain(
         "the join `wh_customer` of `wh_orders` (connection 'wh' into storage destination 'lake')",
      );
   });

   it("answers a join to a view over a schema the connection may not read live", async () => {
      const answer = await ask("view.malloy", JOINED["view.malloy"]);
      expect(answer.servedFrom).toBe(null);
      expect(answer.rows).toEqual(live.get(JOINED["view.malloy"])!);
      expect(JSON.stringify(answer.path)).toContain(
         "the join `viewed_customer` of `viewed_orders` (connection 'credible' into storage destination 'credible')",
      );
   });

   it("answers a join to a source carrying author SQL live, a sql_* expression or an .sql() source", async () => {
      for (const [m, join, source] of [
         ["sql_expr.malloy", "tagged_customer", "tagged_orders_x"],
         ["sql_source.malloy", "sql_customer", "sql_orders_x"],
      ] as const) {
         const answer = await ask(m, JOINED[m]);
         expect([m, answer.servedFrom]).toEqual([m, null]);
         expect(answer.rows).toEqual(live.get(JOINED[m])!);
         expect(JSON.stringify(answer.path)).toContain(
            `the join \`${join}\` of \`${source}\` (connection 'credible' into storage destination 'credible')`,
         );
      }
   });
});
