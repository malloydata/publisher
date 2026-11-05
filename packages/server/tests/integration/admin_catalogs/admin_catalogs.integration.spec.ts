// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admin API's catalog domain against a real Postgres catalog database and
 * the pinned DuckDB + DuckLake. Needs a Postgres superuser to set the database
 * up the way an orchestrator provisions one (an admin role that may act as each
 * catalog's owner role, a schema created empty and owned by that role):
 *
 *   PUBLISHER_TEST_ADMIN_SUPERUSER="host=127.0.0.1 port=5432 user=postgres password=…"
 *
 * Skipped without it.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
   CatalogAdmin,
   CatalogAdminError,
} from "../../../src/service/catalog_admin";
import {
   attachDuckLakeReadOnly,
   recordedCatalogSettings,
   recordedSchemaPath,
} from "../../../src/service/connection";
import { listStorageTables } from "../../../src/service/materialization_build_session";
import type { components } from "../../../src/api";

const SUPERUSER = process.env.PUBLISHER_TEST_ADMIN_SUPERUSER;
const DB = `admin_it_${process.pid}`;
const PASSWORD = "admin-it-secret";

async function superuserSql(sql: string, database = "postgres"): Promise<void> {
   const c = new DuckDBConnection(
      "su",
      ":memory:",
      mkdtempSync(join(tmpdir(), "su-")),
   );
   await c.runSQL("INSTALL postgres; LOAD postgres;");
   await c.runSQL(
      `ATTACH '${SUPERUSER} dbname=${database}' AS su (TYPE postgres)`,
   );
   await c.runSQL(
      `CALL postgres_execute('su', '${sql.replace(/'/g, "''")}', use_transaction => false)`,
   );
   await c.close();
}

async function superuserQuery(
   sql: string,
   database = DB,
): Promise<Record<string, unknown>[]> {
   const c = new DuckDBConnection(
      "su",
      ":memory:",
      mkdtempSync(join(tmpdir(), "su-")),
   );
   await c.runSQL("INSTALL postgres; LOAD postgres;");
   await c.runSQL(
      `ATTACH '${SUPERUSER} dbname=${database}' AS su (TYPE postgres, READ_ONLY)`,
   );
   const r = (await c.runSQL(
      `SELECT * FROM postgres_query('su', '${sql.replace(/'/g, "''")}')`,
   )) as unknown as { rows: Record<string, unknown>[] };
   await c.close();
   return r.rows;
}

/**
 * Where a table's live rows are: in its live data files (and whether every one
 * is encrypted), and in the catalog database as inlined rows.
 */
async function writtenEncrypted(
   catalog: string,
   table: string,
): Promise<{ rows: number; encrypted: boolean; inlined: number }> {
   const tableId = `(select table_id from ${catalog}.ducklake_table where table_name = '${table}' and end_snapshot is null)`;
   const files = (
      await superuserQuery(
         "select coalesce(sum(record_count), 0)::int as n, " +
            "coalesce(bool_and(encryption_key is not null), false) as encrypted " +
            `from ${catalog}.ducklake_data_file where end_snapshot is null and table_id = ${tableId}`,
      )
   )[0] as { n: number; encrypted: boolean };
   let inlined = 0;
   for (const row of await superuserQuery(
      `select table_name from ${catalog}.ducklake_inlined_data_tables where table_id = ${tableId}`,
   )) {
      const counted = await superuserQuery(
         `select count(*)::int as n from ${catalog}."${String(row.table_name)}" where end_snapshot is null`,
      );
      inlined += Number(counted[0]?.n ?? 0);
   }
   return {
      rows: Number(files.n),
      encrypted: files.encrypted === true,
      inlined,
   };
}

async function refusal(p: Promise<unknown>): Promise<CatalogAdminError> {
   const error = await p.then(
      () => undefined,
      (e) => e,
   );
   expect(error).toBeInstanceOf(CatalogAdminError);
   return error as CatalogAdminError;
}

describe.skipIf(!SUPERUSER)(
   "admin catalogs against a real catalog database",
   () => {
      const root = mkdtempSync(join(tmpdir(), "admin-catalogs-"));
      let admin: CatalogAdmin;
      const responses: unknown[] = [];
      const keep = <T>(v: T): T => {
         responses.push(v);
         return v;
      };

      beforeAll(async () => {
         // Postgres 16 split membership into SET and INHERIT; before it, a plain
         // grant already lets the member act as the role.
         const version = Number(
            (
               await superuserQuery(
                  "SELECT current_setting('server_version_num')::int AS server_version_num",
                  "postgres",
               )
            )[0]?.server_version_num,
         );
         const membership =
            version >= 160000 ? " WITH SET TRUE, INHERIT TRUE" : "";
         await superuserSql(`DROP DATABASE IF EXISTS ${DB}`);
         await superuserSql(`CREATE DATABASE ${DB}`);
         for (const stmt of [
            `DROP ROLE IF EXISTS ${DB}_admin`,
            `DROP ROLE IF EXISTS ${DB}_owner`,
            `DROP ROLE IF EXISTS ${DB}_stranger`,
            `CREATE ROLE ${DB}_admin LOGIN PASSWORD '${PASSWORD}'`,
            `CREATE ROLE ${DB}_stranger NOLOGIN`,
            `CREATE ROLE ${DB}_owner NOLOGIN`,
            `GRANT ${DB}_owner TO ${DB}_admin${membership}`,
            `GRANT CREATE, CONNECT ON DATABASE ${DB} TO ${DB}_admin`,
         ]) {
            await superuserSql(stmt);
         }
         // As an orchestrator provisions one: the schema exists, empty, owned by the
         // catalog's own role.
         await superuserSql(
            `CREATE SCHEMA ds_owned AUTHORIZATION ${DB}_owner`,
            DB,
         );
         const host = SUPERUSER!
            .split(/\s+/)
            .filter((kv) => /^(host|port)=/.test(kv))
            .join(" ");
         admin = new CatalogAdmin({
            catalogDatabase: `${host} dbname=${DB} user=${DB}_admin password=${PASSWORD}`,
         });
         mkdirSync(join(root, "ds_owned"), { recursive: true });
         writeFileSync(
            join(root, "ds_owned", "orders.csv"),
            "id,amount\n1,10\n2,20\n3,30\n",
         );
         mkdirSync(join(root, "ds_owned2"), { recursive: true });
         writeFileSync(join(root, "ds_owned2", "x.csv"), "id\n1\n");
      });

      afterAll(async () => {
         rmSync(root, { recursive: true, force: true });
         if (!SUPERUSER) return;
         await superuserSql(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
         await superuserSql(`DROP ROLE IF EXISTS ${DB}_admin`);
         await superuserSql(`DROP ROLE IF EXISTS ${DB}_owner`);
         await superuserSql(`DROP ROLE IF EXISTS ${DB}_stranger`);
      });

      it("finds no catalog in an empty database", async () => {
         expect(keep(await admin.listCatalogs())).toEqual([]);
      });

      it("creates a catalog in a provisioned schema, owned by that schema's role", async () => {
         const created = keep(
            await admin.createCatalog({
               name: "ds_owned",
               dataPath: join(root, "ds_owned"),
               schemas: ["materialized"],
               dataInliningRowLimit: 0,
               fileDeletionGrace: "1 hour",
            }),
         );
         expect(created).toMatchObject({
            name: "ds_owned",
            encrypted: true,
            dataInliningRowLimit: 0,
            fileDeletionGrace: "1 hour",
         });
         expect(created.dataPath.replace(/\/+$/, "")).toBe(
            join(root, "ds_owned"),
         );
         expect(created.schemas).toEqual(
            expect.arrayContaining(["main", "materialized"]),
         );
         const owners = await superuserQuery(
            "select distinct tableowner from pg_tables where schemaname = 'ds_owned'",
         );
         expect(owners).toEqual([{ tableowner: `${DB}_owner` }]);
      });

      it("creates a catalog where no schema exists yet, owned by the configured role", async () => {
         keep(
            await admin.createCatalog({
               name: "ds_standalone",
               dataPath: join(root, "ds_standalone"),
               encrypted: false,
            }),
         );
         const owners = await superuserQuery(
            "select distinct tableowner from pg_tables where schemaname = 'ds_standalone'",
         );
         expect(owners).toEqual([{ tableowner: `${DB}_admin` }]);
         expect((await admin.getCatalog("ds_standalone")).encrypted).toBe(
            false,
         );
      });

      it("refuses a second create of the same catalog", async () => {
         const error = await refusal(
            admin.createCatalog({
               name: "ds_owned",
               dataPath: join(root, "x"),
            }),
         );
         expect([error.status, error.reason]).toEqual([409, "CATALOG_EXISTS"]);
      });

      it("lists both catalogs with what they record", async () => {
         const names = keep(await admin.listCatalogs()).map((c) => c.name);
         expect(names).toEqual(["ds_owned", "ds_standalone"]);
      });

      it("records the data path and the main schema's path a storage secret is scoped to", async () => {
         // What `attachDuckLakeWithMode` reads from the attached catalog to scope
         // a storage secret when the config states no data path: the unit suite
         // stubs these reads, so this is where their SQL meets a real catalog.
         const host = SUPERUSER!
            .split(/\s+/)
            .filter((kv) => /^(host|port)=/.test(kv))
            .join(" ");
         const session = new DuckDBConnection(
            "scope",
            ":memory:",
            mkdtempSync(join(tmpdir(), "scope-")),
         );
         try {
            await attachDuckLakeReadOnly(session, "ds_owned", {
               catalog: {
                  postgresConnection: {
                     connectionString: `${host} dbname=${DB} user=${DB}_admin password=${PASSWORD} options=-crole=${DB}_owner`,
                  },
                  metadataSchema: "ds_owned",
               },
            });
            const settings = await recordedCatalogSettings(
               session,
               "ds_owned",
               "ds_owned",
            );
            expect(settings.data_path.replace(/\/+$/, "")).toBe(
               join(root, "ds_owned"),
            );
            expect(
               await recordedSchemaPath(
                  session,
                  "ds_owned",
                  "ds_owned",
                  "main",
               ),
            ).toEqual({ path: "main/", relative: true });
            expect(
               await recordedSchemaPath(
                  session,
                  "ds_owned",
                  "ds_owned",
                  "nowhere",
               ),
            ).toBeUndefined();
         } finally {
            await session.close();
         }
      });

      it("creates, refuses to recreate, and replaces a table from a file under the data path", async () => {
         const source = {
            uri: join(root, "ds_owned", "orders.csv"),
            format: "csv",
         };
         const first = keep(
            await admin.putTable(
               "ds_owned",
               "main",
               "orders",
               { source },
               true,
            ),
         );
         expect(first.created).toBe(true);
         expect(first.table.columns.map((c) => c.name)).toEqual([
            "id",
            "amount",
         ]);
         const again = await refusal(
            admin.putTable("ds_owned", "main", "orders", { source }, true),
         );
         expect([again.status, again.reason]).toEqual([412, "TABLE_EXISTS"]);
         const replaced = keep(
            await admin.putTable(
               "ds_owned",
               "main",
               "orders",
               { source },
               false,
            ),
         );
         expect(replaced.created).toBe(false);
      });

      it("refuses an object outside the data path, a look-alike sibling prefix included", async () => {
         const error = await refusal(
            admin.putTable(
               "ds_owned",
               "main",
               "x",
               {
                  source: {
                     uri: join(root, "ds_owned2", "x.csv"),
                     format: "csv",
                  },
               },
               false,
            ),
         );
         expect([error.status, error.reason]).toEqual([
            400,
            "OBJECT_OUTSIDE_DATA_PATH",
         ]);
      });

      it("lists and describes the table, and counts its bytes in the catalog", async () => {
         const tables = keep(await admin.listTables("ds_owned", "main"));
         expect(tables.map((t) => t.name)).toEqual(["orders"]);
         expect(tables[0].sizeBytes).toBeGreaterThan(0);
         const table = keep(await admin.getTable("ds_owned", "main", "orders"));
         expect(table.columns.map((c) => c.name)).toEqual(["id", "amount"]);
         expect((await admin.getCatalog("ds_owned")).sizeBytes).toBeGreaterThan(
            0,
         );
      });

      it("copies a table from another catalog of the database, rewriting it into the encrypted one", async () => {
         // The source stands in for a legacy catalog: unencrypted, and owned by
         // a different role than the target.
         mkdirSync(join(root, "ds_standalone"), { recursive: true });
         writeFileSync(
            join(root, "ds_standalone", "legacy.csv"),
            "id,amount\n1,5\n2,6\n",
         );
         await admin.putTable(
            "ds_standalone",
            "main",
            "legacy",
            {
               source: {
                  uri: join(root, "ds_standalone", "legacy.csv"),
                  format: "csv",
               },
            },
            true,
         );
         const source = {
            catalog: "ds_standalone",
            schema: "main",
            table: "legacy",
         };
         const copied = keep(
            await admin.putTable(
               "ds_owned",
               "materialized",
               "legacy",
               { source },
               true,
            ),
         );
         expect(copied.created).toBe(true);
         expect(copied.table).toMatchObject({
            schema: "materialized",
            name: "legacy",
         });
         expect(copied.table.columns.map((c) => c.name)).toEqual([
            "id",
            "amount",
         ]);
         const again = await refusal(
            admin.putTable(
               "ds_owned",
               "materialized",
               "legacy",
               { source },
               true,
            ),
         );
         expect([again.status, again.reason]).toEqual([412, "TABLE_EXISTS"]);
         // DuckLake records the rows it wrote, in the target catalog's own files.
         expect(await writtenEncrypted("ds_owned", "legacy")).toEqual({
            rows: 2,
            encrypted: true,
            inlined: 0,
         });
      });

      it("writes every row to encrypted Parquet in a catalog that records an inlining limit", async () => {
         // The session sets inlining off, but a limit the catalog records wins over
         // that default, so without the flush both of these would stay as rows in
         // the catalog database.
         keep(
            await admin.createCatalog({
               name: "ds_inline",
               dataPath: join(root, "ds_inline"),
               dataInliningRowLimit: 10,
            }),
         );
         mkdirSync(join(root, "ds_inline"), { recursive: true });
         writeFileSync(join(root, "ds_inline", "small.csv"), "id\n1\n2\n3\n");
         await admin.putTable(
            "ds_inline",
            "main",
            "copied",
            {
               source: {
                  catalog: "ds_standalone",
                  schema: "main",
                  table: "legacy",
               },
            },
            true,
         );
         await admin.putTable(
            "ds_inline",
            "main",
            "loaded",
            {
               source: {
                  uri: join(root, "ds_inline", "small.csv"),
                  format: "csv",
               },
            },
            true,
         );
         expect(await writtenEncrypted("ds_inline", "copied")).toEqual({
            rows: 2,
            encrypted: true,
            inlined: 0,
         });
         expect(await writtenEncrypted("ds_inline", "loaded")).toEqual({
            rows: 3,
            encrypted: true,
            inlined: 0,
         });
      });

      it("copies within one catalog, and answers a missing source with SOURCE_NOT_FOUND", async () => {
         const within = keep(
            await admin.putTable(
               "ds_owned",
               "materialized",
               "orders_copy",
               {
                  source: {
                     catalog: "ds_owned",
                     schema: "main",
                     table: "orders",
                  },
               },
               false,
            ),
         );
         expect(within.created).toBe(true);
         for (const source of [
            { catalog: "ds_nowhere", schema: "main", table: "orders" },
            { catalog: "ds_standalone", schema: "nope", table: "legacy" },
            { catalog: "ds_standalone", schema: "main", table: "nope" },
         ]) {
            const error = await refusal(
               admin.putTable(
                  "ds_owned",
                  "materialized",
                  "missing",
                  { source },
                  false,
               ),
            );
            expect([error.status, error.reason]).toEqual([
               404,
               "SOURCE_NOT_FOUND",
            ]);
         }
         expect(
            (await admin.listTables("ds_owned", "materialized")).map(
               (t) => t.name,
            ),
         ).toEqual(["legacy", "orders_copy"]);
      });

      it("refuses to copy an encrypted catalog's table into an unencrypted one", async () => {
         const error = await refusal(
            admin.putTable(
               "ds_standalone",
               "main",
               "leaked",
               {
                  source: {
                     catalog: "ds_owned",
                     schema: "main",
                     table: "orders",
                  },
               },
               false,
            ),
         );
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
         expect(
            (await admin.listTables("ds_standalone", "main")).map(
               (t) => t.name,
            ),
         ).toEqual(["legacy"]);
      });

      it("refuses a source catalog owned by a role it cannot act as, before reading it", async () => {
         await superuserSql(
            `CREATE SCHEMA ds_strange AUTHORIZATION ${DB}_stranger`,
            DB,
         );
         const error = await refusal(
            admin.putTable(
               "ds_owned",
               "materialized",
               "x",
               {
                  source: { catalog: "ds_strange", schema: "main", table: "t" },
               },
               false,
            ),
         );
         expect([error.status, error.reason]).toEqual([
            409,
            "CATALOG_OWNER_UNSUPPORTED",
         ]);
      });

      it("refuses a key the source's shape does not have", async () => {
         const error = await refusal(
            admin.putTable(
               "ds_owned",
               "materialized",
               "x",
               {
                  source: {
                     catalog: "ds_standalone",
                     schema: "main",
                     table: "legacy",
                     snapshot: 5,
                  },
               },
               false,
            ),
         );
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
      });

      it("refuses a source that is both an object and a table", async () => {
         const error = await refusal(
            admin.putTable(
               "ds_owned",
               "materialized",
               "x",
               {
                  source: {
                     catalog: "ds_standalone",
                     schema: "main",
                     table: "legacy",
                     uri: join(root, "ds_owned", "orders.csv"),
                  },
               },
               false,
            ),
         );
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
      });

      it("serves the catalog to the main API from a config with no data path, and refuses to create a missing one", async () => {
         const host = SUPERUSER!
            .split(/\s+/)
            .filter((kv) => /^(host|port)=/.test(kv))
            .join(" ");
         const destination = (schema: string) =>
            ({
               name: "lake",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        connectionString: `${host} dbname=${DB} user=${DB}_admin password=${PASSWORD} options=-crole=${DB}_owner`,
                     },
                     metadataSchema: schema,
                  },
               },
            }) as components["schemas"]["Connection"];
         expect(
            await listStorageTables({
               destinationName: "lake",
               destinationConnection: destination("ds_owned"),
               schemaName: "main",
               environmentPath: "",
            }),
         ).toEqual(["orders"]);
         const missing = await admin
            .putTable(
               "ds_nowhere",
               "main",
               "t",
               { source: { uri: "/x", format: "csv" } },
               false,
            )
            .then(
               () => undefined,
               (e) => e,
            );
         expect((missing as CatalogAdminError).reason).toBe(
            "CATALOG_NOT_FOUND",
         );
      });

      it("changes a setting, drops a table idempotently, and reclaims history", async () => {
         expect(
            keep(
               await admin.updateCatalog("ds_owned", {
                  fileDeletionGrace: "2 hours",
               }),
            ).fileDeletionGrace,
         ).toBe("2 hours");
         await admin.deleteTable("ds_owned", "main", "orders");
         await admin.deleteTable("ds_owned", "main", "orders");
         expect(await admin.listTables("ds_owned", "main")).toEqual([]);
         const snapshots = keep(await admin.listSnapshots("ds_owned", 100));
         expect(snapshots.length).toBeGreaterThan(1);
         const reclaimed = keep(
            await admin.deleteSnapshots("ds_owned", new Date()),
         );
         expect(reclaimed.snapshotsExpired).toBeGreaterThan(0);
         expect((await admin.listSnapshots("ds_owned", 100)).length).toBe(1);
      });

      it("answers a missing catalog, schema or table with its own reason", async () => {
         expect((await refusal(admin.getCatalog("ds_nowhere"))).reason).toBe(
            "CATALOG_NOT_FOUND",
         );
         expect(
            (await refusal(admin.listTables("ds_owned", "nope"))).reason,
         ).toBe("SCHEMA_NOT_FOUND");
         expect(
            (await refusal(admin.getTable("ds_owned", "main", "nope"))).reason,
         ).toBe("TABLE_NOT_FOUND");
      });

      it("never returns a credential", () => {
         const all = JSON.stringify(responses);
         expect(all).not.toContain(PASSWORD);
         expect(all).not.toContain("password");
      });
   },
);
