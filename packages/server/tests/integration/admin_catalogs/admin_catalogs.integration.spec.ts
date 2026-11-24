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
import type { Environment } from "../../../src/service/environment";
import type { LoadedEnvironmentsReader } from "../../../src/service/environment_ports";
import { fakeOf } from "../../../src/test_helpers/store_fakes";
import {
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
} from "../../../src/service/connection";
import {
   recordedCatalogSettings,
   recordedSchemaPaths,
} from "../../../src/service/dialects/ducklake_catalog";
import { listStorageTables } from "../../../src/service/materialization/run/storage_admin";
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

/** Runs SQL on a catalog as the configured role, to set up what the admin API refuses to write: a table in `main`. */
async function rawSql(catalog: string, statements: string[]): Promise<void> {
   const host = SUPERUSER!
      .split(/\s+/)
      .filter((kv) => /^(host|port)=/.test(kv))
      .join(" ");
   const session = new DuckDBConnection(
      "raw",
      ":memory:",
      mkdtempSync(join(tmpdir(), "raw-")),
   );
   try {
      await attachDuckLakeReadWrite(session, catalog, {
         catalog: {
            postgresConnection: {
               connectionString: `${host} dbname=${DB} user=${DB}_admin password=${PASSWORD}`,
            },
            metadataSchema: catalog,
         },
      });
      await session.runSQL("SET ducklake_default_data_inlining_row_limit = 0");
      for (const statement of statements) await session.runSQL(statement);
   } finally {
      await session.close();
   }
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
      let catalogDatabase: string;
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
         catalogDatabase = `${host} dbname=${DB} user=${DB}_admin password=${PASSWORD}`;
         admin = new CatalogAdmin({ catalogDatabase });
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
               schemas: ["materialized", "sales"],
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
               schemas: ["stage"],
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
            const schemaPaths = await recordedSchemaPaths(
               session,
               "ds_owned",
               "ds_owned",
               ["main", "nowhere"],
            );
            expect(schemaPaths.get("main")).toEqual({
               path: "main/",
               relative: true,
            });
            expect(schemaPaths.has("nowhere")).toBe(false);
            // With no names, every live schema is returned.
            const all = await recordedSchemaPaths(
               session,
               "ds_owned",
               "ds_owned",
            );
            expect(all.get("main")).toEqual(schemaPaths.get("main"));
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
               "sales",
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
            admin.putTable("ds_owned", "sales", "orders", { source }, true),
         );
         expect([again.status, again.reason]).toEqual([412, "TABLE_EXISTS"]);
         const replaced = keep(
            await admin.putTable(
               "ds_owned",
               "sales",
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
               "sales",
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

      it("refuses a missing object and a file that is not Parquet as 422 OBJECT_UNREADABLE, writing nothing", async () => {
         writeFileSync(
            join(root, "ds_owned", "broken.parquet"),
            "this is not parquet",
         );
         for (const file of ["broken.parquet", "absent.parquet"]) {
            const error = await refusal(
               admin.putTable(
                  "ds_owned",
                  "sales",
                  "unreadable",
                  {
                     source: {
                        uri: join(root, "ds_owned", file),
                        format: "parquet",
                     },
                  },
                  false,
               ),
            );
            expect([file, error.status, error.reason]).toEqual([
               file,
               422,
               "OBJECT_UNREADABLE",
            ]);
            expect(error.message).toContain(file);
            expect(error.message).not.toContain(root);
            expect(error.detail).toBeUndefined();
         }
         expect(
            (await admin.listTables("ds_owned", "sales")).map((t) => t.name),
         ).toEqual(["orders"]);
      });

      it("lists and describes the table, and counts its bytes in the catalog", async () => {
         const tables = keep(await admin.listTables("ds_owned", "sales"));
         expect(tables.map((t) => t.name)).toEqual(["orders"]);
         expect(tables[0].sizeBytes).toBeGreaterThan(0);
         const table = keep(
            await admin.getTable("ds_owned", "sales", "orders"),
         );
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
            "stage",
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
            schema: "stage",
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
         expect(copied.table.partitionedBy).toEqual([]);
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
               schemas: ["lake"],
            }),
         );
         mkdirSync(join(root, "ds_inline"), { recursive: true });
         writeFileSync(join(root, "ds_inline", "small.csv"), "id\n1\n2\n3\n");
         await admin.putTable(
            "ds_inline",
            "lake",
            "copied",
            {
               source: {
                  catalog: "ds_standalone",
                  schema: "stage",
                  table: "legacy",
               },
            },
            true,
         );
         await admin.putTable(
            "ds_inline",
            "lake",
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
                     schema: "sales",
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
            { catalog: "ds_standalone", schema: "stage", table: "nope" },
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

      it("refuses a table PUT and a load in the schema a storage destination builds into, and allows a DELETE there, a copy from it and every other schema", async () => {
         // The same database as another role, as a destination's own credentials would be.
         const asOwner = catalogDatabase.replace(
            `user=${DB}_admin password=${PASSWORD}`,
            `user=${DB}_owner password=not-the-admins`,
         );
         expect(asOwner).not.toBe(catalogDatabase);
         const lake = (name: string) => ({
            name,
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: { connectionString: asOwner },
                  metadataSchema: "ds_owned",
               },
               destinationSchema: "materialized",
            },
         });
         const over = (destinations: unknown[], connections: unknown[] = []) =>
            new CatalogAdmin(
               { catalogDatabase },
               fakeOf<LoadedEnvironmentsReader>({
                  getLoadedEnvironments: () => [
                     fakeOf<Environment>({
                        getEnvironmentName: () => "prod",
                        listStorageDestinations: () => destinations,
                        listApiConnections: () => connections,
                     }),
                  ],
               }),
            );
         const guarded = over([lake("dest")]);
         const source = {
            uri: join(root, "ds_owned", "orders.csv"),
            format: "csv",
         };
         // What a build wrote, put there without the guard.
         await admin.putTable(
            "ds_owned",
            "materialized",
            "built",
            { source },
            true,
         );
         const names = async (schema: string) =>
            (await admin.listTables("ds_owned", schema)).map((t) => t.name);
         try {
            for (const attempt of [
               () =>
                  guarded.putTable(
                     "ds_owned",
                     "materialized",
                     "mine",
                     { source },
                     true,
                  ),
               () =>
                  guarded.loadTable("ds_owned", "materialized", "mine", {
                     mode: "merge",
                     key: ["id"],
                     objects: [join(root, "ds_owned", "mine.parquet")],
                  }),
            ]) {
               const error = await refusal(attempt());
               expect([error.status, error.reason]).toEqual([
                  409,
                  "SCHEMA_IS_DESTINATION",
               ]);
               expect(error.message).toContain("'dest' of environment 'prod'");
            }
            expect(await names("materialized")).toEqual([
               "built",
               "legacy",
               "orders_copy",
            ]);

            // Another schema of the catalog, `main` included, is not the destination's.
            expect(
               (
                  await guarded.putTable(
                     "ds_owned",
                     "main",
                     "upload",
                     { source },
                     true,
                  )
               ).created,
            ).toBe(true);
            await guarded.deleteTable("ds_owned", "main", "upload");
            expect(await names("main")).toEqual([]);

            // Copying out of the destination's schema is a read of it.
            expect(
               (
                  await guarded.putTable(
                     "ds_owned",
                     "sales",
                     "built_copy",
                     {
                        source: {
                           catalog: "ds_owned",
                           schema: "materialized",
                           table: "built",
                        },
                     },
                     true,
                  )
               ).created,
            ).toBe(true);

            // A connection's `destinationSchema` is not a destination's.
            const plain = over([], [lake("db_x")]);
            expect(
               (
                  await plain.putTable(
                     "ds_owned",
                     "materialized",
                     "plain",
                     { source },
                     true,
                  )
               ).created,
            ).toBe(true);

            // Dropping a build's table is cleanup, so it is allowed.
            await guarded.deleteTable("ds_owned", "materialized", "built");
            expect(await names("materialized")).toEqual([
               "legacy",
               "orders_copy",
               "plain",
            ]);
         } finally {
            await admin.deleteTable("ds_owned", "materialized", "built");
            await admin.deleteTable("ds_owned", "materialized", "plain");
            await admin.deleteTable("ds_owned", "materialized", "mine");
            await admin.deleteTable("ds_owned", "sales", "built_copy");
         }
      });

      it("copies a partitioned table with its partition spec and layout, and reports both", async () => {
         // A legacy catalog's partitioned table, as a `partition=` build writes
         // one: the spec set before the rows, so every file holds one partition.
         await rawSql("ds_standalone", [
            "CREATE TABLE ds_standalone.stage.events (id INTEGER, day DATE, category VARCHAR)",
            "ALTER TABLE ds_standalone.stage.events SET PARTITIONED BY (year(day), category)",
            "INSERT INTO ds_standalone.stage.events VALUES " +
               "(1, DATE '2025-03-01', 'a'), (2, DATE '2026-03-01', 'b'), " +
               "(3, DATE '2025-06-01', 'b'), (4, DATE '2026-06-01', 'a'), " +
               "(5, DATE '2025-09-01', 'a'), (6, DATE '2026-09-01', 'b')",
         ]);
         // Later cases list the legacy stand-in's tables, so it is left as found.
         try {
            const spec = [
               { column: "day", transform: "year" },
               { column: "category", transform: "identity" },
            ];
            expect(
               (await admin.getTable("ds_standalone", "stage", "events"))
                  .partitionedBy,
            ).toEqual(spec);

            const copied = keep(
               await admin.putTable(
                  "ds_owned",
                  "materialized",
                  "events",
                  {
                     source: {
                        catalog: "ds_standalone",
                        schema: "stage",
                        table: "events",
                     },
                  },
                  true,
               ),
            );
            expect(copied.table.partitionedBy).toEqual(spec);
            expect(await writtenEncrypted("ds_owned", "events")).toEqual({
               rows: 6,
               encrypted: true,
               inlined: 0,
            });
            // The layout, not only the spec: one file per (year, category), each
            // recording its value for both keys.
            const files = await superuserQuery(
               "select f.data_file_id::int as file, count(v.partition_key_index)::int as keys " +
                  "from ds_owned.ducklake_data_file f " +
                  "left join ds_owned.ducklake_file_partition_value v on v.data_file_id = f.data_file_id " +
                  "where f.end_snapshot is null and f.table_id = " +
                  "(select table_id from ds_owned.ducklake_table where table_name = 'events' and end_snapshot is null) " +
                  "group by f.data_file_id",
            );
            expect(files).toHaveLength(4);
            expect(files.every((f) => Number(f.keys) === 2)).toBe(true);

            // Onto itself, the partitioned write would replace the table empty
            // before reading it. DuckDB resolves names case-insensitively, so
            // another spelling of the same name is the same table.
            for (const target of ["events", "Events", "EVENTS"]) {
               const self = await refusal(
                  admin.putTable(
                     "ds_owned",
                     "materialized",
                     target,
                     {
                        source: {
                           catalog: "ds_owned",
                           schema: "materialized",
                           table: "events",
                        },
                     },
                     false,
                  ),
               );
               expect([target, self.status, self.reason]).toEqual([
                  target,
                  400,
                  "INVALID_REQUEST",
               ]);
               expect(
                  await writtenEncrypted("ds_owned", "events"),
               ).toMatchObject({ rows: 6 });
            }
         } finally {
            await admin.deleteTable("ds_standalone", "stage", "events");
         }
      });

      it("writes a table to main, copies it into another schema and drops it, and a load refuses main", async () => {
         const source = {
            uri: join(root, "ds_standalone", "legacy.csv"),
            format: "csv",
         };
         const written = keep(
            await admin.putTable(
               "ds_standalone",
               "main",
               "early",
               { source },
               true,
            ),
         );
         expect(written.created).toBe(true);
         expect(written.table).toMatchObject({ schema: "main", name: "early" });
         try {
            const copied = keep(
               await admin.putTable(
                  "ds_standalone",
                  "stage",
                  "migrated",
                  {
                     source: {
                        catalog: "ds_standalone",
                        schema: "main",
                        table: "early",
                     },
                  },
                  true,
               ),
            );
            expect(copied.created).toBe(true);
            expect(copied.table).toMatchObject({
               schema: "stage",
               name: "migrated",
            });
            expect(copied.table.columns.map((c) => c.name)).toEqual([
               "id",
               "amount",
            ]);
            expect(await writtenEncrypted("ds_standalone", "migrated")).toEqual(
               {
                  rows: 2,
                  encrypted: false,
                  inlined: 0,
               },
            );

            const error = await refusal(
               admin.loadTable("ds_standalone", "main", "late", {
                  mode: "merge",
                  key: ["id"],
                  objects: [join(root, "ds_standalone", "late.parquet")],
               }),
            );
            expect([error.status, error.reason]).toEqual([
               400,
               "SCHEMA_RESERVED",
            ]);
            expect(
               (await admin.listTables("ds_standalone", "main")).map(
                  (t) => t.name,
               ),
            ).toEqual(["early"]);
         } finally {
            try {
               await admin.deleteTable("ds_standalone", "stage", "migrated");
            } finally {
               await admin.deleteTable("ds_standalone", "main", "early");
            }
         }
         expect(await admin.listTables("ds_standalone", "main")).toEqual([]);
      });

      it("refuses to copy an encrypted catalog's table into an unencrypted one", async () => {
         const error = await refusal(
            admin.putTable(
               "ds_standalone",
               "stage",
               "leaked",
               {
                  source: {
                     catalog: "ds_owned",
                     schema: "sales",
                     table: "orders",
                  },
               },
               false,
            ),
         );
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
         expect(
            (await admin.listTables("ds_standalone", "stage")).map(
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
                     schema: "stage",
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
                     schema: "stage",
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
               schemaName: "sales",
               environmentPath: "",
            }),
         ).toEqual(["orders"]);
         // A reserved schema is refused before any catalog I/O; otherwise the catalog is looked up before the schema.
         const missing = await admin
            .putTable(
               "ds_nowhere",
               "sales",
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
         await admin.deleteTable("ds_owned", "sales", "orders");
         await admin.deleteTable("ds_owned", "sales", "orders");
         expect(await admin.listTables("ds_owned", "sales")).toEqual([]);
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

      it("refuses the slower of two concurrent creates of one catalog, rather than reporting the other's catalog as its own", async () => {
         // Both pass the existence check before either creates, so DuckLake's
         // own create decides, and the one that loses must not attach the
         // winner's catalog and answer as if it had made it with its settings.
         const results = await Promise.allSettled(
            [true, false].map((encrypted) =>
               admin.createCatalog({
                  name: "ds_race",
                  dataPath: join(root, `ds_race_${encrypted}`),
                  encrypted,
               }),
            ),
         );
         const created = results.filter((r) => r.status === "fulfilled");
         const refused = results.flatMap((r) =>
            r.status === "rejected" ? [r.reason] : [],
         );
         expect(created.length).toBe(1);
         expect(refused.length).toBe(1);
         expect(refused[0]).toBeInstanceOf(CatalogAdminError);
         expect([refused[0].status, refused[0].reason]).toEqual([
            409,
            "CATALOG_EXISTS",
         ]);
      });

      describe("schemas", () => {
         const catalog = "ds_schemas";
         const csv = (dir: string, file: string, rowCount: number) => {
            mkdirSync(join(root, dir), { recursive: true });
            const body = Array.from({ length: rowCount }, (_, i) => i + 1)
               .map((i) => `${i},${"x".repeat(50)}`)
               .join("\n");
            writeFileSync(join(root, dir, file), `id,note\n${body}\n`);
            return { uri: join(root, dir, file), format: "csv" };
         };

         beforeAll(async () => {
            await admin.createCatalog({
               name: catalog,
               dataPath: join(root, catalog),
            });
         });

         it("lists no tables across the schemas of an empty catalog", async () => {
            expect(await admin.listTables(catalog)).toEqual([]);
         });

         it("creates a schema with 201 and answers 200 for one that exists", async () => {
            const first = keep(
               await admin.createSchema(catalog, "s_one", {}, false),
            );
            expect(first).toEqual({ schema: { name: "s_one" }, created: true });
            const again = await admin.createSchema(catalog, "s_one", {}, false);
            expect(again.created).toBe(false);
            expect((await admin.getCatalog(catalog)).schemas).toEqual(
               expect.arrayContaining(["main", "s_one"]),
            );
         });

         it("refuses a create-only PUT of a schema that exists with SCHEMA_EXISTS", async () => {
            const error = await refusal(
               admin.createSchema(catalog, "s_one", {}, true),
            );
            expect([error.status, error.reason]).toEqual([
               412,
               "SCHEMA_EXISTS",
            ]);
         });

         it("refuses a body, an uppercase name, a reserved name and an unknown catalog", async () => {
            const body = await refusal(
               admin.createSchema(catalog, "s_body", { x: 1 }, false),
            );
            expect([body.status, body.reason]).toEqual([
               400,
               "INVALID_REQUEST",
            ]);
            for (const bad of ["S_Upper", "9lives", "a-b"]) {
               const error = await refusal(
                  admin.createSchema(catalog, bad, {}, false),
               );
               expect([bad, error.status, error.reason]).toEqual([
                  bad,
                  400,
                  "INVALID_REQUEST",
               ]);
            }
            for (const reserved of [
               "main",
               "memory",
               "information_schema",
               "pg_catalog",
               "temp",
               "system",
               "__ducklake_metadata_x",
            ]) {
               const put = await refusal(
                  admin.createSchema(catalog, reserved, {}, false),
               );
               const del = await refusal(admin.deleteSchema(catalog, reserved));
               expect([reserved, put.status, put.reason]).toEqual([
                  reserved,
                  400,
                  "SCHEMA_RESERVED",
               ]);
               expect([reserved, del.status, del.reason]).toEqual([
                  reserved,
                  400,
                  "SCHEMA_RESERVED",
               ]);
               const create = await refusal(
                  admin.createCatalog({
                     name: "ds_reserved_schema",
                     dataPath: join(root, "ds_reserved_schema"),
                     schemas: [reserved],
                  }),
               );
               expect([reserved, create.status, create.reason]).toEqual([
                  reserved,
                  400,
                  "SCHEMA_RESERVED",
               ]);
            }
            expect(
               (await refusal(admin.getCatalog("ds_reserved_schema"))).reason,
            ).toBe("CATALOG_NOT_FOUND");
            expect(
               (
                  await refusal(
                     admin.createSchema("ds_nowhere", "s_x", {}, false),
                  )
               ).reason,
            ).toBe("CATALOG_NOT_FOUND");
            expect(
               (await refusal(admin.deleteSchema("ds_nowhere", "s_x"))).reason,
            ).toBe("CATALOG_NOT_FOUND");
            expect((await admin.getCatalog(catalog)).schemas).not.toContain(
               "s_body",
            );
         });

         it("lets exactly one of two concurrent create-only PUTs create the schema", async () => {
            const results = await Promise.allSettled(
               [0, 1].map(() =>
                  admin.createSchema(catalog, "s_race", {}, true),
               ),
            );
            const made = results.filter((r) => r.status === "fulfilled");
            const refused = results.flatMap((r) =>
               r.status === "rejected" ? [r.reason] : [],
            );
            expect(made.length).toBe(1);
            expect(refused.length).toBe(1);
            expect(refused[0]).toBeInstanceOf(CatalogAdminError);
            expect([refused[0].status, refused[0].reason]).toEqual([
               412,
               "SCHEMA_EXISTS",
            ]);
         });

         it("answers 200 to the loser of two concurrent PUTs without If-None-Match", async () => {
            const results = await Promise.all(
               [0, 1].map(() =>
                  admin.createSchema(catalog, "s_race2", {}, false),
               ),
            );
            expect(results.map((r) => r.created).sort()).toEqual([false, true]);
         });

         it("lists the same table name in two schemas as two rows with their own sizes", async () => {
            await admin.createSchema(catalog, "s_two", {}, false);
            await admin.putTable(
               catalog,
               "s_one",
               "t",
               { source: csv("ds_schemas", "small.csv", 3) },
               true,
            );
            await admin.putTable(
               catalog,
               "s_two",
               "t",
               { source: csv("ds_schemas", "big.csv", 3000) },
               true,
            );
            const all = keep(await admin.listTables(catalog));
            expect(all.map((t) => [t.schema, t.name])).toEqual([
               ["s_one", "t"],
               ["s_two", "t"],
            ]);
            const [small, big] = all;
            expect(small.sizeBytes).toBeGreaterThan(0);
            expect(big.sizeBytes).toBeGreaterThan(small.sizeBytes);
            expect(
               (await admin.listTables(catalog, "s_one")).map(
                  (t) => t.sizeBytes,
               ),
            ).toEqual([small.sizeBytes]);
            expect(
               all.filter((t) =>
                  ["information_schema", "pg_catalog"].includes(t.schema),
               ),
            ).toEqual([]);
         });

         it("drops a schema with its tables, idempotently, and lets it be created again", async () => {
            await admin.deleteSchema(catalog, "s_two");
            expect(
               (await admin.listTables(catalog)).map((t) => t.schema),
            ).toEqual(["s_one"]);
            expect((await admin.getCatalog(catalog)).schemas).not.toContain(
               "s_two",
            );
            await admin.deleteSchema(catalog, "s_two");
            const again = await admin.createSchema(catalog, "s_two", {}, true);
            expect(again.created).toBe(true);
            expect(await admin.listTables(catalog, "s_two")).toEqual([]);
         });
      });

      it("never returns a credential", () => {
         const all = JSON.stringify(responses);
         expect(all).not.toContain(PASSWORD);
         expect(all).not.toContain("password");
      });
   },
);
