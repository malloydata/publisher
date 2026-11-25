// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DuckLake data inlining on the catalogs Publisher writes into, on a real
// Postgres catalog. DuckLake's default keeps a table of up to 10 rows as rows
// in the catalog database instead of a file, and a read-only reader confined
// to the catalog's files then still reads them. A catalog Publisher creates
// records the limit 0, so every writer's small table lands in a file; an
// existing catalog that records more is left as it is and warned about.
//
// Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a Postgres the test may create
// schemas and databases in.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import sinon from "sinon";
import type { components } from "../api";
import { logger } from "../logger";
import { attachDuckLakeForCreate, attachDuckLakeReadWrite } from "./connection";
import { DUCKLAKE } from "./dialects/ducklake";
import {
   DUCKLAKE_DATA_INLINING_ON,
   recordedInliningLimit,
} from "./dialects/ducklake_inlining";

type DucklakeConnection = components["schemas"]["DucklakeConnection"];

const PG_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

describe.skipIf(!PG_URL)(
   "DuckLake data inlining on Publisher's catalogs",
   () => {
      let pg: Client;
      let dataRoot: string;
      const schemas: string[] = [];
      const databases: string[] = [];
      const sessions: DuckDBConnection[] = [];
      let round = 0;

      beforeAll(async () => {
         pg = new Client({ connectionString: PG_URL });
         await pg.connect();
         dataRoot = mkdtempSync(join(tmpdir(), "ducklake-inlining-"));
      });

      afterAll(async () => {
         for (const session of sessions) await session.close().catch(() => {});
         for (const schema of schemas) {
            await pg.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         }
         for (const db of databases) {
            await pg.query(`DROP DATABASE IF EXISTS "${db}" WITH (FORCE)`);
         }
         await pg.end();
         rmSync(dataRoot, { recursive: true, force: true });
      });

      function session(): DuckDBConnection {
         const dir = mkdtempSync(join(dataRoot, "session-"));
         const s = new DuckDBConnection(`s${round}`, ":memory:", dir);
         sessions.push(s);
         return s;
      }

      /**
       * A config for a catalog that does not exist yet: in its own metadata
       * schema, or (`ownDatabase`) in the default schema of a database of its own,
       * where the format preflight cannot tell that it is absent.
       */
      async function newCatalog(
         opts: { ownDatabase?: boolean } = {},
      ): Promise<DucklakeConnection> {
         const name = `inl_${process.pid}_${round++}`;
         const u = new URL(PG_URL!);
         let databaseName = u.pathname.slice(1);
         let metadataSchema: string | undefined;
         if (opts.ownDatabase) {
            await pg.query(`CREATE DATABASE "${name}"`);
            databases.push(name);
            databaseName = name;
         } else {
            await pg.query(`CREATE SCHEMA "${name}"`);
            schemas.push(name);
            metadataSchema = name;
         }
         return {
            catalog: {
               postgresConnection: {
                  host: u.hostname,
                  port: Number(u.port || 5432),
                  databaseName,
                  userName: decodeURIComponent(u.username),
                  password: decodeURIComponent(u.password),
               },
               ...(metadataSchema ? { metadataSchema } : {}),
            },
            storage: { bucketUrl: join(dataRoot, name) + "/" },
         } as DucklakeConnection;
      }

      async function recordedLimit(
         s: DuckDBConnection,
         cfg: DucklakeConnection,
      ): Promise<string | undefined> {
         const schema = cfg.catalog?.metadataSchema;
         const ref = `"__ducklake_metadata_lake"${schema ? `."${schema}"` : ""}`;
         const rows = (
            await s.runSQL(
               `SELECT value FROM ${ref}.ducklake_metadata WHERE scope IS NULL AND key = 'data_inlining_row_limit'`,
            )
         ).rows as { value: string }[];
         return rows[0]?.value;
      }

      /**
       * A table of one row written by a session that sets no inlining default of
       * its own (as any writer but Publisher's build may), read back by a reader
       * confined to a directory that holds none of the catalog's files.
       */
      async function writeSmallTableAndReadConfined(
         writer: DuckDBConnection,
         cfg: DucklakeConnection,
      ): Promise<string> {
         await writer.runSQL(`CREATE TABLE lake.main.small AS SELECT 1 AS x`);
         const reader = session();
         await reader.runSQL(
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
         );
         const p = cfg.catalog!.postgresConnection!;
         const schemaClause = cfg.catalog?.metadataSchema
            ? `, METADATA_SCHEMA '${cfg.catalog.metadataSchema}'`
            : "";
         await reader.runSQL(
            `ATTACH 'ducklake:postgres:host=${p.host} port=${p.port} dbname=${p.databaseName} user=${p.userName} password=${p.password}' AS r (READ_ONLY true${schemaClause})`,
         );
         await reader.runSQL(
            `SET allowed_directories=['${join(dataRoot, "nowhere")}/']`,
         );
         await reader.runSQL("SET enable_external_access=false");
         return reader.runSQL("SELECT x FROM r.main.small").then(
            (r) => `read ${JSON.stringify(r.rows)}`,
            (e: Error) => e.message,
         );
      }

      it("a destination a build creates records the limit 0, so a small table is a file a confined reader cannot read", async () => {
         const cfg = await newCatalog();
         const s = session();
         await attachDuckLakeReadWrite(s, "lake", cfg);
         expect(await recordedLimit(s, cfg)).toBe("0");
         expect(await writeSmallTableAndReadConfined(s, cfg)).toMatch(
            /Permission Error: Cannot access file/,
         );
      }, 60_000);

      it("records 0 too where the preflight cannot tell the catalog was absent", async () => {
         const cfg = await newCatalog({ ownDatabase: true });
         const s = session();
         await attachDuckLakeReadWrite(s, "lake", cfg);
         expect(await recordedLimit(s, cfg)).toBe("0");
         expect(await writeSmallTableAndReadConfined(s, cfg)).toMatch(
            /Permission Error: Cannot access file/,
         );
      }, 60_000);

      it("a catalog the admin API creates records 0 before any limit the create names", async () => {
         const unset = await newCatalog();
         const a = session();
         await attachDuckLakeForCreate(a, "lake", unset, { encrypted: false });
         expect(await recordedLimit(a, unset)).toBe("0");
      }, 60_000);

      it("leaves an existing catalog's setting as it is, and warns of a nonzero one", async () => {
         const cfg = await newCatalog();
         const host = session();
         await host.runSQL(
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
         );
         const p = cfg.catalog!.postgresConnection!;
         await host.runSQL(
            `ATTACH 'ducklake:postgres:host=${p.host} port=${p.port} dbname=${p.databaseName} user=${p.userName} password=${p.password}' AS lake (DATA_PATH '${cfg.storage!.bucketUrl}', METADATA_SCHEMA '${cfg.catalog!.metadataSchema}')`,
         );
         await host.runSQL(
            "CALL lake.set_option('data_inlining_row_limit', 10)",
         );
         await host.runSQL("DETACH lake");

         const warn = sinon.spy(logger, "warn");
         try {
            const reader = session();
            await DUCKLAKE.destination!.attach(
               reader,
               "lake",
               { name: "lake", type: "ducklake", ducklakeConnection: cfg },
               { readOnly: true, origin: "storage-destination" },
            );
            const coded = warn
               .getCalls()
               .filter(
                  (c) =>
                     ((c.args as unknown[])[1] as { code?: string } | undefined)
                        ?.code === DUCKLAKE_DATA_INLINING_ON,
               );
            expect(coded.length).toBe(1);
            expect(String(coded[0].args[0])).toContain(
               "records data_inlining_row_limit 10",
            );
            expect(recordedInliningLimit(cfg)).toBe(10);

            // A read-write attach does not reset it either.
            const writer = session();
            await attachDuckLakeReadWrite(writer, "lake", cfg);
            expect(await recordedLimit(writer, cfg)).toBe("10");
         } finally {
            warn.restore();
         }
      }, 60_000);

      it("leaves alone an existing catalog that records no limit", async () => {
         const cfg = await newCatalog();
         const host = session();
         await host.runSQL(
            "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;",
         );
         const p = cfg.catalog!.postgresConnection!;
         await host.runSQL(
            `ATTACH 'ducklake:postgres:host=${p.host} port=${p.port} dbname=${p.databaseName} user=${p.userName} password=${p.password}' AS lake (DATA_PATH '${cfg.storage!.bucketUrl}', METADATA_SCHEMA '${cfg.catalog!.metadataSchema}')`,
         );
         await host.runSQL("CREATE TABLE lake.main.t AS SELECT 1 AS x");
         await host.runSQL("DETACH lake");
         const writer = session();
         await attachDuckLakeReadWrite(writer, "lake", cfg);
         expect(await recordedLimit(writer, cfg)).toBeUndefined();
         expect(recordedInliningLimit(cfg)).toBeUndefined();
      }, 60_000);
   },
);
