// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Two builds attaching a DuckLake catalog that does not exist yet both create
// it, and Postgres refuses the slower create with a duplicate key "already
// exists". The attach used to read those words as "already attached" and skip,
// leaving nothing attached, so the build failed on its next statement with
// "Schema with name lake does not exist". It now attaches again (the other
// create has committed by then) and skips only an alias that is attached.
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { Client } from "pg";
import type { components } from "../api";
import {
   attachDuckLakeForCreate,
   attachDuckLakeReadWrite,
   DUCKLAKE_INIT_RACE_ATTEMPTS,
   isDuckLakeInitRace,
} from "./connection";
import { createIsolatedBuildSession } from "./materialization/run/materialization_build_session";

type DucklakeConnection = components["schemas"]["DucklakeConnection"];

/** What DuckLake reports when another session created the catalog first. */
const RACE_LOST =
   'Invalid Error: Failed to initialize DuckLake: Failed to execute query "CREATE TABLE ' +
   '"lakemeta"."ducklake_metadata"(...)": ERROR:  duplicate key value violates unique ' +
   'constraint "pg_type_typname_nsp_index"\nDETAIL:  Key (typname, typnamespace)=' +
   "(ducklake_metadata, 17860) already exists.";

/**
 * A session whose DuckLake ATTACHes fail with `failures` in turn and then
 * succeed. `attached` lists the databases attached, by stored name.
 */
function stub(
   failures: string[],
   attached: string[] = [],
): { conn: DuckDBConnection; attaches: () => number } {
   let attaches = 0;
   const conn = {
      runSQL: async (q: string) => {
         if (q.includes("'ducklake:postgres:")) {
            attaches++;
            const failure = failures.shift();
            if (failure !== undefined) throw new Error(failure);
         }
         const lookup =
            /^SELECT 1 FROM duckdb_databases\(\) WHERE lower\(database_name\) = lower\('((?:[^']|'')*)'\)$/.exec(
               q,
            );
         if (lookup) {
            // As DuckDB resolves it: case-insensitively.
            const alias = lookup[1].replaceAll("''", "'").toLowerCase();
            const rows = attached
               .filter((name) => name.toLowerCase() === alias)
               .map(() => ({ "1": 1 }));
            return { rows, totalRows: rows.length };
         }
         return { rows: [], totalRows: 0 };
      },
   } as unknown as DuckDBConnection;
   return { conn, attaches: () => attaches };
}

const config = (): DucklakeConnection =>
   ({
      catalog: {
         postgresConnection: {
            host: "catalog",
            port: 5432,
            databaseName: "ducklake",
            userName: "u",
            password: "p",
         },
      },
      storage: { bucketUrl: "/tmp/lake" },
   }) as DucklakeConnection;

describe("a DuckLake attach that lost a race to create the catalog", () => {
   it("is recognised by DuckLake's create failure, not by any 'already exists'", () => {
      expect(isDuckLakeInitRace(new Error(RACE_LOST))).toBe(true);
      expect(
         isDuckLakeInitRace(
            new Error(
               'Failed to initialize DuckLake: ERROR: relation "ducklake_metadata" already exists',
            ),
         ),
      ).toBe(true);
      expect(
         isDuckLakeInitRace(new Error('Catalog "lake" already exists')),
      ).toBe(false);
   });

   it("attaches again, and the attach succeeds", async () => {
      const s = stub([RACE_LOST]);
      await attachDuckLakeReadWrite(s.conn, "lake", config());
      expect(s.attaches()).toBe(2);
   });

   it("fails with the create's error when every try loses", async () => {
      const s = stub(Array(DUCKLAKE_INIT_RACE_ATTEMPTS).fill(RACE_LOST));
      await expect(
         attachDuckLakeReadWrite(s.conn, "lake", config()),
      ).rejects.toThrow("Failed to initialize DuckLake");
      expect(s.attaches()).toBe(DUCKLAKE_INIT_RACE_ATTEMPTS);
   });

   it("is not tried again when the attach is a create, which fails instead", async () => {
      // A retry would open the other creator's catalog, with its settings, and
      // the create would report it as its own.
      const s = stub([RACE_LOST]);
      await expect(
         attachDuckLakeForCreate(s.conn, "lake", config(), { encrypted: true }),
      ).rejects.toThrow("Failed to initialize DuckLake");
      expect(s.attaches()).toBe(1);
   });

   it("is never read as an alias already attached", async () => {
      // Even when the alias is attached on the session: the create failed.
      const s = stub(Array(DUCKLAKE_INIT_RACE_ATTEMPTS).fill(RACE_LOST), [
         "lake",
      ]);
      await expect(
         attachDuckLakeReadWrite(s.conn, "lake", config()),
      ).rejects.toThrow("Failed to initialize DuckLake");
   });
});

describe("an attach failing 'already exists'", () => {
   it("is skipped when the alias is attached", async () => {
      const s = stub(['Catalog "lake" already exists'], ["lake"]);
      await attachDuckLakeReadWrite(s.conn, "lake", config());
      expect(s.attaches()).toBe(1);
   });

   it("is skipped when the alias is attached under another case", async () => {
      // DuckDB names are case-insensitive, and a replace keeps the stored name.
      const s = stub(['Catalog "lake" already exists'], ["Lake"]);
      await attachDuckLakeReadWrite(s.conn, "lake", config());
      expect(s.attaches()).toBe(1);
   });

   it("looks the alias up as a quoted literal", async () => {
      const s = stub(["it already exists"], ["o'lake"]);
      await attachDuckLakeReadWrite(s.conn, "o'lake", config());
      expect(s.attaches()).toBe(1);
   });

   it("fails when nothing is attached under the alias", async () => {
      const s = stub(['Catalog "lake" already exists'], ["memory"]);
      await expect(
         attachDuckLakeReadWrite(s.conn, "lake", config()),
      ).rejects.toThrow("already exists");
   });
});

/**
 * The race itself, on a real Postgres catalog. Opt-in: set
 * PUBLISHER_TEST_POSTGRES_URL to a database the test may create schemas in,
 * e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */
const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
const BUILDS = 4;
const ROUNDS = 8;

describe.skipIf(!url)(
   "builds attaching one new DuckLake catalog at once",
   () => {
      let pg: Client;
      let dataRoot: string;
      const schemas: string[] = [];

      beforeAll(async () => {
         pg = new Client({ connectionString: url });
         await pg.connect();
         dataRoot = mkdtempSync(join(tmpdir(), "ducklake-race-"));
      });

      afterAll(async () => {
         for (const schema of schemas) {
            await pg.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
         }
         await pg.end();
         rmSync(dataRoot, { recursive: true, force: true });
      });

      it(`every one writes into it (${ROUNDS} rounds of ${BUILDS})`, async () => {
         const parsed = new URL(url!);
         const failures: string[] = [];
         for (let round = 0; round < ROUNDS; round++) {
            // A fresh catalog each round: only its creation races.
            const schema = `race_${process.pid}_${Date.now()}_${round}`;
            schemas.push(schema);
            await pg.query(`CREATE SCHEMA "${schema}"`);
            const cfg = {
               catalog: {
                  postgresConnection: {
                     host: parsed.hostname,
                     port: Number(parsed.port || 5432),
                     databaseName: parsed.pathname.slice(1),
                     userName: decodeURIComponent(parsed.username),
                     password: decodeURIComponent(parsed.password),
                  },
                  metadataSchema: schema,
               },
               storage: { bucketUrl: join(dataRoot, schema) + "/" },
            } as DucklakeConnection;
            const outcomes = await Promise.all(
               Array.from({ length: BUILDS }, async (_, i) => {
                  const { session, dispose } = createIsolatedBuildSession(
                     `race_${i}`,
                  );
                  try {
                     await attachDuckLakeReadWrite(session, "lake", cfg);
                     await session.runSQL(
                        `CREATE TABLE "lake"."main"."t${i}" AS SELECT ${i} AS x`,
                     );
                     return undefined;
                  } catch (err) {
                     return err instanceof Error ? err.message : String(err);
                  } finally {
                     await dispose();
                  }
               }),
            );
            failures.push(
               ...outcomes.filter((o): o is string => o !== undefined),
            );
         }
         expect(failures).toEqual([]);
      }, 120_000);
   },
);
