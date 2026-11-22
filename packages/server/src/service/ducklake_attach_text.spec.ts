// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The exact statements the three exported DuckLake attaches the run layer
// calls (attachDuckLakeReadWrite, attachDuckLakeReadOnly and
// attachDuckLakeReadOnlyIfPresent, each through attachDuckLakeWithMode) send
// to their session, recorded on a fake connection: the extension setup, the
// catalog-format preflight, the ATTACH itself and the storage secret. A
// characterization ahead of the run layer's tier refactor, which moves these
// calls behind a port: whatever reaches them must keep sending this text.
//
// Two parts of the text are not deterministic, and are fixed here: the
// preflight's temporary alias carries a process-wide counter
// (`<db>_fmt_preflight_<n>`), and `verify-full` names the ambient trust
// bundle, a file written under a fresh temporary directory. Whitespace is
// collapsed, so the secret statements read on one line. Every setting is its
// default (the extension policy, the row-group and file-size bounds a
// read-write attach applies when set), whatever the shell running the spec.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { components } from "../api";
import { installSettings } from "../test_helpers/settings_env";
import {
   attachDuckLakeReadOnly,
   attachDuckLakeReadOnlyIfPresent,
   attachDuckLakeReadWrite,
} from "./connection";
import { pgAttachSecretName } from "./postgres_conninfo";

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
});
afterAll(() => restoreSettings());

type DucklakeConnection = components["schemas"]["DucklakeConnection"];
type Rows = Record<string, unknown>[];

/**
 * A connection that records every statement and answers each with `answer`'s
 * rows (none by default): the preflight's catalog lookup, the recorded data
 * path and the secret lookup are the only reads that matter.
 */
function recorder(answer: (sql: string) => Rows = () => []): {
   conn: DuckDBConnection;
   statements: () => string[];
} {
   const sql: string[] = [];
   const conn = {
      runSQL: async (q: string) => {
         sql.push(q);
         const rows = answer(q);
         return { rows, totalRows: rows.length };
      },
   } as unknown as DuckDBConnection;
   return {
      conn,
      statements: () => {
         const all = sql.map(normalize);
         // The catalog's password appears in its secret and nowhere else.
         expect(
            all.filter((q) => q.includes("test-password") && q !== PG_SECRET),
         ).toEqual([]);
         return all;
      },
   };
}

function normalize(sql: string): string {
   return (
      sql
         .replace(/\s+/g, " ")
         .trim()
         .replace(/_fmt_preflight_\d+/g, "_fmt_preflight_N")
         // A path with a backslash (Windows) is quoted, libpq-style, inside the
         // SQL literal: sslrootcert=''D:\\Temp\\….pem''.
         .replace(
            /sslrootcert=(?:''(?:[^'\\]|\\.)*''|[^\s']+)/g,
            "sslrootcert=<ambient-bundle>",
         )
   );
}

/** Answers the preflight's lookup of the metadata schema: the catalog exists. */
const catalogPresent = (sql: string): Rows =>
   sql.startsWith("SELECT count(*) AS n FROM duckdb_tables()")
      ? [{ n: 1 }]
      : [];

const PG = {
   host: "catalog.test",
   port: 5433,
   databaseName: "lakemeta",
   userName: "lake_user",
   password: "test-password",
};
const S3 = {
   accessKeyId: "AKIATESTKEY",
   secretAccessKey: "test-secret-key",
   region: "us-east-1",
};
const GCS = { keyId: "GOOGTESTKEY", secret: "test-gcs-secret" };

// The attach path: the catalog's user and password are in its secret.
const CONNINFO_VERIFY_FULL =
   "host=catalog.test port=5433 dbname=lakemeta sslmode=verify-full sslrootcert=<ambient-bundle>";
const CONNINFO_DISABLE =
   "host=catalog.test port=5433 dbname=lakemeta sslmode=disable";

/** The catalog's credential, created before anything attaches. */
const PG_SECRET_NAME = pgAttachSecretName("lake");
const PG_SECRET = `CREATE OR REPLACE TEMPORARY SECRET ${PG_SECRET_NAME} (TYPE postgres, USER 'lake_user', PASSWORD 'test-password');`;
/** How the DuckLake ATTACH names that secret for its metadata catalog. */
const META_SECRET = `METADATA_PARAMETERS MAP {'SECRET': '${PG_SECRET_NAME}'}`;

/**
 * After every httpfs load: the AWS_* keys httpfs seeds blanked, and the global
 * S3 fallback off (duckdb_lockdown.ts).
 */
const HTTPFS_AMBIENT_OFF = [
   "SET GLOBAL enable_global_s3_configuration=false",
   "SET GLOBAL s3_access_key_id=''",
   "SET GLOBAL s3_secret_access_key=''",
   "SET GLOBAL s3_session_token=''",
];

/** Extension policy and the explicit DuckLake stack, before every attach. */
const EXTENSIONS = [
   "SET autoinstall_known_extensions=false;",
   "SET autoload_known_extensions=true;",
   "INSTALL ducklake;",
   "LOAD ducklake;",
   "INSTALL postgres;",
   "LOAD postgres;",
   "INSTALL aws;",
   "LOAD aws;",
   "INSTALL httpfs;",
   "LOAD httpfs;",
   ...HTTPFS_AMBIENT_OFF,
];
/** Before an S3 credential reaches anything: no ambient AWS_* fallback. */
const S3_GLOBAL_OFF = "SET GLOBAL enable_global_s3_configuration=false";

/** The format preflight: a plain postgres attach that reads the catalog. */
function preflight(conninfo: string, metadataSchema?: string): string[] {
   const alias = `"lake_fmt_preflight_N"`;
   return [
      PG_SECRET,
      `ATTACH '${conninfo}' AS ${alias} (TYPE postgres, READ_ONLY, SECRET ${PG_SECRET_NAME});`,
      ...(metadataSchema
         ? [
              `SELECT count(*) AS n FROM duckdb_tables() WHERE database_name = 'lake_fmt_preflight_N' AND schema_name = '${metadataSchema}' AND table_name = 'ducklake_metadata';`,
              `SELECT value FROM ${alias}."${metadataSchema}".ducklake_metadata WHERE key = 'version' LIMIT 1;`,
           ]
         : [
              `SELECT value FROM ${alias}.ducklake_metadata WHERE key = 'version' LIMIT 1;`,
           ]),
      `DETACH ${alias};`,
   ];
}

/** The storage secret, created after the ATTACH, scoped to `scope`. */
function s3Secret(scope: string): string[] {
   return [
      "INSTALL httpfs;",
      "LOAD httpfs;",
      ...HTTPFS_AMBIENT_OFF,
      "SELECT COUNT(*) AS count FROM duckdb_secrets() WHERE name = 'secret_s3_lake_storage';",
      `CREATE OR REPLACE SECRET secret_s3_lake_storage ( TYPE s3, KEY_ID 'AKIATESTKEY', SECRET 'test-secret-key', REGION 'us-east-1', SCOPE ['${scope}'] );`,
   ];
}

function gcsSecret(scope: string): string[] {
   return [
      "INSTALL httpfs;",
      "LOAD httpfs;",
      ...HTTPFS_AMBIENT_OFF,
      "SELECT COUNT(*) AS count FROM duckdb_secrets() WHERE name = 'secret_gcs_lake_storage';",
      `CREATE OR REPLACE SECRET secret_gcs_lake_storage ( TYPE gcs, KEY_ID 'GOOGTESTKEY', SECRET 'test-gcs-secret', SCOPE ['${scope}'] );`,
   ];
}

describe("the DuckLake ATTACH text: a Postgres catalog with a metadata schema and an S3 data path", () => {
   const config = {
      catalog: {
         postgresConnection: { ...PG, sslmode: "verify-full" },
         metadataSchema: "lake_meta",
      },
      storage: { bucketUrl: "s3://test-bucket/lake", s3Connection: S3 },
   } as DucklakeConnection;
   const attach = (clauses: string) =>
      `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_VERIFY_FULL}' AS "lake" (${clauses});`;

   it("read-write: the data path may be overridden and the catalog created", async () => {
      const r = recorder(catalogPresent);
      await attachDuckLakeReadWrite(r.conn, "lake", config);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         S3_GLOBAL_OFF,
         ...preflight(CONNINFO_VERIFY_FULL, "lake_meta"),
         attach(
            `DATA_PATH 's3://test-bucket/lake', OVERRIDE_DATA_PATH true, METADATA_SCHEMA 'lake_meta', ${META_SECRET}`,
         ),
         ...s3Secret("s3://test-bucket/lake/"),
      ]);
   });

   it("read-only: the same, with READ_ONLY", async () => {
      const r = recorder(catalogPresent);
      await attachDuckLakeReadOnly(r.conn, "lake", config);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         S3_GLOBAL_OFF,
         ...preflight(CONNINFO_VERIFY_FULL, "lake_meta"),
         attach(
            `DATA_PATH 's3://test-bucket/lake', OVERRIDE_DATA_PATH true, READ_ONLY true, METADATA_SCHEMA 'lake_meta', ${META_SECRET}`,
         ),
         ...s3Secret("s3://test-bucket/lake/"),
      ]);
   });

   it("read-only if present, on a catalog that exists: the read-only attach", async () => {
      const r = recorder(catalogPresent);
      expect(
         await attachDuckLakeReadOnlyIfPresent(r.conn, "lake", config),
      ).toBe(true);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         S3_GLOBAL_OFF,
         ...preflight(CONNINFO_VERIFY_FULL, "lake_meta"),
         attach(
            `DATA_PATH 's3://test-bucket/lake', OVERRIDE_DATA_PATH true, READ_ONLY true, METADATA_SCHEMA 'lake_meta', ${META_SECRET}`,
         ),
         ...s3Secret("s3://test-bucket/lake/"),
      ]);
   });

   it("read-only if present, on a catalog that does not exist: the preflight alone, and no attach", async () => {
      const r = recorder();
      expect(
         await attachDuckLakeReadOnlyIfPresent(r.conn, "lake", config),
      ).toBe(false);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         S3_GLOBAL_OFF,
         PG_SECRET,
         `ATTACH '${CONNINFO_VERIFY_FULL}' AS "lake_fmt_preflight_N" (TYPE postgres, READ_ONLY, SECRET ${PG_SECRET_NAME});`,
         "SELECT count(*) AS n FROM duckdb_tables() WHERE database_name = 'lake_fmt_preflight_N' AND schema_name = 'lake_meta' AND table_name = 'ducklake_metadata';",
         'DETACH "lake_fmt_preflight_N";',
      ]);
   });
});

describe("the DuckLake ATTACH text: a Postgres catalog with no metadata schema and a GCS data path", () => {
   const config = {
      catalog: { postgresConnection: { ...PG, sslmode: "verify-full" } },
      storage: { bucketUrl: "gs://test-bucket/lake/", gcsConnection: GCS },
   } as DucklakeConnection;

   it("read-write: no S3 fallback switch, the version read from the default schema, and a GCS secret", async () => {
      const r = recorder();
      await attachDuckLakeReadWrite(r.conn, "lake", config);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         ...preflight(CONNINFO_VERIFY_FULL),
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_VERIFY_FULL}' AS "lake" (DATA_PATH 'gs://test-bucket/lake/', OVERRIDE_DATA_PATH true, ${META_SECRET});`,
         ...gcsSecret("gs://test-bucket/lake/"),
      ]);
   });

   it("read-only: READ_ONLY, and the same secret", async () => {
      const r = recorder();
      await attachDuckLakeReadOnly(r.conn, "lake", config);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         ...preflight(CONNINFO_VERIFY_FULL),
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_VERIFY_FULL}' AS "lake" (DATA_PATH 'gs://test-bucket/lake/', OVERRIDE_DATA_PATH true, READ_ONLY true, ${META_SECRET});`,
         ...gcsSecret("gs://test-bucket/lake/"),
      ]);
   });

   it("read-only if present: with no metadata schema the catalog is never found absent, so it attaches", async () => {
      const r = recorder();
      expect(
         await attachDuckLakeReadOnlyIfPresent(r.conn, "lake", config),
      ).toBe(true);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         ...preflight(CONNINFO_VERIFY_FULL),
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_VERIFY_FULL}' AS "lake" (DATA_PATH 'gs://test-bucket/lake/', OVERRIDE_DATA_PATH true, READ_ONLY true, ${META_SECRET});`,
         ...gcsSecret("gs://test-bucket/lake/"),
      ]);
   });
});

describe("the DuckLake ATTACH text: no configured data path", () => {
   it("read-write, local storage: the recorded path, and a missing catalog refused", async () => {
      const r = recorder();
      await attachDuckLakeReadWrite(r.conn, "lake", {
         catalog: { postgresConnection: { ...PG, sslmode: "disable" } },
      } as DucklakeConnection);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         ...preflight(CONNINFO_DISABLE),
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_DISABLE}' AS "lake" (CREATE_IF_NOT_EXISTS false, ${META_SECRET});`,
         // The recorded data path, which the lock allows (duckdb_lockdown.ts).
         `SELECT key, value FROM "__ducklake_metadata_lake".ducklake_metadata WHERE scope IS NULL`,
      ]);
   });

   it("read-only, local storage: READ_ONLY alone", async () => {
      const r = recorder();
      await attachDuckLakeReadOnly(r.conn, "lake", {
         catalog: { postgresConnection: { ...PG, sslmode: "disable" } },
      } as DucklakeConnection);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         ...preflight(CONNINFO_DISABLE),
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_DISABLE}' AS "lake" (READ_ONLY true, ${META_SECRET});`,
         // The recorded data path, which the lock allows (duckdb_lockdown.ts).
         `SELECT key, value FROM "__ducklake_metadata_lake".ducklake_metadata WHERE scope IS NULL`,
      ]);
   });

   it("read-write with an S3 credential: the secret is scoped to the path the attached catalog recorded", async () => {
      const r = recorder((sql) =>
         sql.startsWith("SELECT key, value FROM")
            ? [{ key: "data_path", value: "s3://test-bucket/recorded/" }]
            : catalogPresent(sql),
      );
      await attachDuckLakeReadWrite(r.conn, "lake", {
         catalog: {
            postgresConnection: { ...PG, sslmode: "disable" },
            metadataSchema: "lake_meta",
         },
         storage: { s3Connection: S3 },
      } as DucklakeConnection);
      expect(r.statements()).toEqual([
         ...EXTENSIONS,
         S3_GLOBAL_OFF,
         ...preflight(CONNINFO_DISABLE, "lake_meta"),
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_DISABLE}' AS "lake" (CREATE_IF_NOT_EXISTS false, METADATA_SCHEMA 'lake_meta', ${META_SECRET});`,
         'SELECT key, value FROM "__ducklake_metadata_lake"."lake_meta".ducklake_metadata WHERE scope IS NULL',
         ...s3Secret("s3://test-bucket/recorded/"),
      ]);
   });

   it("read-write with an S3 credential and no recorded path: no secret, and the catalog is detached again", async () => {
      const r = recorder(catalogPresent);
      await expect(
         attachDuckLakeReadWrite(r.conn, "lake", {
            catalog: {
               postgresConnection: { ...PG, sslmode: "disable" },
               metadataSchema: "lake_meta",
            },
            storage: { s3Connection: S3 },
         } as DucklakeConnection),
      ).rejects.toThrow(/No storage secret could be scoped/);
      expect(r.statements().slice(-3)).toEqual([
         `ATTACH OR REPLACE 'ducklake:postgres:${CONNINFO_DISABLE}' AS "lake" (CREATE_IF_NOT_EXISTS false, METADATA_SCHEMA 'lake_meta', ${META_SECRET});`,
         'SELECT key, value FROM "__ducklake_metadata_lake"."lake_meta".ducklake_metadata WHERE scope IS NULL',
         'DETACH "lake";',
      ]);
   });
});
