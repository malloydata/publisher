// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The DuckDB lockdown (duckdb_lockdown.ts), one vector at a time, on each
 * instance class it covers: the package sandbox (Malloy's sandboxed policy),
 * an environment `duckdb` connection with only setupSQL (allowlist and lock as
 * options), an environment `duckdb` connection with an attached bucket and a
 * DuckLake catalog (both networked: their own setup, then the lock). A loopback
 * HTTP server stands in for both the cloud metadata server and the bucket.
 */

import { DuckDBInstance } from "@duckdb/node-api";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as http from "http";
import * as os from "os";
import * as path from "path";
import { installSettings } from "../test_helpers/settings_env";
import { buildEnvironmentMalloyConfig } from "./connection";
import { instanceKeyOf } from "./duckdb_instance_sharing";
import {
   allowForLockdown,
   azureAllowPrefix,
   differsOnlyInLockedSetup,
   lockDownDuckDBSession,
   networkedDuckDBOptions,
   sandboxedDuckDBOptions,
} from "./duckdb_lockdown";
import { Package } from "./package";

const ENV_KEY = "AKIA_LOCKDOWN_ENV_KEY";
const ENV_SECRET = "lockdown-env-secret-value";

type Seen = { url: string; headers: http.IncomingHttpHeaders };

let root: string;
let server: http.Server;
let origin: string;
const seen: Seen[] = [];
const savedEnv: Record<string, string | undefined> = {};

function rowsOf(result: unknown): Record<string, unknown>[] {
   const rows = (result as { rows?: unknown }).rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

async function refused(
   connection: { runSQL(sql: string): Promise<unknown> },
   sql: string,
): Promise<string> {
   try {
      await connection.runSQL(sql);
   } catch (error) {
      return error instanceof Error ? error.message : String(error);
   }
   throw new Error(`expected DuckDB to refuse: ${sql}`);
}

function requestsTo(fragment: string): Seen[] {
   return seen.filter((request) => request.url.includes(fragment));
}

/** Every vector a locked instance must refuse, whatever its tier. */
async function expectCommonVectorsRefused(
   connection: DuckDBConnection,
): Promise<void> {
   // A local file outside the allowlist, directly and by traversal.
   await refused(connection, `SELECT * FROM '${root}/outside/secret.csv'`);
   await refused(
      connection,
      `SELECT * FROM '${root}/pkg/../outside/secret.csv'`,
   );
   await refused(connection, `SELECT * FROM read_text('/etc/hosts')`);
   // The env credentials never read back.
   for (const name of ["s3_access_key_id", "s3_secret_access_key"]) {
      let value: unknown;
      try {
         value = rowsOf(
            await connection.runSQL(`SELECT current_setting('${name}') AS v`),
         )[0]?.v;
      } catch {
         value = undefined; // httpfs not loaded: the setting does not exist.
      }
      expect(value === ENV_KEY || value === ENV_SECRET).toBe(false);
   }
   // Host credentials through a credential_chain secret.
   await refused(
      connection,
      "CREATE SECRET lockdown_chain (TYPE s3, PROVIDER credential_chain)",
   );
   // The metadata server, through an http secret carrying its header or a
   // bearer token: either the secret is refused or the read is, and no request
   // reaches the stand-in.
   for (const secret of [
      `CREATE SECRET lockdown_md (TYPE http, EXTRA_HTTP_HEADERS MAP {'Metadata-Flavor': 'Google'}, SCOPE '${origin}')`,
      `CREATE SECRET lockdown_bearer (TYPE http, BEARER_TOKEN 'tok', SCOPE '${origin}/b')`,
   ]) {
      await connection.runSQL(secret).catch(() => {});
   }
   await refused(
      connection,
      `SELECT * FROM read_text('${origin}/computeMetadata/v1/instance/service-accounts/default/token')`,
   );
   expect(requestsTo("computeMetadata")).toEqual([]);
   // ATTACH of another catalog.
   await refused(
      connection,
      `ATTACH '${root}/outside/other.duckdb' AS lockdown_other`,
   );
   await refused(
      connection,
      `ATTACH 'host=127.0.0.1 port=1 dbname=x' AS lockdown_pg (TYPE postgres)`,
   );
   // Community extensions.
   await refused(connection, "INSTALL h3 FROM community");
   await refused(connection, "LOAD h3");
   // A persistent secret.
   await refused(
      connection,
      "CREATE PERSISTENT SECRET lockdown_persist (TYPE http, BEARER_TOKEN 'x')",
   );
   // SET or RESET after the lock.
   await refused(connection, "SET enable_external_access=true");
   await refused(connection, "RESET enable_external_access");
   await refused(connection, "SET allowed_directories=['/']");
   await refused(connection, "SET lock_configuration=false");
   await refused(connection, "SET allow_persistent_secrets=true");
}

beforeAll(async () => {
   for (const name of [
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_SESSION_TOKEN",
   ]) {
      savedEnv[name] = process.env[name];
   }
   process.env.AWS_ACCESS_KEY_ID = ENV_KEY;
   process.env.AWS_SECRET_ACCESS_KEY = ENV_SECRET;
   process.env.AWS_SESSION_TOKEN = "lockdown-env-token";

   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "duckdb-lockdown-")),
   );
   fs.mkdirSync(path.join(root, "pkg", "data"), { recursive: true });
   fs.mkdirSync(path.join(root, "outside"), { recursive: true });
   fs.writeFileSync(path.join(root, "pkg", "data", "x.csv"), "a,b\n1,2\n");
   fs.writeFileSync(path.join(root, "pkg", "top.csv"), "z\n9\n");
   fs.writeFileSync(path.join(root, "outside", "secret.csv"), "s\nleak\n");
   fs.symlinkSync(
      path.join(root, "outside"),
      path.join(root, "pkg", "escape_link"),
   );
   // An .xlsx fixture, written by an unrestricted instance.
   const writer = await (await DuckDBInstance.create(":memory:")).connect();
   await writer.run("LOAD excel");
   await writer.run(
      `COPY (SELECT 1 AS a, 'x' AS b) TO '${root}/pkg/data/book.xlsx' (FORMAT xlsx)`,
   );
   writer.closeSync();

   server = http.createServer((req, res) => {
      seen.push({ url: req.url ?? "", headers: req.headers });
      const body = "a,b\n7,8\n";
      res.writeHead(200, {
         "content-type": "text/csv",
         "content-length": Buffer.byteLength(body),
         "last-modified": new Date(0).toUTCString(),
      });
      res.end(req.method === "HEAD" ? undefined : body);
   });
   await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", () => resolve()),
   );
   const address = server.address() as { port: number };
   origin = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
   await new Promise<void>((resolve) => server.close(() => resolve()));
   fs.rmSync(root, { recursive: true, force: true });
   for (const [name, value] of Object.entries(savedEnv)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
   }
});

describe("package sandbox: Malloy's sandboxed policy", () => {
   let connection: DuckDBConnection;

   beforeAll(async () => {
      const config = (
         Package as unknown as {
            buildPackageMalloyConfig(
               packagePath: string,
               env: () => MalloyConfig,
            ): MalloyConfig;
         }
      ).buildPackageMalloyConfig(
         path.join(root, "pkg"),
         () => new MalloyConfig({ connections: {} }),
      );
      connection = (await config.connections.lookupConnection(
         "duckdb",
      )) as DuckDBConnection;
   });

   afterAll(async () => {
      await connection.close();
   });

   it("reads the package's own files, relative and absolute, and .xlsx", async () => {
      expect(
         rowsOf(await connection.runSQL("SELECT * FROM 'top.csv'")),
      ).toEqual([{ z: "9" }]);
      expect(
         rowsOf(await connection.runSQL("SELECT * FROM 'data/x.csv'")),
      ).toEqual([{ a: "1", b: "2" }]);
      expect(
         rowsOf(
            await connection.runSQL(`SELECT * FROM '${root}/pkg/data/x.csv'`),
         ),
      ).toEqual([{ a: "1", b: "2" }]);
      expect(
         rowsOf(
            await connection.runSQL(
               "SELECT * FROM read_xlsx('data/book.xlsx')",
            ),
         ),
      ).toEqual([{ A1: 1, B1: "x" }]);
   });

   it("refuses traversal out of the package: `..`, relative or absolute, and a symlink", async () => {
      await refused(
         connection,
         "SELECT * FROM 'data/../../outside/secret.csv'",
      );
      await refused(
         connection,
         `SELECT * FROM '${root}/pkg/data/../../outside/secret.csv'`,
      );
      await refused(connection, "SELECT * FROM 'escape_link/secret.csv'");
      await refused(
         connection,
         `SELECT * FROM '${root}/pkg/escape_link/secret.csv'`,
      );
      await refused(
         connection,
         `SELECT * FROM glob('${root}/pkg/escape_link/*')`,
      );
   });

   it("refuses every other vector", async () => {
      await refused(connection, "LOAD httpfs");
      await expectCommonVectorsRefused(connection);
   });

   it("never shares an instance with an unsandboxed connection", async () => {
      const plain = new DuckDBConnection({
         name: "duckdb",
         databasePath: ":memory:",
         workingDirectory: path.join(root, "pkg"),
      });
      try {
         await plain.runSQL("SELECT 1");
         const keyOf = (c: unknown) => (c as { shareKey: string }).shareKey;
         expect(keyOf(plain)).not.toBe(keyOf(connection));
         expect(DuckDBConnection.activeDBs[keyOf(plain)]).not.toBe(
            DuckDBConnection.activeDBs[keyOf(connection)],
         );
         expect(
            rowsOf(
               await plain.runSQL(
                  "SELECT current_setting('lock_configuration') AS v",
               ),
            )[0]?.v,
         ).toBe(false);
         const normalized = (c: unknown) =>
            (c as { normalized: Parameters<typeof instanceKeyOf>[0] })
               .normalized;
         expect(instanceKeyOf(normalized(plain))).not.toBe(
            instanceKeyOf(normalized(connection)),
         );
      } finally {
         await plain.close();
      }
   });

   it("puts the policy in the options both share keys digest", () => {
      const options = sandboxedDuckDBOptions(path.join(root, "pkg"));
      expect(options.securityPolicy).toBe("sandboxed");
      expect(options.allowedDirectories).toContain(path.join(root, "pkg"));
      expect(options.setupSQL).toBeUndefined();
   });
});

describe("environment duckdb with setupSQL: allowlist, then lock, as options", () => {
   let restore: () => void;
   let release: () => Promise<void>;
   let connection: DuckDBConnection;
   let envDir: string;

   beforeAll(async () => {
      ({ restore } = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }));
      envDir = path.join(root, "env_local");
      fs.mkdirSync(path.join(envDir, "data"), { recursive: true });
      fs.writeFileSync(path.join(envDir, "data", "y.csv"), "c\n3\n");
      const built = buildEnvironmentMalloyConfig(
         [
            {
               name: "local_db",
               type: "duckdb",
               duckdbConnection: {
                  setupSQL: `CREATE OR REPLACE VIEW ys AS SELECT * FROM '${envDir}/data/y.csv'`,
               },
            },
         ],
         envDir,
      );
      release = built.releaseConnections;
      connection = (await built.malloyConfig.connections.lookupConnection(
         "local_db",
      )) as DuckDBConnection;
   });

   afterAll(async () => {
      await release();
      restore();
   });

   it("runs its setupSQL and reads its environment's files", async () => {
      expect(rowsOf(await connection.runSQL("SELECT * FROM ys"))).toEqual([
         { c: "3" },
      ]);
      expect(
         rowsOf(await connection.runSQL("SELECT * FROM 'data/y.csv'")),
      ).toEqual([{ c: "3" }]);
   });

   it("refuses every vector", async () => {
      await refused(connection, "LOAD httpfs");
      await expectCommonVectorsRefused(connection);
   });
});

describe("environment duckdb with an attached bucket: setup, then lock", () => {
   let release: () => Promise<void>;
   let connection: DuckDBConnection;

   beforeAll(async () => {
      const envDir = path.join(root, "env_bucket");
      fs.mkdirSync(envDir, { recursive: true });
      const built = buildEnvironmentMalloyConfig(
         [
            {
               name: "bucket_db",
               type: "duckdb",
               duckdbConnection: {
                  attachedDatabases: [
                     {
                        name: "lake_files",
                        type: "s3",
                        s3Connection: {
                           accessKeyId: "configured-key",
                           secretAccessKey: "configured-secret",
                           region: "us-east-1",
                           endpoint: origin,
                        },
                     },
                  ],
               },
            },
         ],
         envDir,
      );
      release = built.releaseConnections;
      connection = (await built.malloyConfig.connections.lookupConnection(
         "bucket_db",
      )) as DuckDBConnection;
   });

   afterAll(async () => {
      await release();
   });

   it("reads the bucket through its own secret", async () => {
      expect(
         rowsOf(
            await connection.runSQL(
               "SELECT * FROM read_csv('s3://bucket/data/x.csv')",
            ),
         ),
      ).toEqual([{ a: "7", b: "8" }]);
      // Signed with the configured key, never the environment's.
      const signed = requestsTo("/bucket/data/x.csv").map(
         (request) => request.headers.authorization ?? "",
      );
      expect(signed.length).toBeGreaterThan(0);
      expect(signed.some((auth) => auth.includes(ENV_KEY))).toBe(false);
   });

   it("refuses every vector", async () => {
      await expectCommonVectorsRefused(connection);
   });

   it("keys a different attachment set to a different instance", () => {
      const otherEnv = path.join(root, "env_bucket");
      const keyFor = (endpoint: string) =>
         instanceKeyOf(
            (
               new DuckDBConnection({
                  name: "probe",
                  databasePath: path.join(otherEnv, "bucket_db.duckdb"),
                  ...networkedDuckDBOptions("attached", endpoint),
               }) as unknown as {
                  normalized: Parameters<typeof instanceKeyOf>[0];
               }
            ).normalized,
         );
      const a = keyFor("http://a");
      const b = keyFor("http://b");
      expect(a).not.toBe(b);
      expect(differsOnlyInLockedSetup(a, b)).toBe(true);
      expect(differsOnlyInLockedSetup(a, a.replace("probe", "x"))).toBe(false);
   });
});

describe("a networked instance with a DuckLake catalog: setup, then lock", () => {
   let connection: DuckDBConnection;
   let lake: string;

   beforeAll(async () => {
      lake = path.join(root, "lake");
      fs.mkdirSync(path.join(lake, "data"), { recursive: true });
      connection = new DuckDBConnection({
         name: "lake_conn",
         databasePath: ":memory:",
         ...networkedDuckDBOptions("ducklake", "spec"),
      });
      await connection.runSQL("LOAD ducklake");
      await connection.runSQL(
         `ATTACH 'ducklake:${lake}/catalog.ducklake' AS lake (DATA_PATH '${lake}/data/')`,
      );
      await connection.runSQL("CREATE TABLE lake.t AS SELECT 1 AS v");
      allowForLockdown(connection, `${lake}/data/`);
      allowForLockdown(connection, `${origin}/bucket/org_a/`);
      await lockDownDuckDBSession(connection);
   });

   afterAll(async () => {
      await connection.close();
   });

   it("still reads and builds its own catalog, and reads its own prefix", async () => {
      await connection.runSQL("INSERT INTO lake.t VALUES (2)");
      await connection.runSQL("CREATE TABLE lake.u AS SELECT * FROM lake.t");
      expect(
         rowsOf(await connection.runSQL("SELECT count(*) AS n FROM lake.u")),
      ).toEqual([{ n: "2" }]);
      expect(
         rowsOf(
            await connection.runSQL(
               `SELECT * FROM read_csv('${origin}/bucket/org_a/x.csv')`,
            ),
         ),
      ).toEqual([{ a: "7", b: "8" }]);
   });

   it("refuses a read outside its prefix and another catalog", async () => {
      await refused(
         connection,
         `SELECT * FROM read_csv('${origin}/bucket/org_b/x.csv')`,
      );
      await refused(
         connection,
         `ATTACH 'ducklake:${root}/outside/other.ducklake' AS lake2 (DATA_PATH '${root}/outside/')`,
      );
   });

   it("refuses every vector", async () => {
      await expectCommonVectorsRefused(connection);
   });

   it("is idempotent on a locked instance", async () => {
      await lockDownDuckDBSession(connection);
   });
});

describe("azureAllowPrefix", () => {
   it("keeps the directory of a SAS or file URL, never its query", () => {
      expect(
         azureAllowPrefix(
            "https://acct.blob.core.windows.net/c/dir/*.parquet?sv=1&sig=x",
         ),
      ).toBe("https://acct.blob.core.windows.net/c/dir/");
      expect(
         azureAllowPrefix("abfss://c@acct.dfs.core.windows.net/dir/file.csv"),
      ).toBe("abfss://c@acct.dfs.core.windows.net/dir/");
      expect(azureAllowPrefix(undefined)).toBeUndefined();
   });
});
