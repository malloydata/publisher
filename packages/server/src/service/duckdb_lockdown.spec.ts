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
   runAsInternalStatement,
   sandboxedDuckDBOptions,
} from "./duckdb_lockdown";
import { unrecordedAudit } from "./access_record";
import { QueryPrivileges } from "./model_query_request";
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

async function refusedStream(
   connection: DuckDBConnection,
   sql: string,
): Promise<string> {
   try {
      for await (const _row of connection.runSQLStream(sql)) {
         // Drained: a refusal throws before the first row.
      }
   } catch (error) {
      return error instanceof Error ? error.message : String(error);
   }
   throw new Error(`expected DuckDB to refuse (stream): ${sql}`);
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
   // Every secret statement a caller can write is refused before it runs: a
   // credential_chain secret (host credentials), an http secret carrying the
   // metadata server's header or a bearer token, an s3 secret pointing its
   // own ENDPOINT anywhere, and every spelling of CREATE and DROP.
   for (const secret of [
      "CREATE SECRET lockdown_chain (TYPE s3, PROVIDER credential_chain)",
      `CREATE SECRET lockdown_md (TYPE http, EXTRA_HTTP_HEADERS MAP {'Metadata-Flavor': 'Google'}, SCOPE '${origin}')`,
      `CREATE SECRET lockdown_bearer (TYPE http, BEARER_TOKEN 'tok', SCOPE '${origin}/b')`,
      `CREATE SECRET lockdown_ep (TYPE s3, KEY_ID 'k', SECRET 's', ENDPOINT '${origin.replace("http://", "")}', URL_STYLE 'path', USE_SSL false)`,
      "create or replace persistent secret x (TYPE http, BEARER_TOKEN 't')",
      "CREATE TEMPORARY SECRET IF NOT EXISTS x (TYPE http, BEARER_TOKEN 't')",
      'CREATE /* hidden */ "SECRET" x (TYPE http)',
      "SELECT 1; CREATE SECRET x (TYPE http)",
      "PREPARE p AS CREATE SECRET x (TYPE http)",
      "DROP SECRET IF EXISTS secret_s3_lake_files",
      "DROP PERSISTENT SECRET x",
   ]) {
      expect(await refused(connection, secret)).toContain("locked down");
   }
   // DuckDB's logging and profiling, which its configuration lock does not
   // cover, in every form: a call, a table function, the log views, a pragma,
   // any casing or quoting, behind a comment, after another statement, and
   // as text handed to query().
   for (const statement of [
      "CALL enable_logging(level='trace')",
      "SELECT * FROM enable_logging()",
      "CaLl /* x */ \"ENABLE_LOGGING\"('HTTP')",
      "SELECT 1; CALL enable_logging()",
      "SELECT * FROM duckdb_logs",
      "SELECT * FROM duckdb_log_contexts()",
      "CALL truncate_duckdb_logs()",
      "CALL disable_logging()",
      "PRAGMA enable_profiling",
      "PRAGMA enable_profile",
      "SELECT write_log('x')",
      "SELECT * FROM query('SELECT * FROM duckdb_logs')",
      "SELECT * FROM query_table('duckdb_' || 'logs')",
   ]) {
      expect(await refused(connection, statement)).toContain("locked down");
   }
   // Hiding a statement from a reader that splits SQL differently from DuckDB:
   // a line comment DuckDB ends at a carriage return, a dollar-quote tag with
   // digits or non-ASCII, SQL run from its serialized form, and a database
   // import that runs the SQL files it reads. Each refused through runSQL and
   // through runSQLStream.
   const exportDir = await fs.promises.mkdtemp(path.join(root, "export-"));
   for (const statement of [
      "SELECT 1 AS a --x\r, (SELECT count(*) FROM enable_logging(level:='trace')) AS b",
      "SELECT 1 --\r; CREATE SECRET md (TYPE http, BEARER_TOKEN 't')",
      "SELECT $a1$ -- $a1$ AS s, (SELECT count(*) FROM duckdb_logs) AS hits",
      "SELECT $é$ -- $é$ AS s, (SELECT count(*) FROM duckdb_logs) AS hits",
      "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1'))",
      "PRAGMA json_execute_serialized_sql('{}')",
      `IMPORT DATABASE '${exportDir}'`,
      `PRAGMA import_database('${exportDir}')`,
   ]) {
      expect(await refused(connection, statement)).toContain("locked down");
      expect(await refusedStream(connection, statement)).toContain(
         "locked down",
      );
   }
   // Nothing Malloy's own separator could put in front of a query but a
   // macro: author text that spells it cannot slip a write ahead of the query.
   expect(
      await refused(
         connection,
         `COPY (SELECT 1) TO '${root}/pkg/x.csv'\n-- hack: split on this\nSELECT 1`,
      ),
   ).toContain("locked down");
   // A .sql() source is one SELECT, decided by DuckDB's parser.
   const sqlSource = (selectStr: string) =>
      (
         connection as unknown as {
            fetchSchemaForSQLStruct(
               ref: { connection: string; selectStr: string },
               options: { refreshTimestamp?: number },
            ): Promise<{ structDef?: unknown; error?: string }>;
         }
      ).fetchSchemaForSQLStruct(
         { connection: connection.name, selectStr },
         { refreshTimestamp: Date.now() },
      );
   for (const selectStr of [
      `COPY (SELECT 1) TO '${root}/pkg/y.csv'`,
      "SELECT 1; SELECT 2",
      "INSERT INTO t VALUES (1)",
   ]) {
      expect((await sqlSource(selectStr)).error).toContain(
         "runs only as a single SELECT",
      );
   }
   expect(
      (await sqlSource("WITH x AS (SELECT 1 AS a) SELECT a FROM x")).structDef,
   ).toBeDefined();
   // A name shared with a refused function is not refused as a column.
   expect(
      rowsOf(await connection.runSQL("SELECT 1 AS query, 2 AS duckdb_log")),
   ).toEqual([{ query: 1, duckdb_log: 2 }]);
   // Nothing the refused statements could have set up reaches the metadata
   // stand-in.
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
      // Off POSIX, the same confinement as options (no `sandboxed` there).
      if (process.platform === "win32") {
         expect(options.lockConfiguration).toBe(true);
         expect(options.enableExternalAccess).toBe(false);
      } else {
         expect(options.securityPolicy).toBe("sandboxed");
      }
      expect(options.allowedDirectories).toContain(path.join(root, "pkg"));
      expect(options.setupSQL).toBeUndefined();
   });
});

describe.skipIf(process.platform !== "win32")(
   "package sandbox on Windows: the allowlist across path spellings",
   () => {
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
         // A junction (no privilege needed on Windows) out of the package.
         fs.symlinkSync(
            path.join(root, "outside"),
            path.join(root, "pkg", "junction_out"),
            "junction",
         );
      });

      afterAll(async () => {
         await connection.close();
      });

      it("reads the package's own files with either separator", async () => {
         const inside = path.join(root, "pkg", "data", "x.csv");
         for (const spelling of [inside, inside.replace(/\\/g, "/")]) {
            expect(
               rowsOf(await connection.runSQL(`SELECT * FROM '${spelling}'`)),
            ).toEqual([{ a: "1", b: "2" }]);
         }
      });

      it("refuses a file outside the package however its path is spelled", async () => {
         const outside = path.join(root, "outside", "secret.csv");
         for (const spelling of [
            outside,
            outside.replace(/\\/g, "/"),
            outside.toUpperCase(),
            outside.toLowerCase(),
            `\\\\?\\${outside}`,
            path.join(root, "pkg", "..", "outside", "secret.csv"),
            `${path.join(root, "pkg")}/../outside\\secret.csv`,
            path.join(root, "pkg", "junction_out", "secret.csv"),
            "junction_out/secret.csv",
         ]) {
            await refused(connection, `SELECT * FROM '${spelling}'`);
         }
      });
   },
);

describe("author SQL in a model on a DuckDB connection reads as one SELECT", () => {
   const load = async (model: string) => {
      const dir = fs.mkdtempSync(path.join(root, "authorsql-"));
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "authorsql" }),
      );
      fs.writeFileSync(
         path.join(dir, "m.malloy"),
         `##! experimental.sql_functions\n${model}`,
      );
      return Package.create("env", "authorsql", dir, new Map());
   };

   it("refuses a .sql() source or a raw SQL expression that ends the query", async () => {
      for (const model of [
         `source: s is duckdb.sql("""SELECT 1 AS a); COPY (SELECT 1) TO 'x.csv'; SELECT (1""")`,
         `source: s is duckdb.sql("SELECT 1 AS a") extend { dimension: d is sql_number("1); COPY (SELECT 1) TO 'x.csv'; SELECT (1") }`,
      ]) {
         await expect(load(model)).rejects.toThrow(/(one|single) SELECT/);
      }
   });

   it("loads one SELECT, CTEs included, and a plain raw expression", async () => {
      const pkg = await load(
         `source: s is duckdb.sql("WITH x AS (SELECT 1 AS a) SELECT a FROM x") extend { dimension: d is sql_number("a + 1") }`,
      );
      expect(pkg.getModel("m.malloy")).toBeDefined();
      await pkg.getMalloyConfig().releaseConnections();
   });
});

describe("an argument interpolated into a raw SQL expression cannot end the statement", () => {
   let restore: () => void;
   beforeAll(() => {
      ({ restore } = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }));
   });
   afterAll(() => restore());

   /** A string dimension whose value, interpolated into a dollar quote, ends it. */
   const model = (connection: string, target: string) =>
      [
         "##! experimental.sql_functions",
         `source: s is ${connection}.sql("SELECT 1 AS a") extend {`,
         `  dimension: inj is '$q$; COPY (SELECT 42 AS x) TO $w$${target}$w$; SELECT $q$'`,
         '  dimension: d is sql_string("$q$${inj}$q$")',
         "}",
         "query: q is s -> { group_by: d }",
         "",
      ].join("\n");
   const adHoc = (target: string) =>
      `run: s extend { dimension: inj is '$q$; COPY (SELECT 1 AS x) TO $w$${target}$w$; SELECT $q$' dimension: d is sql_string("$q$\${inj}$q$") } -> { group_by: d }`;

   const packageAt = (dir: string, text: string) => {
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: path.basename(dir) }),
      );
      fs.writeFileSync(path.join(dir, "m.malloy"), text);
   };

   for (const tier of ["sandbox", "env"] as const) {
      it(`refuses it at load and at run time on the ${tier} tier, and writes nothing`, async () => {
         const base = path.join(root, `interp-${tier}`);
         const envDir = path.join(base, "env");
         const pkgDir = path.join(envDir, "pkg");
         const target = path.join(pkgDir, "written.csv");
         let release = async () => {};
         let connections: Map<string, unknown> | MalloyConfig = new Map();
         let connection = "duckdb";
         if (tier === "env") {
            fs.mkdirSync(envDir, { recursive: true });
            const built = buildEnvironmentMalloyConfig(
               [
                  {
                     name: "local_db",
                     type: "duckdb",
                     duckdbConnection: { setupSQL: "SELECT 1" },
                  },
               ],
               envDir,
            );
            release = built.releaseConnections;
            connections = built.malloyConfig;
            connection = "local_db";
         }
         try {
            // At load: the argument reads back as no column reference.
            packageAt(pkgDir, model(connection, target));
            await expect(
               Package.create("env", "pkg", pkgDir, connections as never),
            ).rejects.toThrow(/one SELECT/);
            // At run time: an ad-hoc query is never loaded, and its extra
            // statement is refused on the locked instance.
            packageAt(
               pkgDir,
               `source: s is ${connection}.sql("SELECT 1 AS a")\n`,
            );
            const pkg = await Package.create(
               "env",
               "pkg",
               pkgDir,
               connections as never,
            );
            await expect(
               pkg.getModel("m.malloy")!.runQuery(
                  unrecordedAudit(),
                  {
                     query: adHoc(target),
                     filterParams: {},
                     givens: {},
                  } as never,
                  QueryPrivileges.none,
               ),
            ).rejects.toThrow();
            expect(fs.existsSync(target)).toBe(false);
            await pkg.getMalloyConfig().releaseConnections();
         } finally {
            await release();
         }
      });
   }
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

   it("admits Publisher's own secret renewal, and nothing a caller sends while it runs", async () => {
      const internal =
         "CREATE OR REPLACE SECRET internal_renewal (TYPE http, BEARER_TOKEN 'renewed')";
      const caller =
         "CREATE SECRET caller_secret (TYPE http, BEARER_TOKEN 'caller')";
      let release!: () => void;
      const held = new Promise<void>((resolve) => (release = resolve));
      // A renewal in flight, holding the marker on its own async chain...
      const renewing = runAsInternalStatement(internal, async () => {
         await held;
         await connection.runSQL(internal);
      });
      // ...while callers, on theirs, send another secret and the very same text.
      const other = refused(connection, caller);
      const sameText = refused(connection, internal);
      release();
      await renewing;
      expect(await other).toContain("locked down");
      expect(await sameText).toContain("locked down");
      // Inside the renewal's own scope, only its own statement passes.
      expect(
         await runAsInternalStatement(internal, () =>
            refused(connection, caller),
         ),
      ).toContain("locked down");
      const names = rowsOf(
         await connection.runSQL("SELECT name FROM duckdb_secrets()"),
      ).map((row) => row.name);
      expect(names).toContain("internal_renewal");
      expect(names).not.toContain("caller_secret");
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

describe("a MotherDuck connection whose lock fails", () => {
   /** A session that answers like an unlocked instance and fails on `failOn`. */
   const stubSession = (failOn: RegExp) =>
      ({
         runSQL: async (sql: string) => {
            if (failOn.test(sql))
               throw new Error("simulated MotherDuck failure");
            if (/current_setting\('enable_external_access'\)/.test(sql)) {
               return { rows: [{ v: true }] };
            }
            if (/current_setting\('temp_directory'\)/.test(sql)) {
               return { rows: [{ v: "" }] };
            }
            return { rows: [{ v: false }] };
         },
      }) as unknown as DuckDBConnection;

   for (const [step, failOn] of [
      ["secret manager start", /duckdb_secrets\(\)/],
      ["allowlist", /allowed_directories/],
      ["enable_external_access=false", /SET enable_external_access=false/],
      ["lock_configuration", /SET lock_configuration=true/],
      ["a catalog read after the lock", /information_schema\.schemata/],
   ] as const) {
      it(`fails by name, at ${step}, rather than come up half-locked`, async () => {
         await expect(
            lockDownDuckDBSession(stubSession(failOn), {
               motherDuck: "md_prod",
            }),
         ).rejects.toThrow(
            `MotherDuck connection 'md_prod' could not be locked down (step: ${step})`,
         );
      });
   }
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
