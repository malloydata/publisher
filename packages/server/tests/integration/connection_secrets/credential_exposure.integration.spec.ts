/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

/**
 * A connection's credentials must not appear in any response body, from any
 * endpoint, ever.
 *
 * Written by behaviour rather than by location on purpose. The unit specs pin
 * the projection at the three call sites that exist today, which says nothing
 * about a fourth added later, or about a response builder that spreads a
 * connection after the projection has run. This boots a real server with a real
 * secret and sweeps every endpoint that could carry a connection, so a new leak
 * fails here even if nobody thought to update a unit test.
 *
 * The secret arrives the way the docs recommend storing one: a `${VAR}`
 * reference in the config file rather than a literal. `processConfigValue`
 * substitutes it during config load, so by the time any response is built the
 * real credential is in memory. A test using a literal would not cover that
 * step, and the env-var form is the one an operator is most likely to believe
 * is safe.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// tests/integration/connection_secrets -> packages/server
const SERVER_DIR = path.resolve(__dirname, "../../..");

const ENV_NAME = "secret-env";
const PKG_NAME = "secret-pkg";

const PG_SECRET = "S3NT1NEL-pg-password-do-not-return";
const BQ_SECRET = "S3NT1NEL-bq-private-key-do-not-return";
const SF_SECRET = "S3NT1NEL-sf-private-key-do-not-return";
const ALL_SECRETS = [PG_SECRET, BQ_SECRET, SF_SECRET];

async function getFreePort(): Promise<number> {
   return new Promise<number>((resolve, reject) => {
      const srv = net.createServer();
      srv.on("error", reject);
      srv.listen(0, "127.0.0.1", () => {
         const addr = srv.address();
         const found = typeof addr === "object" && addr ? addr.port : 0;
         srv.close(() =>
            found ? resolve(found) : reject(new Error("no free port")),
         );
      });
   });
}

async function poll(
   predicate: () => Promise<boolean>,
   timeoutMs: number,
   intervalMs = 300,
): Promise<boolean> {
   const deadline = Date.now() + timeoutMs;
   while (Date.now() < deadline) {
      if (await predicate()) return true;
      await new Promise((r) => setTimeout(r, intervalMs));
   }
   return false;
}

describe("connection credentials never reach a response body", () => {
   let proc: ChildProcess | undefined;
   let exited = false;
   let combinedLog = "";
   let serverRoot = "";
   let srcDir = "";
   let port = 0;

   const url = (suffix: string) => `http://127.0.0.1:${port}${suffix}`;

   beforeAll(async () => {
      srcDir = fs.mkdtempSync(path.join(os.tmpdir(), "secret-src-"));
      fs.writeFileSync(
         path.join(srcDir, "publisher.json"),
         JSON.stringify({ name: PKG_NAME }),
      );
      fs.writeFileSync(
         path.join(srcDir, "first.malloy"),
         'source: first_source is duckdb.sql("SELECT 1 as n")\n',
      );

      serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "secret-root-"));
      fs.writeFileSync(
         path.join(serverRoot, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: ENV_NAME,
                  packages: [{ name: PKG_NAME, location: srcDir }],
                  connections: [
                     {
                        name: "pg_conn",
                        type: "postgres",
                        postgresConnection: {
                           host: "127.0.0.1",
                           port: 5432,
                           databaseName: "analytics",
                           userName: "reader",
                           password: "${TEST_PG_PASSWORD}",
                        },
                     },
                     {
                        name: "bq_conn",
                        type: "bigquery",
                        bigqueryConnection: {
                           defaultProjectId: "demo-project",
                           serviceAccountKeyJson: JSON.stringify({
                              type: "service_account",
                              project_id: "demo-project",
                              client_email: "demo@demo.iam.gserviceaccount.com",
                              private_key: "${TEST_BQ_PRIVATE_KEY}",
                           }),
                        },
                     },
                     {
                        name: "sf_conn",
                        type: "snowflake",
                        snowflakeConnection: {
                           account: "acct",
                           username: "reader",
                           warehouse: "WH",
                           privateKey: "${TEST_SF_PRIVATE_KEY}",
                        },
                     },
                  ],
               },
            ],
         }),
      );

      port = await getFreePort();
      let mcpPort = await getFreePort();
      while (mcpPort === port) mcpPort = await getFreePort();

      proc = spawn("bun", ["src/server.ts"], {
         cwd: SERVER_DIR,
         env: {
            ...process.env,
            SERVER_ROOT: serverRoot,
            PUBLISHER_HOST: "127.0.0.1",
            PUBLISHER_PORT: String(port),
            MCP_PORT: String(mcpPort),
            TEST_PG_PASSWORD: PG_SECRET,
            TEST_BQ_PRIVATE_KEY: BQ_SECRET,
            TEST_SF_PRIVATE_KEY: SF_SECRET,
         },
         stdio: ["ignore", "pipe", "pipe"],
      });
      proc.stdout?.on("data", (d: Buffer) => {
         combinedLog += d.toString();
      });
      proc.stderr?.on("data", (d: Buffer) => {
         combinedLog += d.toString();
      });
      proc.on("exit", () => {
         exited = true;
      });

      const serving = await poll(async () => {
         if (exited) throw new Error("server exited before becoming ready");
         try {
            const res = await fetch(url("/api/v0/status"));
            if (!res.ok) return false;
            const body = (await res.json()) as { operationalState?: string };
            return body.operationalState === "serving";
         } catch {
            return false;
         }
      }, 120_000);
      if (!serving) {
         throw new Error(
            `server did not reach serving within 120s\n--- log tail ---\n${combinedLog.slice(-4000)}`,
         );
      }
   }, 180_000);

   afterAll(async () => {
      if (proc && !exited) {
         proc.kill("SIGKILL");
         await new Promise((r) => setTimeout(r, 200));
      }
      for (const dir of [serverRoot, srcDir]) {
         if (dir) fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   /**
    * Guards the sweep below from passing vacuously. If the config failed to
    * load, or the `${VAR}` reference was never substituted, no response would
    * contain a secret and every assertion would pass while proving nothing.
    * `withheldFields` is the observable that says the server really is holding
    * these credentials and deliberately not returning them.
    */
   it("loaded all three credentials, so the sweep is not vacuous", async () => {
      const res = await fetch(
         url(`/api/v0/environments/${ENV_NAME}/connections`),
      );
      expect(res.status).toBe(200);
      const connections = (await res.json()) as Array<{
         name: string;
         withheldFields?: string[];
      }>;
      const withheld = Object.fromEntries(
         connections.map((c) => [c.name, c.withheldFields ?? []]),
      );
      expect(withheld["pg_conn"]).toContain("postgresConnection.password");
      expect(withheld["bq_conn"]).toContain(
         "bigqueryConnection.serviceAccountKeyJson",
      );
      expect(withheld["sf_conn"]).toContain("snowflakeConnection.privateKey");
   });

   it("returns no credential from any endpoint that can carry a connection", async () => {
      const paths = [
         "/api/v0/status",
         "/api/v0/environments",
         `/api/v0/environments/${ENV_NAME}`,
         `/api/v0/environments/${ENV_NAME}/connections`,
         `/api/v0/environments/${ENV_NAME}/connections/pg_conn`,
         `/api/v0/environments/${ENV_NAME}/connections/bq_conn`,
         `/api/v0/environments/${ENV_NAME}/connections/sf_conn`,
         `/api/v0/environments/${ENV_NAME}/packages`,
      ];

      for (const suffix of paths) {
         const res = await fetch(url(suffix));
         const body = await res.text();
         for (const secret of ALL_SECRETS) {
            // Named per path so a failure says which endpoint leaked.
            expect({ path: suffix, leaked: body.includes(secret) }).toEqual({
               path: suffix,
               leaked: false,
            });
         }
      }
   });

   it("does not write a credential to the server log", async () => {
      for (const secret of ALL_SECRETS) {
         expect(combinedLog.includes(secret)).toBe(false);
      }
   });

   it("keeps the credential when an update omits it", async () => {
      // The other half of the same contract: a caller cannot echo back a
      // credential it never received, so omission must not read as deletion.
      // Checked through withheldFields rather than by reading the secret, since
      // reading it is the thing being prevented.
      const patch = await fetch(
         url(`/api/v0/environments/${ENV_NAME}/connections/pg_conn`),
         {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: "pg_conn",
               type: "postgres",
               postgresConnection: {
                  host: "127.0.0.2",
                  port: 5432,
                  databaseName: "analytics",
                  userName: "reader",
               },
            }),
         },
      );
      expect(patch.status).toBe(200);

      const res = await fetch(
         url(`/api/v0/environments/${ENV_NAME}/connections/pg_conn`),
      );
      const connection = (await res.json()) as {
         withheldFields?: string[];
         postgresConnection?: { host?: string };
      };
      expect(connection.postgresConnection?.host).toBe("127.0.0.2");
      expect(connection.withheldFields ?? []).toContain(
         "postgresConnection.password",
      );

      const status = await fetch(url("/api/v0/status"));
      const statusBody = await status.text();
      expect(statusBody.includes(PG_SECRET)).toBe(false);
   });
});
