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
      // The legacy /projects aliases are included because they are registered
      // on the same app and answer from the same serializer, so a sweep of the
      // /environments paths alone would report a surface narrower than the one
      // a caller can actually reach.
      const paths = [
         "/api/v0/status",
         "/api/v0/environments",
         `/api/v0/environments/${ENV_NAME}`,
         `/api/v0/environments/${ENV_NAME}/connections`,
         `/api/v0/environments/${ENV_NAME}/connections/pg_conn`,
         `/api/v0/environments/${ENV_NAME}/connections/bq_conn`,
         `/api/v0/environments/${ENV_NAME}/connections/sf_conn`,
         `/api/v0/environments/${ENV_NAME}/packages`,
         "/api/v0/projects",
         `/api/v0/projects/${ENV_NAME}`,
         `/api/v0/projects/${ENV_NAME}/connections`,
         `/api/v0/projects/${ENV_NAME}/connections/pg_conn`,
      ];

      for (const suffix of paths) {
         const res = await fetch(url(suffix));
         // Asserted, not assumed. A path that 404s returns a body with no
         // secret in it, so without this the sweep would report "clean" for a
         // route it never actually reached, and would keep reporting clean if a
         // route were renamed.
         expect({ path: suffix, status: res.status }).toEqual({
            path: suffix,
            status: 200,
         });
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

   it("keeps every credential when the environment PATCH rewrites the whole list", async () => {
      // This is the path the connections UI writes through: it GETs the list,
      // edits one entry, and PATCHes the WHOLE list back, so every other
      // connection arrives as the credential-free view the read returned. The
      // per-connection PATCH test below does not reach the merge that protects
      // this, and neither does the unit spec, which exercises the function
      // against literals. Without that merge a single add, edit or delete in
      // the app strips the credentials of every connection in the environment
      // and answers 200, which is the worst defect this change fixes and the
      // one a refactor could undo in silence.
      const before = await fetch(
         url(`/api/v0/environments/${ENV_NAME}/connections`),
      );
      expect(before.status).toBe(200);
      const original = (await before.json()) as Array<{
         name: string;
         withheldFields?: string[];
      }>;
      const originalWithheld = Object.fromEntries(
         original.map((c) => [c.name, c.withheldFields ?? []]),
      );
      // Guard: the assertion below is only meaningful if there was something to
      // lose in the first place.
      expect(
         Object.values(originalWithheld).filter((w) => w.length > 0).length,
      ).toBeGreaterThanOrEqual(3);

      const patch = await fetch(url(`/api/v0/environments/${ENV_NAME}`), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            connections: [
               ...original,
               {
                  name: "added_pg",
                  type: "postgres",
                  postgresConnection: {
                     host: "127.0.0.3",
                     port: 5432,
                     databaseName: "extra",
                     userName: "reader",
                     password: "added-connection-password",
                  },
               },
            ],
         }),
      });
      expect(patch.status).toBe(200);

      const after = await fetch(
         url(`/api/v0/environments/${ENV_NAME}/connections`),
      );
      expect(after.status).toBe(200);
      const now = (await after.json()) as Array<{
         name: string;
         withheldFields?: string[];
      }>;
      const nowWithheld = Object.fromEntries(
         now.map((c) => [c.name, c.withheldFields ?? []]),
      );

      // Every connection that held a credential before must still hold it.
      // withheldFields is the observable for that, since reading the credential
      // is the thing being prevented.
      for (const [name, withheld] of Object.entries(originalWithheld)) {
         expect({ name, withheld: nowWithheld[name] ?? [] }).toEqual({
            name,
            withheld,
         });
      }

      // And the rewrite must not have leaked anything on the way through.
      const statusBody = await (await fetch(url("/api/v0/status"))).text();
      for (const secret of ALL_SECRETS) {
         expect(statusBody.includes(secret)).toBe(false);
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
