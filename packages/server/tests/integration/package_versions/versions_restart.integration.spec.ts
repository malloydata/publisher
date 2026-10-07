// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

/**
 * Published versions across a real restart: a server process publishes,
 * builds, moves `latest`, archives and binds; it is stopped; a second process
 * on the same server root must serve exactly what the first left behind, and
 * go on publishing from there.
 *
 * Each version's model answers a different number, so which version served a
 * request can be read off the answer.
 *
 * Not covered here: a version reading materialized tables after the restart.
 * A package's own `duckdb` is in-memory and does not outlive the process, and
 * an environment DuckDB connection must attach an external database, so there
 * is no local store a restart test can build durable tables into. The scope
 * rules for a version's restored bindings are pinned by the unit tests of
 * `MaterializationService` (rebinds each loaded version from the runs that own
 * its tables).
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SERVER_DIR = path.resolve(__dirname, "../../..");
const ENV_NAME = "restart-env";
const json = { "Content-Type": "application/json" };

setDefaultTimeout(300_000);

async function freePort(): Promise<number> {
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
   intervalMs = 250,
): Promise<boolean> {
   const deadline = Date.now() + timeoutMs;
   while (Date.now() < deadline) {
      if (await predicate()) return true;
      await new Promise((r) => setTimeout(r, intervalMs));
   }
   return false;
}

/** One server process on `serverRoot`, with package versioning on. */
class Server {
   private proc: ChildProcess | undefined;
   private exited = false;
   log = "";
   baseUrl = "";

   constructor(private serverRoot: string) {}

   async start(): Promise<void> {
      const port = await freePort();
      let mcpPort = await freePort();
      while (mcpPort === port) mcpPort = await freePort();
      this.baseUrl = `http://127.0.0.1:${port}`;
      this.exited = false;
      this.proc = spawn("bun", ["src/server.ts"], {
         cwd: SERVER_DIR,
         env: {
            ...process.env,
            SERVER_ROOT: this.serverRoot,
            PUBLISHER_HOST: "127.0.0.1",
            PUBLISHER_PORT: String(port),
            MCP_PORT: String(mcpPort),
            PUBLISHER_PACKAGE_VERSIONING: "on",
            PUBLISHER_NO_MCP_CONFIG: "1",
         },
         stdio: ["ignore", "pipe", "pipe"],
      });
      const keep = (d: Buffer) => {
         this.log = (this.log + d.toString()).slice(-12000);
      };
      this.proc.stdout?.on("data", keep);
      this.proc.stderr?.on("data", keep);
      this.proc.on("exit", () => {
         this.exited = true;
      });
      const serving = await poll(async () => {
         if (this.exited) throw new Error(`server exited:\n${this.log}`);
         try {
            const res = await fetch(`${this.baseUrl}/api/v0/status`);
            if (!res.ok) return false;
            const body = (await res.json()) as {
               operationalState?: string;
               loadErrors?: unknown[];
            };
            if (body.loadErrors?.length) {
               throw new Error(
                  `server reported load errors: ${JSON.stringify(body.loadErrors)}`,
               );
            }
            return body.operationalState === "serving";
         } catch (error) {
            if (String(error).includes("load errors")) throw error;
            return false;
         }
      }, 150_000);
      if (!serving) throw new Error(`server did not serve:\n${this.log}`);
   }

   async stop(): Promise<void> {
      const proc = this.proc;
      if (!proc || this.exited) return;
      await new Promise<void>((resolve) => {
         const backstop = setTimeout(() => proc.kill("SIGKILL"), 10_000);
         proc.on("exit", () => {
            clearTimeout(backstop);
            resolve();
         });
         proc.kill("SIGTERM");
      });
   }
}

describe("published versions across a restart", () => {
   let server: Server;
   let serverRoot: string;
   let srcRoot: string;
   let manifestFile: string;
   const runs: Record<string, string> = {};

   const pkgApi = (name: string) =>
      `${server.baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`;

   function packageDir(
      name: string,
      version: string,
      n: number,
      scope?: "version" | "package",
   ): string {
      const dir = fs.mkdtempSync(path.join(srcRoot, `${name}-`));
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name,
            version,
            ...(scope ? { materialization: { scope } } : {}),
         }),
      );
      fs.writeFileSync(
         path.join(dir, "model.malloy"),
         [
            "##! experimental.persistence",
            `source: base is duckdb.sql("SELECT ${n} as n")`,
            `#@ persist name="summary"`,
            "source: summary is base -> { group_by: n }",
            "",
         ].join("\n"),
      );
      return dir;
   }

   async function publish(
      name: string,
      version: string,
      n: number,
      scope?: "version" | "package",
   ): Promise<{ status: number; reason?: string }> {
      const res = await fetch(
         `${server.baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               name,
               location: packageDir(name, version, n, scope),
            }),
         },
      );
      const body = (await res.json()) as { reason?: string };
      return { status: res.status, reason: body.reason };
   }

   async function n(name: string, versionId?: string): Promise<number> {
      const res = await fetch(`${pkgApi(name)}/models/model.malloy/query`, {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            query: "run: base -> { select: n }",
            compactJson: true,
            ...(versionId ? { versionId } : {}),
         }),
      });
      expect(res.status).toBe(200);
      return JSON.parse(((await res.json()) as { result: string }).result)[0].n;
   }

   async function build(name: string, versionId?: string): Promise<string> {
      const res = await fetch(`${pkgApi(name)}/materializations`, {
         method: "POST",
         headers: json,
         body: JSON.stringify(versionId ? { versionId } : {}),
      });
      expect(res.status).toBe(201);
      const { id } = (await res.json()) as { id: string };
      let status = "PENDING";
      await poll(async () => {
         const r = await fetch(`${pkgApi(name)}/materializations/${id}`);
         status = ((await r.json()) as { status: string }).status;
         return ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(status);
      }, 120_000);
      expect(status).toBe("MANIFEST_FILE_READY");
      return id;
   }

   async function versions(name: string): Promise<[string, boolean, string][]> {
      const res = await fetch(`${pkgApi(name)}/versions`);
      expect(res.status).toBe(200);
      return (
         (await res.json()) as {
            id: string;
            latest: boolean;
            archiveStatus: string;
         }[]
      ).map((v) => [v.id, v.latest, v.archiveStatus]);
   }

   beforeAll(async () => {
      serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "versions-restart-"));
      srcRoot = fs.mkdtempSync(path.join(os.tmpdir(), "versions-restart-src-"));
      // A configured environment needs a package to load at all, so it gets an
      // unversioned one; everything under test is published at runtime.
      const anchor = fs.mkdtempSync(path.join(srcRoot, "anchor-"));
      fs.writeFileSync(
         path.join(anchor, "publisher.json"),
         JSON.stringify({ name: "anchor" }),
      );
      fs.writeFileSync(
         path.join(anchor, "anchor.malloy"),
         'source: anchor is duckdb.sql("SELECT 1 as n")\n',
      );
      fs.writeFileSync(
         path.join(serverRoot, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: ENV_NAME,
                  packages: [{ name: "anchor", location: anchor }],
                  connections: [],
               },
            ],
         }),
      );
      manifestFile = path.join(srcRoot, "manifest.json");
      fs.writeFileSync(
         manifestFile,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {},
         }),
      );

      // ---- First process: publish, build, move latest, archive, bind. ----
      server = new Server(serverRoot);
      await server.start();

      expect(await publish("kept", "1.0.0", 7)).toEqual({ status: 200 });
      expect(await publish("kept", "1.1.0", 8)).toEqual({ status: 200 });
      expect(await publish("kept", "1.2.0", 9)).toEqual({ status: 200 });
      const rollback = await fetch(`${pkgApi("kept")}/latest`, {
         method: "PUT",
         headers: json,
         body: JSON.stringify({ versionId: "1.1.0" }),
      });
      expect(rollback.status).toBe(200);
      const archived = await fetch(`${pkgApi("kept")}/versions/1.2.0`, {
         method: "PATCH",
         headers: json,
         body: JSON.stringify({ archiveStatus: "archive" }),
      });
      expect(archived.status).toBe(200);
      const bound = await fetch(`${pkgApi("kept")}/versions/1.0.0/manifest`, {
         method: "PUT",
         headers: json,
         body: JSON.stringify({ manifestLocation: manifestFile }),
      });
      expect(bound.status).toBe(200);

      expect(await publish("owned", "1.0.0", 1, "version")).toEqual({
         status: 200,
      });
      expect(await publish("owned", "1.1.0", 2, "version")).toEqual({
         status: 200,
      });
      runs.owned100 = await build("owned", "1.0.0");
      runs.owned110 = await build("owned");

      await server.stop();

      // ---- Second process, same server root. ----
      await server.start();
   });

   afterAll(async () => {
      await server?.stop();
      for (const dir of [serverRoot, srcRoot]) {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("keeps every version, latest where it was moved, and an archived version archived", async () => {
      expect(await versions("kept")).toEqual([
         ["1.2.0", false, "archive"],
         ["1.1.0", true, "unarchive"],
         ["1.0.0", false, "unarchive"],
      ]);
      expect(await n("kept")).toBe(8);
      expect(await n("kept", "1.0.0")).toBe(7);
      const archived = await fetch(
         `${pkgApi("kept")}/models/model.malloy/query`,
         {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               query: "run: base -> { select: n }",
               versionId: "1.2.0",
            }),
         },
      );
      expect(archived.status).toBe(410);
   });

   it("keeps a version's manifest binding, and only that version's", async () => {
      const bound = (await (
         await fetch(`${pkgApi("kept")}?versionId=1.0.0`)
      ).json()) as { manifestLocation?: string; boundManifestUri?: string };
      expect(bound.manifestLocation).toBe(manifestFile);
      expect(bound.boundManifestUri).toBe(manifestFile);
      const other = (await (
         await fetch(`${pkgApi("kept")}?versionId=1.1.0`)
      ).json()) as { manifestLocation?: string | null };
      expect(other.manifestLocation ?? null).toBeNull();
   });

   it("lists each version's materialization runs, which kept their version", async () => {
      const list = async (versionId?: string) =>
         (
            (await (
               await fetch(
                  `${pkgApi("owned")}/materializations${
                     versionId ? `?versionId=${versionId}` : ""
                  }`,
               )
            ).json()) as { id: string; metadata?: { versionId?: string } }[]
         ).map((r) => [r.id, r.metadata?.versionId]);
      expect(await list("1.0.0")).toEqual([[runs.owned100, "1.0.0"]]);
      expect(await list()).toEqual([[runs.owned110, "1.1.0"]]);
   });

   it("still knows what was published: the same content again is placement, different content a conflict", async () => {
      expect(await publish("kept", "1.0.0", 7)).toEqual({ status: 200 });
      expect(await publish("kept", "1.0.0", 70)).toEqual({
         status: 409,
         reason: "VERSION_CONFLICT",
      });
      // Placement moved nothing.
      expect(await n("kept")).toBe(8);
   });

   it("publishes on from where it left off", async () => {
      expect(await publish("kept", "1.3.0", 10)).toEqual({ status: 200 });
      expect(await n("kept")).toBe(10);
      expect((await versions("kept")).map(([id]) => id)).toEqual([
         "1.3.0",
         "1.2.0",
         "1.1.0",
         "1.0.0",
      ]);
   });
});
