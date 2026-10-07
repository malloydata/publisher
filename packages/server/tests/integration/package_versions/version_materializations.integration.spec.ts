// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Materializations of a versioned package, over real HTTP. A run builds one
 * version, and records it; who owns the tables it builds is the package's
 * materialization scope:
 *
 *  - `scope: version`: each version builds into tables of its own, named for
 *    it, and archiving a version reclaims them.
 *  - `scope: package` (the default): the versions share the package's tables,
 *    so only latest is auto-run, and a version whose source is defined
 *    differently never serves another version's table.
 *
 * Each version's persisted source answers a different number, so which table
 * (or live query) served a request can be read off the answer.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "version-materializations-env";

interface Run {
   id: string;
   status: string;
   metadata?: Record<string, unknown> | null;
   manifest?: {
      entries?: Record<string, { physicalTableName?: string }>;
   } | null;
}

describe("materializations of a versioned package", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scratch: string;
   const savedVersioning = process.env.PUBLISHER_PACKAGE_VERSIONING;
   const json = { "Content-Type": "application/json" };

   const api = (pkg: string, sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${pkg}${sub}`;

   async function publish(
      name: string,
      version: string,
      n: number,
      scope?: "version" | "package",
   ): Promise<void> {
      const dir = await fs.mkdtemp(path.join(scratch, `${name}-`));
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name,
            version,
            ...(scope ? { materialization: { scope } } : {}),
         }),
      );
      await fs.writeFile(
         path.join(dir, "model.malloy"),
         [
            "##! experimental.persistence",
            `source: base is duckdb.sql("SELECT ${n} as n")`,
            `#@ persist name="summary"`,
            "source: summary is base -> { group_by: n }",
            "",
         ].join("\n"),
      );
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
         {
            method: "POST",
            headers: json,
            body: JSON.stringify({ name, location: dir }),
         },
      );
      expect(res.status).toBe(200);
   }

   async function n(pkg: string, versionId?: string): Promise<number> {
      const res = await fetch(api(pkg, "/models/model.malloy/query"), {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            query: "run: summary -> { select: n }",
            compactJson: true,
            ...(versionId ? { versionId } : {}),
         }),
      });
      expect(res.status).toBe(200);
      return JSON.parse(((await res.json()) as { result: string }).result)[0].n;
   }

   const build = (pkg: string, body: Record<string, unknown>) =>
      fetch(api(pkg, "/materializations"), {
         method: "POST",
         headers: json,
         body: JSON.stringify(body),
      });

   async function settle(pkg: string, id: string): Promise<Run> {
      const deadline = Date.now() + 90_000;
      while (Date.now() < deadline) {
         const res = await fetch(api(pkg, `/materializations/${id}`));
         expect(res.status).toBe(200);
         const run = (await res.json()) as Run;
         if (
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(run.status)
         )
            return run;
         await new Promise((r) => setTimeout(r, 200));
      }
      throw new Error(`Materialization ${id} did not finish`);
   }

   async function buildAndSettle(
      pkg: string,
      body: Record<string, unknown>,
   ): Promise<Run> {
      const res = await build(pkg, body);
      expect(res.status).toBe(201);
      const run = await settle(pkg, ((await res.json()) as Run).id);
      expect(run.status).toBe("MANIFEST_FILE_READY");
      return run;
   }

   const tablesOf = (run: Run) =>
      Object.values(run.manifest?.entries ?? {}).map(
         (e) => e.physicalTableName,
      );

   async function runs(pkg: string, versionId?: string): Promise<Run[]> {
      const res = await fetch(
         api(
            pkg,
            `/materializations${versionId ? `?versionId=${versionId}` : ""}`,
         ),
      );
      expect(res.status).toBe(200);
      return (await res.json()) as Run[];
   }

   beforeAll(async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      scratch = await fs.mkdtemp(
         path.join(os.tmpdir(), "version-materializations-"),
      );
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBe(200);
   });

   afterAll(async () => {
      if (savedVersioning === undefined)
         delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      else process.env.PUBLISHER_PACKAGE_VERSIONING = savedVersioning;
      if (env) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         });
         await env.stop();
      }
      await fs.rm(scratch, { recursive: true, force: true });
   });

   describe('scope: "version"', () => {
      const PKG = "owned";
      let olderRun: Run;

      beforeAll(async () => {
         await publish(PKG, "1.0.0", 1, "version");
         await publish(PKG, "1.1.0", 2, "version");
      });

      it("builds a named version into tables of its own, and records the version", async () => {
         olderRun = await buildAndSettle(PKG, { versionId: "1.0.0" });
         expect(olderRun.metadata).toMatchObject({
            versionId: "1.0.0",
            scope: "version",
            mode: "auto",
         });
         expect(tablesOf(olderRun)).toEqual(["summary__v1_0_0"]);
      });

      it("builds latest when no version is named, into latest's own tables", async () => {
         const latestRun = await buildAndSettle(PKG, {});
         expect(latestRun.metadata?.versionId).toBe("1.1.0");
         expect(tablesOf(latestRun)).toEqual(["summary__v1_1_0"]);
      });

      it("serves each version from its own table", async () => {
         expect(await n(PKG, "1.0.0")).toBe(1);
         expect(await n(PKG, "1.1.0")).toBe(2);
         expect(await n(PKG)).toBe(2);
      });

      it("lists one version's runs: the one named, or latest's", async () => {
         const older = await runs(PKG, "1.0.0");
         expect(older.map((r) => r.id)).toEqual([olderRun.id]);
         const latest = await runs(PKG);
         expect(latest.map((r) => r.metadata?.versionId)).toEqual(["1.1.0"]);
      });

      it("finds a run only under the version that built it, when one is named", async () => {
         const own = await fetch(
            api(PKG, `/materializations/${olderRun.id}?versionId=1.0.0`),
         );
         expect(own.status).toBe(200);
         const other = await fetch(
            api(PKG, `/materializations/${olderRun.id}?versionId=1.1.0`),
         );
         expect(other.status).toBe(404);
      });

      it("refuses a version the package does not have", async () => {
         const res = await build(PKG, { versionId: "9.9.9" });
         expect(res.status).toBe(404);
         expect(((await res.json()) as { reason?: string }).reason).toBe(
            "VERSION_NOT_FOUND",
         );
         expect((await build(PKG, { versionId: 7 })).status).toBe(400);
      });

      it("reclaims an archived version's runs, and serves it live once unarchived", async () => {
         const archived = await fetch(api(PKG, "/versions/1.0.0"), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ archiveStatus: "archive" }),
         });
         expect(archived.status).toBe(200);
         const refused = await build(PKG, { versionId: "1.0.0" });
         expect(refused.status).toBe(410);
         expect(
            (await fetch(api(PKG, "/materializations?versionId=1.0.0"))).status,
         ).toBe(410);

         // Reclaim runs in the background; its run record goes when it does.
         const deadline = Date.now() + 30_000;
         let gone = false;
         while (!gone && Date.now() < deadline) {
            gone =
               (await fetch(api(PKG, `/materializations/${olderRun.id}`)))
                  .status === 404;
            if (!gone) await new Promise((r) => setTimeout(r, 100));
         }
         expect(gone).toBe(true);
         // Latest's run is another version's, and untouched.
         expect((await runs(PKG)).length).toBe(1);

         const unarchived = await fetch(api(PKG, "/versions/1.0.0"), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ archiveStatus: "unarchive" }),
         });
         expect(unarchived.status).toBe(200);
         expect(await runs(PKG, "1.0.0")).toEqual([]);
         expect(await n(PKG, "1.0.0")).toBe(1);
      });
   });

   describe('scope: "package"', () => {
      const PKG = "shared";

      beforeAll(async () => {
         await publish(PKG, "1.0.0", 1);
         await publish(PKG, "1.1.0", 2);
      });

      it("refuses an auto-run of a version that is not latest", async () => {
         // Load the older version first, so the test also shows its answer
         // survives latest's build below.
         expect(await n(PKG, "1.0.0")).toBe(1);
         const res = await build(PKG, { versionId: "1.0.0" });
         expect(res.status).toBe(400);
         expect(((await res.json()) as { message?: string }).message).toContain(
            '"scope": "version"',
         );
      });

      it("auto-runs latest into the package's shared table", async () => {
         const run = await buildAndSettle(PKG, {});
         expect(run.metadata).toMatchObject({
            versionId: "1.1.0",
            scope: "package",
         });
         expect(tablesOf(run)).toEqual(["summary"]);
         // Naming latest explicitly is the same build.
         expect(
            tablesOf(await buildAndSettle(PKG, { versionId: "1.1.0" })),
         ).toEqual(["summary"]);
      });

      it("never serves a version from a table built from another version's definition", async () => {
         expect(await n(PKG)).toBe(2);
         expect(await n(PKG, "1.0.0")).toBe(1);
      });
   });

   describe("a package built before its first versioned publish", () => {
      const PKG = "upgraded";

      it("keeps listing the runs it had before it was versioned, under every version", async () => {
         // Versioning off: an unversioned install, and a run of it.
         process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
         let legacy: Run;
         try {
            await publish(PKG, "0.0.1", 7);
            legacy = await buildAndSettle(PKG, {});
         } finally {
            process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
         }
         // Its first versioned publish.
         await publish(PKG, "1.0.0", 8);

         expect((await runs(PKG)).map((r) => r.id)).toContain(legacy.id);
         expect((await runs(PKG, "1.0.0")).map((r) => r.id)).toContain(
            legacy.id,
         );
         // And reachable by id under a version, as the listing shows it.
         const byId = await fetch(
            api(PKG, `/materializations/${legacy.id}?versionId=1.0.0`),
         );
         expect(byId.status).toBe(200);
      });
   });
});
