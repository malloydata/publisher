// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Reads of an unversioned package during its first versioned publish, over
 * HTTP. That publish moves the package's files aside while it compiles the
 * version, so a read served from the copy in memory would reach for files
 * that are not there: a query over a package-local `data.csv`, or a page
 * under `public/`. The compile is held open (the server runs in this process)
 * and both reads are sent inside the hold; each must answer 200, from the
 * version once the publish commits, or from the copy put back when it fails.
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { Package } from "../../../src/service/package";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "versions-first-publish-reads-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
const savedFlag = process.env[FLAG];

function versioning(on: boolean): void {
   if (on) process.env[FLAG] = "on";
   else delete process.env[FLAG];
}

/** A package whose query reads its own data.csv, and whose page says `n`. */
function writePackage(
   folder: string,
   manifest: Record<string, unknown>,
   n: number,
): string {
   const dir = path.join(root, folder);
   fs.mkdirSync(path.join(dir, "public"), { recursive: true });
   fs.writeFileSync(path.join(dir, "publisher.json"), JSON.stringify(manifest));
   fs.writeFileSync(path.join(dir, "data.csv"), `answer\n${n}\n`);
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      "source: numbers is duckdb.table('data.csv') extend {\n" +
         "  view: which is { select: answer }\n}\n",
   );
   fs.writeFileSync(
      path.join(dir, "public", "index.html"),
      `<!doctype html><title>page v${n}</title><body>page v${n}</body>`,
   );
   return dir;
}

const pkgApi = (name: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`;

function publish(name: string, location: string): Promise<Response> {
   return fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name, location }),
   });
}

/** A query's status and, when it answered, its answer or its error. */
async function query(name: string): Promise<[number, unknown]> {
   const res = await fetch(`${pkgApi(name)}/models/model.malloy/query`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
         query: "run: numbers -> which",
         compactJson: true,
      }),
   });
   const json = (await res.json()) as { result?: string; message?: string };
   return res.status === 200
      ? [200, JSON.parse(String(json.result))[0].answer]
      : [res.status, json.message];
}

/** A page's status and whether it is version `n`'s. */
async function page(name: string): Promise<[number, string]> {
   const res = await fetch(
      `${baseUrl}/environments/${ENV_NAME}/packages/${name}/index.html`,
   );
   const text = await res.text();
   return [res.status, /page v(\d+)/.exec(text)?.[0] ?? text.slice(0, 80)];
}

/**
 * Hold the next compile until released: the publish has moved the
 * unversioned files aside by then. `reached` resolves once it is held.
 */
function holdNextCompile(fail = false) {
   let release!: () => void;
   let reached!: () => void;
   const gate = new Promise<void>((r) => (release = r));
   const atGate = new Promise<void>((r) => (reached = r));
   const create = Package.create.bind(Package);
   const spy = spyOn(Package, "create").mockImplementationOnce(
      async (...args: Parameters<typeof Package.create>) => {
         reached();
         await gate;
         if (fail) throw new Error("compile failed");
         return create(...args);
      },
   );
   return { release, reached: atGate, restore: () => spy.mockRestore() };
}

describe("reads of an unversioned package during its first versioned publish", () => {
   beforeAll(async () => {
      versioning(false);
      root = fs.realpathSync(
         fs.mkdtempSync(
            path.join(os.tmpdir(), "versions-first-publish-reads-"),
         ),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);
   }, 180_000);

   afterEach(() => versioning(false));

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
      if (savedFlag === undefined) delete process.env[FLAG];
      else process.env[FLAG] = savedFlag;
   });

   it("a query and a page read inside the hold both answer, from the version", async () => {
      const name = "held";
      expect(
         (await publish(name, writePackage("held-0", { name }, 0))).status,
      ).toBe(200);
      expect(await query(name)).toEqual([200, 0]);
      expect(await page(name)).toEqual([200, "page v0"]);

      versioning(true);
      const held = holdNextCompile();
      try {
         const publishing = publish(
            name,
            writePackage("held-1", { name, version: "1.0.0" }, 1),
         );
         await held.reached;
         const reads = Promise.all([query(name), page(name)]);
         await new Promise((r) => setTimeout(r, 200));
         held.release();
         const published = await publishing;
         expect(published.status).toBe(200);
         expect(await reads).toEqual([
            [200, 1],
            [200, "page v1"],
         ]);
      } finally {
         held.release();
         held.restore();
      }
   });

   it("a query and a page read inside the hold of a publish that fails answer from the copy put back", async () => {
      const name = "failing";
      expect(
         (await publish(name, writePackage("failing-0", { name }, 0))).status,
      ).toBe(200);
      expect(await query(name)).toEqual([200, 0]);

      versioning(true);
      const held = holdNextCompile(true);
      try {
         const publishing = publish(
            name,
            writePackage("failing-1", { name, version: "1.0.0" }, 1),
         );
         await held.reached;
         const reads = Promise.all([query(name), page(name)]);
         await new Promise((r) => setTimeout(r, 200));
         held.release();
         expect((await publishing).status).toBeGreaterThanOrEqual(400);
         expect(await reads).toEqual([
            [200, 0],
            [200, "page v0"],
         ]);
      } finally {
         held.release();
         held.restore();
      }
      expect(await query(name)).toEqual([200, 0]);
   });
});
