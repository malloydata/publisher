// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A server in this process with one environment, and the HTTP calls the
 * package-resource suites make against it: write a package to disk, publish
 * it from there, and read a route as `{ status, json }`.
 */

import { expect } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { startRestE2E } from "../../harness/rest_e2e";

export type Reply = { status: number; json: Record<string, unknown> };

export type ResourcesEnv = {
   /** A package directory with `publisher.json` (`name` plus `manifest`) and `files`. */
   writePackage(
      name: string,
      files: Record<string, string>,
      manifest?: Record<string, unknown>,
   ): string;
   publish(name: string, location: string): Promise<Reply>;
   /** `GET /api/v0{rel}`. */
   get(rel: string): Promise<Reply>;
   /** `POST /api/v0{rel}` with a JSON body. */
   post(rel: string, body: unknown): Promise<Reply>;
   /** `/environments/{env}/packages/{name}{sub}`. */
   pkgUrl(name: string, sub?: string): string;
   /** The package's entries in `GET /status`: one per version it holds. */
   statusEntries(name: string): Promise<Record<string, unknown>[]>;
   stop(): Promise<void>;
};

export async function startResourcesEnv(
   envName: string,
): Promise<ResourcesEnv> {
   const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `${envName}-`));
   const server = await startRestE2E();
   const api = `${server.baseUrl}/api/v0`;

   const reply = async (res: Response): Promise<Reply> => ({
      status: res.status,
      json: (await res.json()) as Record<string, unknown>,
   });

   const created = await fetch(`${api}/environments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: envName, packages: [], connections: [] }),
   });
   expect(created.status).toBeLessThan(300);

   const get = async (rel: string) => reply(await fetch(`${api}${rel}`));

   return {
      writePackage(name, files, manifest = {}) {
         const dir = fs.mkdtempSync(path.join(tempDir, `${name}-`));
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name, ...manifest }),
         );
         for (const [file, text] of Object.entries(files)) {
            fs.writeFileSync(path.join(dir, file), text);
         }
         return dir;
      },
      async publish(name, location) {
         return reply(
            await fetch(`${api}/environments/${envName}/packages`, {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({ name, location }),
            }),
         );
      },
      get,
      async post(rel, body) {
         return reply(
            await fetch(`${api}${rel}`, {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify(body),
            }),
         );
      },
      pkgUrl: (name, sub = "") =>
         `/environments/${envName}/packages/${name}${sub}`,
      async statusEntries(name) {
         const { json } = await get("/status");
         const environments = json.environments as {
            name: string;
            packages: Record<string, unknown>[];
         }[];
         return (
            environments
               .find((e) => e.name === envName)
               ?.packages.filter((p) => p.name === name) ?? []
         );
      },
      async stop() {
         await fetch(`${api}/environments/${envName}`, {
            method: "DELETE",
         }).catch(() => undefined);
         await server.stop();
         fs.rmSync(tempDir, { recursive: true, force: true });
      },
   };
}
