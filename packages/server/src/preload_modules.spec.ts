// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import * as path from "path";
import { Worker } from "worker_threads";

import {
   parsePreloadModules,
   preloadModules,
   preloadModulesFromEnv,
   PRELOAD_MODULES_ENV,
} from "./preload_modules";

describe("parsePreloadModules", () => {
   it("splits on commas, trims, drops empties, keeps order", () => {
      expect(parsePreloadModules(undefined)).toEqual([]);
      expect(parsePreloadModules("")).toEqual([]);
      expect(parsePreloadModules(" , ,")).toEqual([]);
      expect(
         parsePreloadModules("@acme/db-foo, /opt/plugins/bar.mjs ,baz"),
      ).toEqual(["@acme/db-foo", "/opt/plugins/bar.mjs", "baz"]);
   });
});

describe("preloadModules", () => {
   it("imports each specifier in order, awaiting one before the next", async () => {
      const seen: string[] = [];
      let inFlight = 0;
      const importer = async (spec: string) => {
         expect(inFlight).toBe(0);
         inFlight++;
         await new Promise((r) => setTimeout(r, 1));
         inFlight--;
         seen.push(spec);
      };
      const loaded = await preloadModules(["a", "b", "c"], importer);
      expect(seen).toEqual(["a", "b", "c"]);
      expect(loaded).toEqual(["a", "b", "c"]);
   });

   it("names the module that failed and stops there", async () => {
      const seen: string[] = [];
      const importer = async (spec: string) => {
         if (spec === "b") throw new Error("Cannot find package 'b'");
         seen.push(spec);
      };
      await expect(preloadModules(["a", "b", "c"], importer)).rejects.toThrow(
         `${PRELOAD_MODULES_ENV}: failed to import "b": Cannot find package 'b'`,
      );
      expect(seen).toEqual(["a"]);
   });

   it("refuses a relative path before importing anything", async () => {
      const seen: string[] = [];
      const importer = async (spec: string) => {
         seen.push(spec);
      };
      await expect(preloadModules(["./plugin.mjs"], importer)).rejects.toThrow(
         "is a relative path",
      );
      expect(seen).toEqual([]);
   });

   it("really imports an absolute path through the default importer", async () => {
      const dir = mkdtempSync(path.join(tmpdir(), "preload-"));
      const file = path.join(dir, "probe.mjs");
      writeFileSync(file, "globalThis.__preload_probe = 'main';\n");
      await preloadModulesFromEnv({ [PRELOAD_MODULES_ENV]: file });
      expect((globalThis as { __preload_probe?: string }).__preload_probe).toBe(
         "main",
      );
   });
});

describe("preloadModulesFromEnv inside a worker thread", () => {
   // A worker_threads Worker is its own realm: nothing the main thread
   // imported is visible there. This pins the property the load worker
   // relies on -- the same env, read in the worker, imports the module into
   // the worker's realm.
   it("imports the listed module into the worker's own realm", async () => {
      const dir = mkdtempSync(path.join(tmpdir(), "preload-worker-"));
      const probe = path.join(dir, "probe.mjs");
      writeFileSync(probe, "globalThis.__preload_probe = 'worker';\n");
      const loader = path.join(dir, "loader.mjs");
      writeFileSync(
         loader,
         [
            `import { parentPort } from "worker_threads";`,
            `import { preloadModulesFromEnv } from ${JSON.stringify(
               path.resolve(import.meta.dir, "preload_modules.ts"),
            )};`,
            `const before = globalThis.__preload_probe;`,
            `await preloadModulesFromEnv();`,
            `parentPort.postMessage({ before, after: globalThis.__preload_probe });`,
         ].join("\n"),
      );
      const worker = new Worker(loader, {
         env: { ...process.env, [PRELOAD_MODULES_ENV]: probe },
      });
      const result = await new Promise<{ before?: string; after?: string }>(
         (resolve, reject) => {
            worker.once("message", resolve);
            worker.once("error", reject);
         },
      );
      await worker.terminate();
      expect(result).toEqual({ before: undefined, after: "worker" });
   });
});
