// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import { Worker } from "worker_threads";
import { registerConnectionType } from "@malloydata/malloy";

import {
   parsePreloadModules,
   preloadModules,
   preloadModulesFromEnv,
   PRELOAD_MODULES_ENV,
   resolvePreloadSpecifier,
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

describe("resolvePreloadSpecifier", () => {
   it("passes a package name through and turns an absolute path into a file URL", () => {
      expect(resolvePreloadSpecifier("@acme/db-foo")).toBe("@acme/db-foo");
      const abs = path.resolve("/opt/plugins/bar.mjs");
      expect(resolvePreloadSpecifier(abs)).toBe(pathToFileURL(abs).href);
   });

   it("refuses every relative spelling", () => {
      for (const spec of [".", "..", "./x.mjs", "../x.mjs", ".\\x.mjs"]) {
         expect(() => resolvePreloadSpecifier(spec)).toThrow(
            "is a relative path",
         );
      }
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
      expect(loaded.map((m) => m.spec)).toEqual(["a", "b", "c"]);
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

   it("reports the connection types an import added and the ones it replaced", async () => {
      const def = {
         displayName: "Probe",
         properties: [],
         factory: async () => {
            throw new Error("probe");
         },
      };
      // Evaluates each module once, as a real import does: a repeated
      // specifier is served from the module cache and registers nothing.
      const executed = new Set<string>();
      const importer = async (spec: string) => {
         if (executed.has(spec)) return;
         executed.add(spec);
         if (spec === "adds") registerConnectionType("preload_spec_probe", def);
         if (spec === "replaces")
            registerConnectionType("preload_spec_probe", { ...def });
      };
      const loaded = await preloadModules(
         ["adds", "replaces", "nothing", "adds"],
         importer,
      );
      expect(loaded).toEqual([
         {
            spec: "adds",
            addedConnectionTypes: ["preload_spec_probe"],
            replacedConnectionTypes: [],
            repeated: false,
         },
         {
            spec: "replaces",
            addedConnectionTypes: [],
            replacedConnectionTypes: ["preload_spec_probe"],
            repeated: false,
         },
         {
            spec: "nothing",
            addedConnectionTypes: [],
            replacedConnectionTypes: [],
            repeated: false,
         },
         // The second "adds" is served from the module cache: nothing is
         // registered, and the entry is flagged as the repeat it is.
         {
            spec: "adds",
            addedConnectionTypes: [],
            replacedConnectionTypes: [],
            repeated: true,
         },
      ]);
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
   const saved = process.env[PRELOAD_MODULES_ENV];
   afterEach(() => {
      if (saved === undefined) delete process.env[PRELOAD_MODULES_ENV];
      else process.env[PRELOAD_MODULES_ENV] = saved;
   });

   // A worker_threads Worker is its own realm: nothing the main thread
   // imported is visible there. This pins the property the load worker
   // relies on -- the env a Worker inherits by default, read in the worker,
   // imports the module into the worker's realm. The Worker is built the
   // way the pool builds it, with no `env` option, so the default is what is
   // under test.
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
      process.env[PRELOAD_MODULES_ENV] = probe;
      const worker = new Worker(loader, { name: "preload-spec" });
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
