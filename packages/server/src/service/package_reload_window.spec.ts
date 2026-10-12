// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A reload discovers its reports against the models it is about to install,
 * then installs both in one synchronous step. These pin that on a real package
 * with a surface, by holding discovery open mid-reload and asking the package
 * what it serves: until the swap, every answer is the previous generation's —
 * its model, its report set, and the entry-point flag that model was given —
 * and after it every answer is the new one's. A reload that installed the
 * models first would, inside that window, serve a new model with no report
 * flags beside the old report set.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";
import type { Model } from "./model";
import { Package } from "./package";

const PKG = "window";

const FILES: Record<string, string> = {
   "publisher.json": JSON.stringify({ name: PKG, version: "1.0.0" }),
   "base.malloy": `source: s is duckdb.sql("select 1 as x") extend {\n   view: v is { group_by: x }\n}\n`,
   "index.malloy": `import "base.malloy"\nexport { s }\n`,
   // A linear report: an entry point although the surface does not list it.
   "flip.malloy": `## artifact { layout=linear }\nimport "base.malloy"\nrun: s -> v\n`,
};

/** What the reload turns `flip.malloy` into: a model the surface does not list. */
const FLIPPED = `import "base.malloy"\nsource: t is s\n`;
/** A report the reload adds. */
const ADDED = `## artifact { layout=grid tiles=["s -> v"] }\nimport "base.malloy"\n`;

type Discover = (models: unknown, during: string) => Promise<unknown>;

/** Whether the model at `modelPath` may be queried at all, as the query route asks. */
const onSurface = (model: Model | undefined): boolean => {
   if (!model) return false;
   try {
      model.assertFileOnSurface();
      return true;
   } catch {
      return false;
   }
};

describe("a reload's report swap", () => {
   let rootDir: string;
   let env: Environment;
   let restore: (() => void) | undefined;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-window-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      restore?.();
      restore = undefined;
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** Hold every reload's discovery open until `release` is called. */
   const holdReloadDiscovery = () => {
      const proto = Package.prototype as unknown as {
         discoverReportsIn: Discover;
      };
      const original = proto.discoverReportsIn;
      let release!: () => void;
      const released = new Promise<void>((resolve) => (release = resolve));
      let entered!: () => void;
      const held = new Promise<void>((resolve) => (entered = resolve));
      const spy = spyOn(proto, "discoverReportsIn").mockImplementation(
         async function (this: Package, models, during) {
            const discovery = await original.call(this, models, during);
            if (during === "reload") {
               entered();
               await released;
            }
            return discovery;
         },
      );
      restore = () => spy.mockRestore();
      return { held, release };
   };

   it("answers from the previous generation until the swap, and from the new one after", async () => {
      await env.installPackage(PKG, async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         for (const [name, text] of Object.entries(FILES))
            await fs.writeFile(path.join(stagingPath, name), text);
      });
      const pkg = await env.getPackage(PKG, false);
      const before = pkg.getModel("flip.malloy");
      expect(pkg.getReportLayout("flip.malloy")).toBe("linear");
      expect(onSurface(before)).toBe(true);

      const served = path.join(rootDir, "env", PKG);
      await fs.writeFile(path.join(served, "flip.malloy"), FLIPPED);
      await fs.writeFile(path.join(served, "added.malloy"), ADDED);

      const { held, release } = holdReloadDiscovery();
      const reload = pkg.reloadAllModels({});
      await held;

      // Mid-reload: the new models are compiled and discovered, not installed.
      expect(pkg.getModel("flip.malloy")).toBe(before);
      expect(pkg.getReportLayout("flip.malloy")).toBe("linear");
      expect(onSurface(pkg.getModel("flip.malloy"))).toBe(true);
      expect(pkg.getModel("added.malloy")).toBeUndefined();
      expect(pkg.isReport("added.malloy")).toBe(false);
      expect(pkg.listReports().map((r) => r.path)).toEqual(["flip.malloy"]);

      release();
      await reload;

      const after = pkg.getModel("flip.malloy");
      expect(after).not.toBe(before);
      expect(pkg.getReportLayout("flip.malloy")).toBeUndefined();
      expect(onSurface(after)).toBe(false);
      expect(pkg.getReportLayout("added.malloy")).toBe("grid");
      expect(onSurface(pkg.getModel("added.malloy"))).toBe(true);
      expect(pkg.listReports().map((r) => r.path)).toEqual(["added.malloy"]);
   });
});
