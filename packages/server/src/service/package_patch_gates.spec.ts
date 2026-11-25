// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A metadata PATCH re-runs the load gates exactly when it changes what is
 * queryable (`explores`, `queryableSources`); a refusal keeps the old metadata.
 * Runs a real `Environment` and `Package.create` over temp dirs.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { BadRequestError } from "../errors";
import { unrecordedAudit } from "./access_record";
import { Environment } from "./environment";
import { Model } from "./model";
import * as loadGates from "./package_load_gates";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parsePackageName } from "./identity";
import { QueryPrivileges } from "./model_query_request";

const ROWS = `duckdb.sql("select * from (values (1,'org-a'),(2,'org-b')) as t(id, org)")`;

describe("metadata PATCH and the load gates", () => {
   let rootDir: string;
   let envPath: string;
   let env: Environment;

   const CLEAN = `##! experimental.givens
#(secure)
given: ORGS :: string[]
#(access_filter) org in $ORGS
source: tidy is ${ROWS}
`;

   const LEAK = `#(carries) ["none"]
source: leaky is ${ROWS} extend {
   #(classification) "phi.direct_identifier"
   dimension: key is id
}
`;

   const BYPASS = `#(authorize) false
source: vault is ${ROWS}

source: bridge is ${ROWS} extend {
   join_one: locked is vault on locked.id = id
}
`;

   async function write(file: string, text: string): Promise<void> {
      await fs.writeFile(path.join(envPath, "pkg", file), text);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-patch-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(path.join(envPath, "pkg"), { recursive: true });
      await write(
         "publisher.json",
         JSON.stringify({
            name: "pkg",
            description: "fixture",
            explores: ["index.malloy"],
         }),
      );
      await write("index.malloy", `source: open is ${ROWS}\n`);
      await write("hidden.malloy", LEAK);
      await write("clean.malloy", CLEAN);
      env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const metadata = async () =>
      (
         await env.getPackage(parsePackageName("pkg"), false)
      ).getPackageMetadata();

   it("refuses switching to queryableSources all over a hidden #(carries) leak, and keeps the old metadata", async () => {
      const before = await metadata();
      let error: unknown;
      try {
         await env.updatePackage("pkg", {
            name: "pkg",
            queryableSources: "all",
         });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toContain(
         "source leaky (hidden.malloy) carries [none] but exposes key, classified phi.direct_identifier",
      );
      expect(await metadata()).toEqual(before);
      const manifest = JSON.parse(
         await fs.readFile(path.join(envPath, "pkg", "publisher.json"), "utf8"),
      );
      expect(manifest.queryableSources).toBeUndefined();
   });

   it("keeps the package's name when a PATCH that renames it is refused", async () => {
      const pkg = await env.getPackage(parsePackageName("pkg"), false);
      await expect(
         env.updatePackage("pkg", { name: "renamed", queryableSources: "all" }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(pkg.getPackageName()).toBe("pkg");
   });

   it("rolls a reinstall back when the new tree fails the gates under the body's listing, keeping the old tree and location", async () => {
      const before = await metadata();
      const newTree = path.join(rootDir, "next");
      writePackageFiles(
         newTree,
         {
            "index.malloy": `source: open is ${ROWS}\n`,
            "hidden.malloy": LEAK,
         },
         {
            manifest: {
               name: "pkg",
               description: "next",
               explores: ["index.malloy"],
            },
         },
      );
      const download = (staging: string) =>
         fs.cp(newTree, staging, { recursive: true });
      let error: unknown;
      try {
         await env.installPackage("pkg", download, undefined, {
            update: {
               name: "pkg",
               location: "gs://bucket/next.zip",
               queryableSources: "all",
            },
         });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toContain(
         "source leaky (hidden.malloy)",
      );
      expect(await metadata()).toEqual(before);
      const kept = await fs.readFile(
         path.join(envPath, "pkg", "publisher.json"),
         "utf8",
      );
      expect(JSON.parse(kept).description).toBe("fixture");
   });

   it("installs the same tree under a listing that does not expose the bypass", async () => {
      const newTree = path.join(rootDir, "next");
      writePackageFiles(
         newTree,
         {
            "index.malloy": `source: open is ${ROWS}\n`,
            "hidden.malloy": LEAK,
         },
         {
            manifest: {
               name: "pkg",
               description: "next",
               explores: ["index.malloy"],
            },
         },
      );
      await env.installPackage(
         "pkg",
         (staging) => fs.cp(newTree, staging, { recursive: true }),
         undefined,
         { update: { name: "pkg", location: "gs://bucket/next.zip" } },
      );
      expect((await metadata()).location).toBe("gs://bucket/next.zip");
   });

   it("accepts a PATCH over a hidden join bypass in a package that declares #(classification), reporting it as one warning", async () => {
      const dir = path.join(envPath, "gates");
      writePackageFiles(
         dir,
         {
            "index.malloy": `source: open is ${ROWS} extend {\n   #(classification) "none"\n   dimension: key is id\n}\n`,
            "hidden.malloy": BYPASS,
         },
         {
            manifest: {
               name: "gates",
               description: "classified, with a bypass",
               explores: ["index.malloy"],
            },
         },
      );
      await env.addPackage("gates");
      const updated = await env.updatePackage("gates", {
         name: "gates",
         queryableSources: "all",
      });
      expect(updated.queryableSources).toBe("all");
      const warnings = (
         await (
            await env.getPackage(parsePackageName("gates"), false)
         ).getPackageMetadata()
      ).warnings;
      expect(warnings?.filter((w) => w.code === "join-bypass")).toHaveLength(1);
   });

   it("refuses adding a source with a #(carries) leak to explores, and keeps the old metadata", async () => {
      const before = await metadata();
      let error: unknown;
      try {
         await env.updatePackage("pkg", {
            name: "pkg",
            explores: ["index.malloy", "hidden.malloy"],
         });
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      expect((await metadata()).explores).toEqual(["index.malloy"]);
      expect(await metadata()).toEqual(before);
   });

   it("accepts a PATCH to an innocent explores list, running the gates once", async () => {
      const spy = spyOn(loadGates, "applyLoadGates");
      const updated = await env.updatePackage("pkg", {
         name: "pkg",
         explores: ["index.malloy", "clean.malloy"],
      });
      expect(updated.explores).toEqual(["index.malloy", "clean.malloy"]);
      expect(spy).toHaveBeenCalledTimes(1);
      spy.mockRestore();
   });

   it("serves the old surface after a refusal, so a later innocent PATCH still lands", async () => {
      await env
         .updatePackage("pkg", { name: "pkg", queryableSources: "all" })
         .catch(() => undefined);
      const updated = await env.updatePackage("pkg", {
         name: "pkg",
         explores: ["index.malloy", "clean.malloy"],
      });
      expect(updated.queryableSources).not.toBe("all");
   });

   describe("while the gates are judging a new listing", () => {
      let release: (error?: Error) => void;
      let entered: Promise<void>;
      let held: ReturnType<typeof spyOn>;

      // Hold the gates' async step until the test releases it.
      beforeEach(() => {
         const prepared = Model.prototype.preparedCellQueries;
         const gate = new Promise<void>((resolve, reject) => {
            release = (error) => (error ? reject(error) : resolve());
         });
         let enter: () => void = () => {};
         entered = new Promise((resolve) => (enter = resolve));
         held = spyOn(
            Model.prototype,
            "preparedCellQueries",
         ).mockImplementation(async function (this: Model) {
            enter();
            await gate;
            return prepared.call(this);
         });
      });

      afterEach(() => {
         held.mockRestore();
      });

      const OFF_SURFACE = 'No queryable model "hidden.malloy"';
      const queryHidden = async () =>
         (await env.getPackage(parsePackageName("pkg"), false))
            .getModel("hidden.malloy")!
            .runQuery(
               unrecordedAudit(),
               {
                  query: "run: leaky -> { aggregate: c is count() }",
                  filterParams: {},
               },
               QueryPrivileges.of({ bypassFilters: true }),
            );

      const patchToAll = () =>
         env
            .updatePackage("pkg", { name: "pkg", queryableSources: "all" })
            .then(
               () => undefined,
               (e: unknown) => e,
            );

      it("serves the old listing until they refuse it", async () => {
         const before = await metadata();
         const patch = patchToAll();
         await entered;
         expect((await metadata()).queryableSources).not.toBe("all");
         await expect(queryHidden()).rejects.toThrow(OFF_SURFACE);
         release();
         expect(await patch).toBeInstanceOf(BadRequestError);
         expect(await metadata()).toEqual(before);
      });

      it("keeps the old listing when they throw", async () => {
         const before = await metadata();
         const patch = patchToAll();
         await entered;
         await expect(queryHidden()).rejects.toThrow(OFF_SURFACE);
         release(new Error("boom"));
         expect(((await patch) as Error).message).toBe("boom");
         expect(await metadata()).toEqual(before);
         await expect(queryHidden()).rejects.toThrow(OFF_SURFACE);
      });
   });

   it("does not re-run the gates for a PATCH that changes nothing queryable", async () => {
      const spy = spyOn(loadGates, "applyLoadGates");
      try {
         const current = await metadata();
         const bodies = [
            { description: "edited" },
            { resource: "res" },
            { location: "gs://elsewhere" },
            { manifestLocation: null },
            { materialization: {} },
            { queryMetadata: { team: "fin" } },
            { scope: "package" as const },
            // The GET body echoed back: same listing, so not a change.
            {
               explores: current.explores,
               queryableSources: current.queryableSources,
            },
         ];
         for (const body of bodies) {
            await env.updatePackage("pkg", { name: "pkg", ...body });
         }
         expect(spy).not.toHaveBeenCalled();
      } finally {
         spy.mockRestore();
      }
   });
});
