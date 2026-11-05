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
import { Environment } from "./environment";
import * as loadGates from "./package_load_gates";

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

   const BYPASS = `#(authorize) false
source: vault is ${ROWS}

source: leaky is ${ROWS} extend {
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
      await write("hidden.malloy", BYPASS);
      await write("clean.malloy", CLEAN);
      env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const metadata = async () =>
      (await env.getPackage("pkg", false)).getPackageMetadata();

   it("refuses switching to queryableSources all over a hidden join bypass, and keeps the old metadata", async () => {
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
         "source leaky (hidden.malloy) is open to every caller and joins locked, which is locked",
      );
      expect(await metadata()).toEqual(before);
      const manifest = JSON.parse(
         await fs.readFile(path.join(envPath, "pkg", "publisher.json"), "utf8"),
      );
      expect(manifest.queryableSources).toBeUndefined();
   });

   it("refuses adding a source with a join bypass to explores, and keeps the old metadata", async () => {
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
