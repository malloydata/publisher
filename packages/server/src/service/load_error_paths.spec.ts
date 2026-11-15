// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { EnvironmentStore } from "./environment_store";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parseEnvironmentName, parsePackageName } from "./identity";
import { startedStore } from "../test_helpers/started_store";
import type { Environment } from "./environment";

/**
 * A package whose model does not compile at boot is a /status `loadErrors`
 * entry, and its message names the file it failed in package-relative, as a
 * failed reload's does: never the server's root, its `publisher_data/`, a
 * staging folder, or the configured location the package was read from.
 */

const ENV = "org";
const GOOD = `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`;
const BROKEN = `source: numbers is duckdb.sql("SELECT 1 AS answer") extend {\n  dimension: nope is no_such_column\n}\n`;

let restoreSettings: (() => void) | undefined;
let root: string;
let store: EnvironmentStore | undefined;

afterEach(async () => {
   if (store) {
      for (const environment of store.getLoadedEnvironments()) {
         await environment.closeAllConnections();
      }
      const internals = store as unknown as {
         storageManager: { close(): Promise<void> };
      };
      await internals.storageManager.close();
      store = undefined;
   }
   restoreSettings?.();
   restoreSettings = undefined;
   fs.rmSync(root, { recursive: true, force: true });
});

function boot(versioning: "on" | "off", model: string) {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "load-error-paths-")),
   );
   const server = path.join(root, "server");
   const source = path.join(root, "sources", "broken");
   fs.mkdirSync(server, { recursive: true });
   writePackageFiles(
      source,
      { "model.malloy": model },
      { manifest: { name: "broken", version: "1.0.0" } },
   );
   fs.writeFileSync(
      path.join(server, "publisher.config.json"),
      JSON.stringify({
         frozenConfig: false,
         environments: [
            {
               name: ENV,
               packages: [{ name: "broken", location: source }],
               connections: [],
            },
         ],
      }),
   );
   restoreSettings = installSettings({
      env: { PUBLISHER_PACKAGE_VERSIONING: versioning },
      cwd: server,
   }).restore;
   store = startedStore(new EnvironmentStore(server));
   return { server, source };
}

async function environment(): Promise<Environment> {
   await store!.finishedInitialization;
   return store!.getEnvironment(parseEnvironmentName(ENV), false);
}

async function messageOf(run: () => Promise<unknown>): Promise<string> {
   try {
      await run();
   } catch (error) {
      return error instanceof Error ? error.message : String(error);
   }
   throw new Error("expected the call to fail");
}

/** A broken copy of the package's files, as a publish would fetch them. */
function brokenSource(name: string, version: string): string {
   const dir = path.join(root, "sources", `${name}-${version}`);
   writePackageFiles(
      dir,
      { "model.malloy": BROKEN },
      { manifest: { name, version } },
   );
   return dir;
}

/** No absolute path, server root, data dir or source location in `text`. */
function expectNoServerPaths(text: string) {
   expect(text).not.toContain(root);
   expect(text).not.toContain("publisher_data");
   expect(text).not.toContain(".staging");
   expect(text).not.toContain("file://");
   expect(text).not.toMatch(/(^|[\s'"(])\/(private|var|tmp|Users|home)\//);
}

for (const versioning of ["on", "off"] as const) {
   describe(`load error paths (versioning ${versioning})`, () => {
      it("names the failing file package-relative in a boot loadErrors entry", async () => {
         boot(versioning, BROKEN);
         await store!.finishedInitialization;
         const status = await store!.getStatus();
         const entry = status.loadErrors?.find((e) => e.package === "broken");
         expect(entry?.message).toContain("model.malloy");
         expectNoServerPaths(entry?.message ?? "");
      });

      it("names it package-relative where the package resource is read", async () => {
         boot(versioning, BROKEN);
         await store!.finishedInitialization;
         const environment = await store!.getEnvironment(
            parseEnvironmentName(ENV),
            false,
         );
         let message = "";
         try {
            await environment.getPackage(parsePackageName("broken"), false);
         } catch (error) {
            message = error instanceof Error ? error.message : String(error);
         }
         expect(message).toContain("model.malloy");
         expectNoServerPaths(message);
      });
   });
}

describe("load error paths beyond boot", () => {
   it("names the file package-relative in a failed reload and its stale loadErrors entry", async () => {
      const { server } = boot("off", GOOD);
      const env = await environment();
      await env.getPackage(parsePackageName("broken"), false);
      fs.writeFileSync(
         path.join(server, "publisher_data", ENV, "broken", "model.malloy"),
         BROKEN,
      );
      const message = await messageOf(() =>
         env.getPackage(parsePackageName("broken"), true),
      );
      expect(message).toContain("model.malloy");
      expectNoServerPaths(message);
      const status = await store!.getStatus();
      const stale = status.loadErrors?.find((e) => e.stale === true);
      expect(stale?.message).toContain("model.malloy");
      expectNoServerPaths(stale?.message ?? "");
   });

   it("names the file package-relative in a refused unversioned install", async () => {
      boot("off", GOOD);
      const env = await environment();
      const from = brokenSource("other", "1.0.0");
      const message = await messageOf(() =>
         env.installPackage("other", (staging) =>
            fs.promises.cp(from, staging, { recursive: true }),
         ),
      );
      expect(message).toContain("model.malloy");
      expectNoServerPaths(message);
   });

   it("names the file package-relative in a refused version publish", async () => {
      boot("on", GOOD);
      const env = await environment();
      const versions = env.getVersionService();
      if (!versions) throw new Error("versioning is not bound");
      const from = brokenSource("other", "1.0.0");
      const message = await messageOf(() =>
         versions.publish(
            "other",
            (staging) => fs.promises.cp(from, staging, { recursive: true }),
            { sourceLocation: from, promotion: "on-publish" },
         ),
      );
      expect(message).toContain("model.malloy");
      expectNoServerPaths(message);
   });

   for (const versioning of ["on", "off"] as const) {
      it(`names the file package-relative in compile diagnostics (versioning ${versioning})`, async () => {
         boot(versioning, GOOD);
         const env = await environment();
         for (const [scope, text] of [
            ["file", BROKEN],
            ["file", `import "missing.malloy"\n${GOOD}`],
            ["append", `run: numbers -> { group_by: nope }\n`],
         ] as const) {
            const { problems } = await env.compileSource(
               "broken",
               "model.malloy",
               text,
               false,
               undefined,
               scope,
            );
            expect(problems.length).toBeGreaterThan(0);
            for (const problem of problems) {
               expectNoServerPaths(problem.message);
               const url = (problem as { at?: { url?: string } }).at?.url;
               if (url !== undefined) expectNoServerPaths(url);
            }
         }
      });
   }
});
