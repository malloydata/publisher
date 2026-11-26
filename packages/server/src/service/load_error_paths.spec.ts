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
import { unrecordedAudit } from "./access_record";
import { MODEL_RECOMPILE_FAILED } from "./package";

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

function boot(
   versioning: "on" | "off",
   model: string | Record<string, string>,
   options: {
      /** Leave the root as `os.tmpdir()` spells it (macOS: `/var`, a symlink). */
      unresolvedRoot?: boolean;
      /** The configured location, instead of the package's source folder. */
      location?: string;
      /** Files beside the package, under `sources/`. */
      beside?: Record<string, string>;
   } = {},
) {
   const made = fs.mkdtempSync(path.join(os.tmpdir(), "load-error-paths-"));
   root = options.unresolvedRoot ? made : fs.realpathSync(made);
   const server = path.join(root, "server");
   const source = path.join(root, "sources", "broken");
   fs.mkdirSync(server, { recursive: true });
   writePackageFiles(
      source,
      typeof model === "string" ? { "model.malloy": model } : model,
      { manifest: { name: "broken", version: "1.0.0" } },
   );
   for (const [file, text] of Object.entries(options.beside ?? {})) {
      fs.mkdirSync(path.dirname(path.join(root, "sources", file)), {
         recursive: true,
      });
      fs.writeFileSync(path.join(root, "sources", file), text);
   }
   fs.writeFileSync(
      path.join(server, "publisher.config.json"),
      JSON.stringify({
         frozenConfig: false,
         environments: [
            {
               name: ENV,
               packages: [
                  { name: "broken", location: options.location ?? source },
               ],
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
   expect(text).not.toContain(fs.realpathSync(root));
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

describe("load error paths: recompiles, outside paths and every load", () => {
   it("names the file package-relative on a model a recompile could not compile, and in its query's 424", async () => {
      const { server } = boot("off", GOOD);
      const env = await environment();
      const pkg = await env.getPackage(parsePackageName("broken"), false);
      const file = path.join(
         server,
         "publisher_data",
         ENV,
         "broken",
         "model.malloy",
      );
      // A file that compiled before keeps its last good model; the
      // recompile's error is in the package's warning.
      fs.writeFileSync(file, BROKEN);
      await pkg.reloadAllModels({});
      const warning = (pkg.getPackageMetadata().warnings ?? []).find(
         (w) => w.code === MODEL_RECOMPILE_FAILED,
      );
      expect(warning?.message).toContain("model.malloy");
      expectNoServerPaths(warning?.message ?? "");
      // A file that never compiled is kept as a placeholder rather than
      // throwing: its error is what every later reader sees.
      fs.writeFileSync(path.join(path.dirname(file), "other.malloy"), BROKEN);
      await pkg.reloadAllModels({});
      const model = pkg.getModel("other.malloy");
      const compileError = model?.getCompilationError()?.message ?? "";
      expect(compileError).toContain("other.malloy");
      expectNoServerPaths(compileError);
      const queryError = await messageOf(() =>
         model!.runQuery(unrecordedAudit(), {
            query: "run: numbers -> { select: answer }",
         }),
      );
      expect(queryError).toContain("other.malloy");
      expectNoServerPaths(queryError);
   });

   for (const versioning of ["on", "off"] as const) {
      it(`names an import outside the package as outside it (versioning ${versioning})`, async () => {
         boot(
            versioning,
            { "model.malloy": `import "../shared/x.malloy"\n${GOOD}` },
            {
               beside: {
                  "shared/x.malloy": `source: x is duckdb.sql("SELECT 1 AS a") extend { dimension: d is nope }\n`,
               },
            },
         );
         await store!.finishedInitialization;
         const status = await store!.getStatus();
         const entry = status.loadErrors?.find((e) => e.package === "broken");
         expect(entry?.message).toBeTruthy();
         expectNoServerPaths(entry?.message ?? "");
      });

      it(`names a configured location that does not exist by its package (versioning ${versioning})`, async () => {
         const missing = path.join(os.tmpdir(), "no-such-publisher-location");
         boot(versioning, GOOD, { location: missing });
         await store!.finishedInitialization;
         const status = await store!.getStatus();
         const entry = status.loadErrors?.find((e) => e.package === "broken");
         expect(entry?.message).toBeTruthy();
         expectNoServerPaths(entry?.message ?? "");
         expect(entry?.message).not.toContain(missing);
      });

      it(`names the file package-relative under a root spelled through a symlink (versioning ${versioning})`, async () => {
         boot(versioning, BROKEN, { unresolvedRoot: true });
         await store!.finishedInitialization;
         const status = await store!.getStatus();
         const entry = status.loadErrors?.find((e) => e.package === "broken");
         expect(entry?.message).toContain("model.malloy");
         expectNoServerPaths(entry?.message ?? "");
      });
   }

   it("names the file package-relative in a dry run", async () => {
      boot("on", GOOD);
      const env = await environment();
      const from = brokenSource("other", "1.0.0");
      const message = await messageOf(() =>
         env.dryRunInstall(
            "other",
            (staging) => fs.promises.cp(from, staging, { recursive: true }),
            { readVersion: false, describe: () => undefined },
         ),
      );
      expect(message).toContain("model.malloy");
      expectNoServerPaths(message);
   });

   it("judges a dry run as the version it would publish: two equal name=s are two tables only under scope: version", async () => {
      boot("on", GOOD);
      const env = await environment();
      const twoNamed = `##! experimental.persistence
source: raw is duckdb.sql("SELECT 1 AS x")
#@ persist name="t"
source: a is raw -> { group_by: x }
#@ persist name="t"
source: b is raw -> { select: x }
`;
      const dryRun = (scope: string) => {
         const dir = path.join(root, "sources", `twonamed-${scope}`);
         writePackageFiles(
            dir,
            { "model.malloy": twoNamed },
            {
               manifest: {
                  name: "twonamed",
                  version: "1.0.0",
                  materialization: { scope },
               },
            },
         );
         return env.dryRunInstall(
            "twonamed",
            (staging) => fs.promises.cp(dir, staging, { recursive: true }),
            { readVersion: true, describe: () => "admitted" },
         );
      };
      expect(await dryRun("version")).toBe("admitted");
      expect(await messageOf(() => dryRun("package"))).toContain(
         "[materialization-table-collision]",
      );
   });

   it("names the file package-relative in an add from the environment's folder", async () => {
      const { server } = boot("off", GOOD);
      const env = await environment();
      writePackageFiles(
         path.join(server, "publisher_data", ENV, "other"),
         { "model.malloy": BROKEN },
         { manifest: { name: "other", version: "1.0.0" } },
      );
      const message = await messageOf(() => env.addPackage("other"));
      expect(message).toContain("model.malloy");
      expectNoServerPaths(message);
   });

   it("names a file under a spaced directory decoded in a compile's at.url", async () => {
      boot("off", { "model.malloy": GOOD, "sub dir/a b.malloy": GOOD });
      const env = await environment();
      const { problems } = await env.compileSource(
         "broken",
         "sub dir/a b.malloy",
         BROKEN,
         false,
         undefined,
         "file",
      );
      const urls = problems.map(
         (p) => (p as { at?: { url?: string } }).at?.url,
      );
      expect(urls).toContain("sub dir/a b.malloy");
   });
});
