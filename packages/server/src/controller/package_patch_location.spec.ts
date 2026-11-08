// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A PATCH of a package installed from a location, end to end: the controller
 * over a real `Environment` and `Package.create` on temp dirs. The refusal of
 * a change a reload would discard is judged under the package lock, against
 * the metadata the apply sees, so a reinstall that drops a whole-object echo
 * serves the new tree's own settings, and a PATCH racing another is judged
 * against what the other left.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import type { components } from "../api";
import { InstalledFromLocationError } from "../errors";
import { Environment } from "../service/environment";
import type { PackageAdmin } from "../service/environment_ports";
import { parsePackageName } from "../service/identity";
import { fakeOf } from "../test_helpers/store_fakes";
import { testRetrieval } from "../test_helpers/test_retrieval";
import { PackageController } from "./package.controller";

type ApiPackage = components["schemas"]["Package"];

const ROWS = `duckdb.sql("select 1 as id")`;
const V1 = "gs://bucket/pkg___1.0.0.zip";
const V2 = "gs://bucket/pkg___1.0.1.zip";

describe("PATCH of a package installed from a location", () => {
   let rootDir: string;
   let env: Environment;
   let controller: PackageController;
   /** What each location's tree says in its publisher.json. */
   const trees: Record<string, Record<string, unknown>> = {
      [V1]: {
         name: "pkg",
         description: "Sales",
         explores: ["a.malloy"],
         scope: "version",
      },
      [V2]: {
         name: "pkg",
         description: "Sales 1.0.1",
         explores: ["b.malloy"],
      },
   };
   /** Gates a download until resolved, when a test sets it. */
   let downloadGate: Promise<void> | undefined;
   let downloadStarted: () => void = () => {};

   async function writeTree(location: string, into: string): Promise<void> {
      await fs.mkdir(into, { recursive: true });
      await fs.writeFile(
         path.join(into, "publisher.json"),
         JSON.stringify(trees[location]),
      );
      await fs.writeFile(path.join(into, "a.malloy"), `source: a is ${ROWS}\n`);
      await fs.writeFile(path.join(into, "b.malloy"), `source: b is ${ROWS}\n`);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-loc-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      downloadGate = undefined;
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment: sinon.stub().resolves(env),
         addPackageToDatabase: sinon.stub().resolves(undefined),
         downloadPackageInto: async (
            _env: string,
            _pkg: string,
            location: string,
            target: string,
         ) => {
            downloadStarted();
            await downloadGate;
            await writeTree(location, target);
         },
      });
      controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
      // Installed from V1, as a publish records it.
      await env.installPackage(
         "pkg",
         (staging) => writeTree(V1, staging),
         undefined,
         { update: { name: "pkg", location: V1 } },
      );
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const metadata = async () =>
      (
         await env.getPackage(parsePackageName("pkg"), false)
      ).getPackageMetadata();

   it("accepts the whole package sent back with a new location, and serves the new tree's settings", async () => {
      const before = await metadata();
      expect(before.description).toBe("Sales");

      await controller.updatePackage("testEnv", "pkg", {
         ...before,
         location: V2,
      } as ApiPackage);

      const after = await metadata();
      expect(after.location).toBe(V2);
      // The echoed V1 settings were dropped, not applied over V2's.
      expect(after.description).toBe("Sales 1.0.1");
      expect(after.explores).toEqual(["b.malloy"]);
      expect(after.scope ?? "package").toBe("package");
   });

   it("refuses a reinstall carrying a value neither tree has, and keeps V1 serving", async () => {
      await expect(
         controller.updatePackage("testEnv", "pkg", {
            name: "pkg",
            location: V2,
            description: "Sales, renamed",
         }),
      ).rejects.toBeInstanceOf(InstalledFromLocationError);
      const after = await metadata();
      expect(after.location).toBe(V1);
      expect(after.description).toBe("Sales");
   });

   it("judges a PATCH that raced a reinstall against what the reinstall left", async () => {
      // B reads the package while V1 is resident and sends V1's description
      // back, an echo then. Before B takes the lock, A reinstalls from V2. B
      // is judged under the lock, against V2, where "Sales" is a change a
      // reload from V2 would discard.
      let releaseDownload!: () => void;
      downloadGate = new Promise((resolve) => (releaseDownload = resolve));
      const aDownloading = new Promise<void>(
         (resolve) => (downloadStarted = resolve),
      );
      let patchA: Promise<ApiPackage> | undefined;
      const realUpdate = env.updatePackage.bind(env);
      spyOn(env, "updatePackage").mockImplementation(async (...args) => {
         // B has decided, against V1, and is about to apply: A starts now.
         patchA = controller.updatePackage("testEnv", "pkg", {
            name: "pkg",
            location: V2,
         });
         await aDownloading;
         releaseDownload();
         return realUpdate(...args);
      });

      const patchB = controller.updatePackage("testEnv", "pkg", {
         name: "pkg",
         description: "Sales",
      });

      const error = await patchB.catch((e: Error) => e);
      expect(error).toBeInstanceOf(InstalledFromLocationError);
      expect((error as Error).message).toContain(`installed from ${V2}`);
      await patchA;
      const after = await metadata();
      expect(after.location).toBe(V2);
      expect(after.description).toBe("Sales 1.0.1");
   });
});
