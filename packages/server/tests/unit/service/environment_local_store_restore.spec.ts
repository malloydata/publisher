// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Lives under `tests/unit/` (not `src/`) on purpose: it builds a real
// EnvironmentStore over the real StorageManager, which sibling specs under
// `src/` replace with `mock.module(...)`. Bun's module mocks persist
// process-wide across spec files, so under `src/` it would get the mock; the
// integration runner gives it a clean module cache (see theme_store.spec.ts).

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageController } from "../../../src/controller/package.controller";
import { EnvironmentStore } from "../../../src/service/environment_store";

/**
 * Where an UNVERSIONED package restores its serve bindings from the worker's
 * own materialization records when it (re)loads — the behaviour `main` had
 * before package versions, which the versions work must not widen:
 *
 *  - an environment created at runtime (over the API — how a control plane
 *    creates one) does not: a reinstall with no `manifestLocation` serves live
 *    until a build binds it, so a run the host never sanctioned is not served;
 *  - an environment reloaded from the database at startup does, so serving
 *    survives a restart.
 */

const FIXTURE = path.resolve(__dirname, "../../fixtures/persist-multi-level");
const ENV = "org___proj";
const PKG = "pkg___1.0.0";

let root: string;
let source: string;
let store: EnvironmentStore | undefined;

beforeEach(() => {
   delete process.env.PUBLISHER_PACKAGE_VERSIONING;
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "local-store-restore-")),
   );
   source = path.join(root, "source");
   fs.cpSync(FIXTURE, source, { recursive: true });
   fs.mkdirSync(path.join(root, "server"));
});

afterEach(async () => {
   await closeStore();
   fs.rmSync(root, { recursive: true, force: true });
});

async function openStore(): Promise<EnvironmentStore> {
   store = new EnvironmentStore(path.join(root, "server"));
   await store.finishedInitialization;
   return store;
}

async function closeStore() {
   if (!store) return;
   const internals = store as unknown as {
      storageManager: { close(): Promise<void> };
   };
   await internals.storageManager.close();
   store = undefined;
}

/**
 * A run of the package this worker committed locally, as an orchestrated
 * build whose finalize the host abandoned leaves behind: MANIFEST_FILE_READY
 * with an entry for the package's base source.
 */
async function commitLocalRun(current: EnvironmentStore) {
   const pkg = await current.getEnvironment(ENV).then((e) => e.getPackage(PKG));
   const sources = Object.values(
      (pkg.getBuildPlan() as { sources?: Record<string, unknown> } | undefined)
         ?.sources ?? {},
   ) as { name: string; sourceEntityId: string }[];
   const base = sources.find((s) => s.name === "orders_base");
   expect(base).toBeDefined();
   const repository = (
      current as unknown as {
         storageManager: {
            getRepository(): {
               getEnvironmentByName(name: string): Promise<{ id: string }>;
               createMaterialization(
                  environmentId: string,
                  packageName: string,
                  status: string,
                  metadata: Record<string, unknown>,
               ): Promise<{ id: string }>;
               updateMaterialization(
                  id: string,
                  patch: Record<string, unknown>,
               ): Promise<unknown>;
            };
         };
      }
   ).storageManager.getRepository();
   const environment = await repository.getEnvironmentByName(ENV);
   const run = await repository.createMaterialization(
      environment.id,
      PKG,
      "PENDING",
      { mode: "orchestrated" },
   );
   await repository.updateMaterialization(run.id, {
      status: "MANIFEST_FILE_READY",
      manifest: {
         strict: true,
         entries: {
            [base!.sourceEntityId]: {
               sourceEntityId: base!.sourceEntityId,
               sourceName: "orders_base",
               physicalTableName: "host_gen_17_orders_base",
               connectionName: "duckdb",
            },
         },
      },
   });
}

describe("restoring an unversioned package's bindings from the local store", () => {
   it("an environment created at runtime serves a reinstalled package live, as main does", async () => {
      const current = await openStore();
      await current.addEnvironment({ name: ENV, connections: [] } as never);
      const packages = new PackageController(current);
      const install = () =>
         packages.addPackage(ENV, {
            name: PKG,
            location: source,
            manifestLocation: null,
         } as never);

      const first = await install();
      expect(first.getPackageMetadata().manifestBindingStatus).toBe("unbound");

      await commitLocalRun(current);

      // The host installs the same package again, still with no manifest:
      // nothing it sanctioned names that run, so nothing is bound to it.
      const again = await install();
      const metadata = again.getPackageMetadata();
      expect(metadata.manifestBindingStatus).toBe("unbound");
      expect(metadata.manifestEntryCount ?? 0).toBe(0);
   });

   it("an environment reloaded from the database at startup restores them", async () => {
      const current = await openStore();
      await current.addEnvironment({ name: ENV, connections: [] } as never);
      await new PackageController(current).addPackage(ENV, {
         name: PKG,
         location: source,
         manifestLocation: null,
      } as never);
      await commitLocalRun(current);
      await closeStore();

      // A restart: the environment comes back from the database, and the
      // package's last ready run is bound again on load.
      const restarted = await openStore();
      const pkg = await restarted
         .getEnvironment(ENV)
         .then((e) => e.getPackage(PKG));
      const metadata = pkg.getPackageMetadata();
      expect(metadata.manifestBindingStatus).toBe("bound");
      expect(metadata.manifestEntryCount).toBe(1);
   });
});
