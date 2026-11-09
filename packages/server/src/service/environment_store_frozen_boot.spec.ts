// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `frozenConfig` must hold from the moment the store exists: write paths check
// it before they wait for initialization, so a write that arrives during a
// slow first boot, or after a failed one, must already be refused.
import { afterEach, expect, it } from "bun:test";
import { PackageController } from "../controller/package.controller";
import { FrozenConfigError } from "../errors";
import { installSettings } from "../test_helpers/settings_env";
import { stubStorageManager } from "../test_helpers/storage_manager_stub";
import { testRetrieval } from "../test_helpers/test_retrieval";
import { EnvironmentStore } from "./environment_store";

let restoreSettings: (() => void) | undefined;

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
});

function frozenStore(initialize: () => Promise<void>): EnvironmentStore {
   const { serverRoot, restore } = installSettings({
      file: { frozenConfig: true, environments: [] },
   });
   restoreSettings = restore;
   return new EnvironmentStore(serverRoot, {
      storageManager: stubStorageManager({ initialize }),
   });
}

it("refuses a write while storage is still opening", async () => {
   let release!: () => void;
   const store = frozenStore(
      () => new Promise<void>((resolve) => (release = resolve)),
   );
   void store.start();

   expect(store.publisherConfigIsFrozen).toBe(true);
   const packages = new PackageController(store, testRetrieval(store));
   await expect(packages.deletePackage("env", "pkg")).rejects.toBeInstanceOf(
      FrozenConfigError,
   );
   release();
});

it("still refuses a write after initialization failed", async () => {
   const store = frozenStore(async () => {
      throw new Error("storage unavailable");
   });
   await store.start();

   expect(store.publisherConfigIsFrozen).toBe(true);
   const packages = new PackageController(store, testRetrieval(store));
   await expect(packages.deletePackage("env", "pkg")).rejects.toBeInstanceOf(
      FrozenConfigError,
   );
});
