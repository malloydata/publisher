// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { mkdirSync, rmSync, writeFileSync } from "fs";
import * as path from "path";
import { PUBLISHER_CONFIG_NAME, TEMP_DIR_PATH } from "../constants";

/**
 * Pins the precedence that `malloy_getStatus`'s setup advice depends on.
 *
 * `describeWorkspaceSetup` (mcp/tools/workspace_setup.ts) tells a user whether
 * a plain restart is enough after they fix a config, or whether they need
 * `--init`. That advice rests on one rule: `initialize()` loads environments
 * from the STORE when the store has any, and from the config file only when it
 * does not. So a config edit is silently ignored on a restart of a server that
 * already has stored environments, which is exactly the loop the advice exists
 * to prevent.
 *
 * That rule was established by running a server and watching it, and until this
 * spec existed nothing connected the rule to the code implementing it. If the
 * precedence changes, the advice becomes confidently wrong with no test failing
 * anywhere — the failure mode is prose about server behaviour living far from
 * the behaviour, which has now bitten this area more than once.
 *
 * The store is mocked at the repository, because the question is which SOURCE
 * initialize() reads, not whether DuckDB works.
 */
const serverRootPath = path.join(TEMP_DIR_PATH, "config-precedence-root");

/** Flipped per test to stand for "the database already holds environments". */
let storedEnvironments: Array<Record<string, unknown>> = [];

mock.module("../storage/StorageManager", () => ({
   StorageManager: class MockStorageManager {
      async initialize(): Promise<void> {}
      getRepository() {
         return {
            listEnvironments: async () => storedEnvironments,
            getEnvironmentByName: async () => null,
            createEnvironment: async (data: Record<string, unknown>) => ({
               id: "env-id",
               name: data.name,
               path: data.path,
            }),
            listPackages: async () => [],
            getPackageByName: async () => null,
            createPackage: async (data: Record<string, unknown>) => ({
               id: "pkg-id",
               name: data.name,
            }),
            listConnections: async () => [],
            // Absent here logged an error with a stack on every run of this file.
            listStorageDestinations: async () => [],
         };
      }
   },
   StorageConfig: {} as Record<string, unknown>,
}));

const { EnvironmentStore } = await import("./environment_store");

describe("config is read only when the store has no environments", () => {
   const savedConfigPath = process.env.PUBLISHER_CONFIG_PATH;
   const savedInit = process.env.INITIALIZE_STORAGE;

   beforeEach(() => {
      storedEnvironments = [];
      rmSync(serverRootPath, { recursive: true, force: true });
      mkdirSync(serverRootPath, { recursive: true });
      // An environment declaring no valid package is skipped at load, so the
      // config env needs a real one for this to test precedence rather than
      // that rule.
      const pkgDir = path.join(serverRootPath, "sales");
      mkdirSync(pkgDir, { recursive: true });
      writeFileSync(
         path.join(pkgDir, "publisher.json"),
         JSON.stringify({ name: "sales" }),
      );
      const configPath = path.join(serverRootPath, PUBLISHER_CONFIG_NAME);
      writeFileSync(
         configPath,
         JSON.stringify({
            environments: [
               {
                  name: "from-config",
                  packages: [{ name: "sales", location: "./sales" }],
               },
            ],
         }),
      );
      process.env.PUBLISHER_CONFIG_PATH = configPath;
      delete process.env.INITIALIZE_STORAGE;
   });

   afterEach(() => {
      if (savedConfigPath === undefined)
         delete process.env.PUBLISHER_CONFIG_PATH;
      else process.env.PUBLISHER_CONFIG_PATH = savedConfigPath;
      if (savedInit === undefined) delete process.env.INITIALIZE_STORAGE;
      else process.env.INITIALIZE_STORAGE = savedInit;
      rmSync(serverRootPath, { recursive: true, force: true });
   });

   it("loads the config when the store is empty, so a plain restart picks up an edit", async () => {
      storedEnvironments = [];
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      const names = (await store.listEnvironments(true)).map((e) => e.name);
      expect(names).toContain("from-config");
   });

   it("does NOT read the config when the store already holds environments", async () => {
      // The whole reason the setup advice asks for `--init`: the config
      // declares `from-config`, the store holds `from-store`, and a plain boot
      // serves the store's set. If this ever starts returning "from-config",
      // the `--init` advice in workspace_setup.ts is wrong and must change
      // with it.
      // The stored environment's directory must EXIST: without it initialize()
      // takes the files-missing branch and returns an empty list, which
      // satisfies not.toContain("from-config") without showing that the stored
      // set is what gets served. Asserting from-store is present is the half
      // that actually demonstrates precedence.
      const storedPath = path.join(serverRootPath, "stored-env");
      mkdirSync(storedPath, { recursive: true });
      storedEnvironments = [{ id: "e1", name: "from-store", path: storedPath }];
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      const names = (await store.listEnvironments(true)).map((e) => e.name);
      expect(names).toContain("from-store");
      expect(names).not.toContain("from-config");
   });

   it("reads the config again under --init, even with a non-empty store", async () => {
      // The other half of the advice: `--init` is what makes the edit land.
      storedEnvironments = [
         { id: "e1", name: "from-store", path: path.join(serverRootPath, "x") },
      ];
      process.env.INITIALIZE_STORAGE = "true";
      const store = new EnvironmentStore(serverRootPath);
      await store.finishedInitialization;
      const names = (await store.listEnvironments(true)).map((e) => e.name);
      expect(names).toContain("from-config");
   });
});
