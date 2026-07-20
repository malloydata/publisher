import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "fs";
import * as path from "path";
import { PUBLISHER_CONFIG_NAME, TEMP_DIR_PATH } from "../constants";

/**
 * A relative package `location` must anchor at the directory holding the config
 * that declared it, not at the server root. Those are the same path in the
 * common case (`<serverRoot>/publisher.config.json`), so a spec that lets them
 * coincide cannot tell the two rules apart.
 *
 * Here they are deliberately different directories, driven through the real
 * `PUBLISHER_CONFIG_PATH` that `--config` sets, so this exercises the actual
 * resolution rather than a stub of it.
 */
const serverRootPath = path.join(TEMP_DIR_PATH, "anchoring-server-root");
const configDirPath = path.join(TEMP_DIR_PATH, "anchoring-config-dir");

mock.module("../storage/StorageManager", () => ({
   StorageManager: class MockStorageManager {
      async initialize(): Promise<void> {}
      getRepository() {
         return {
            listEnvironments: async () => [],
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
         };
      }
   },
   StorageConfig: {} as Record<string, unknown>,
}));

const { EnvironmentStore } = await import("./environment_store");

describe("relative package locations anchor at the config directory", () => {
   const savedConfigPath = process.env.PUBLISHER_CONFIG_PATH;

   beforeEach(() => {
      for (const dir of [serverRootPath, configDirPath]) {
         rmSync(dir, { recursive: true, force: true });
         mkdirSync(dir, { recursive: true });
      }
      const configPath = path.join(configDirPath, PUBLISHER_CONFIG_NAME);
      writeFileSync(configPath, JSON.stringify({ environments: [] }));
      process.env.PUBLISHER_CONFIG_PATH = configPath;
   });

   afterEach(() => {
      if (savedConfigPath === undefined) {
         delete process.env.PUBLISHER_CONFIG_PATH;
      } else {
         process.env.PUBLISHER_CONFIG_PATH = savedConfigPath;
      }
      for (const dir of [serverRootPath, configDirPath]) {
         rmSync(dir, { recursive: true, force: true });
      }
   });

   it("mounts a package that sits next to the config, not next to the server root", async () => {
      // A `sales` directory exists in BOTH candidate anchors, with different
      // contents. If resolution silently reverted to server-root anchoring it
      // would still mount successfully — off the decoy — so the existence
      // check alone could not catch the regression; the content check does.
      const packageSource = path.join(configDirPath, "sales");
      mkdirSync(packageSource, { recursive: true });
      writeFileSync(
         path.join(packageSource, "publisher.json"),
         JSON.stringify({ name: "sales" }),
      );
      const decoySource = path.join(serverRootPath, "sales");
      mkdirSync(decoySource, { recursive: true });
      writeFileSync(
         path.join(decoySource, "publisher.json"),
         JSON.stringify({ name: "decoy-from-server-root" }),
      );

      const store = new EnvironmentStore(serverRootPath);
      await store.addEnvironment({
         name: "probe",
         packages: [{ name: "sales", location: "./sales" }],
         connections: [],
      } as never);

      const mountedManifest = path.join(
         serverRootPath,
         "publisher_data",
         "probe",
         "sales",
         "publisher.json",
      );
      expect(existsSync(mountedManifest)).toBe(true);
      expect(JSON.parse(readFileSync(mountedManifest, "utf-8")).name).toBe(
         "sales",
      );
   });
});
