// Lives under `tests/unit/` (not `src/`) on purpose: a sibling spec
// (environment_store.spec.ts) calls
// `mock.module("../storage/StorageManager", ...)`. Bun's module mocks
// persist process-wide across spec files, so a spec under `src/` that
// imports the real StorageManager would silently get the mock (which
// doesn't expose `getDuckDbConnection`). The integration runner gives
// us a clean module cache.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { PUBLISHER_CONFIG_NAME } from "../../../src/constants";
import { ThemeController } from "../../../src/controller/theme.controller";
import { BadRequestError, FrozenConfigError } from "../../../src/errors";
import { ThemeStore } from "../../../src/service/theme_store";
import { StorageManager } from "../../../src/storage/StorageManager";

const TEST_ROOT = path.join(process.cwd(), "test-temp-theme-controller");

function writeConfig(json: object) {
   if (!fs.existsSync(TEST_ROOT)) fs.mkdirSync(TEST_ROOT, { recursive: true });
   fs.writeFileSync(
      path.join(TEST_ROOT, PUBLISHER_CONFIG_NAME),
      JSON.stringify(json),
   );
}

async function build(
   configJson: object,
): Promise<{ controller: ThemeController; sm: StorageManager }> {
   writeConfig(configJson);
   const sm = new StorageManager({
      type: "duckdb",
      duckdb: { path: ":memory:" },
   });
   await sm.initialize(true);
   const store = new ThemeStore(sm, TEST_ROOT);
   return { controller: new ThemeController(store, TEST_ROOT), sm };
}

describe("ThemeController", () => {
   beforeEach(() => {
      if (fs.existsSync(TEST_ROOT)) {
         fs.rmSync(TEST_ROOT, { recursive: true, force: true });
      }
   });
   afterEach(() => {
      if (fs.existsSync(TEST_ROOT)) {
         fs.rmSync(TEST_ROOT, { recursive: true, force: true });
      }
   });

   it("getTheme returns the boot-seeded theme", async () => {
      const { controller } = await build({
         frozenConfig: false,
         theme: { palette: { series: ["#abc"] } },
         environments: [],
      });
      const theme = await controller.getTheme();
      expect(theme.palette?.series).toEqual(["#abc"]);
   });

   it("getTheme returns an empty object when no theme configured", async () => {
      const { controller } = await build({
         frozenConfig: false,
         environments: [],
      });
      expect(await controller.getTheme()).toEqual({});
   });

   it("putTheme writes and returns the theme", async () => {
      const { controller } = await build({
         frozenConfig: false,
         environments: [],
      });
      const saved = await controller.putTheme({
         palette: { series: ["#ff0080"] },
      });
      expect(saved.palette?.series).toEqual(["#ff0080"]);
      // The next GET returns the same value.
      expect((await controller.getTheme()).palette?.series).toEqual([
         "#ff0080",
      ]);
   });

   it("putTheme accepts an empty object as a clear-all-overrides signal", async () => {
      const { controller } = await build({
         frozenConfig: false,
         theme: { palette: { series: ["#aaa"] } },
         environments: [],
      });
      // Seed the editor's draft, then PUT {} to clear it.
      await controller.putTheme({ palette: { series: ["#edit"] } });
      const cleared = await controller.putTheme({});
      expect(cleared).toEqual({});
      expect(await controller.getTheme()).toEqual({});
   });

   it("putTheme rejects non-object payloads", async () => {
      const { controller } = await build({
         frozenConfig: false,
         environments: [],
      });
      await expect(controller.putTheme("nope")).rejects.toBeInstanceOf(
         BadRequestError,
      );
      await expect(controller.putTheme([1, 2, 3])).rejects.toBeInstanceOf(
         BadRequestError,
      );
   });

   it("putTheme throws FrozenConfigError when frozenConfig is true", async () => {
      const { controller } = await build({
         frozenConfig: true,
         environments: [],
      });
      await expect(
         controller.putTheme({ palette: { series: ["#abc"] } }),
      ).rejects.toBeInstanceOf(FrozenConfigError);
   });

   it("resetTheme falls back to the boot seed", async () => {
      const { controller } = await build({
         frozenConfig: false,
         theme: { palette: { series: ["#seed"] } },
         environments: [],
      });
      await controller.putTheme({ palette: { series: ["#edit"] } });
      const reseeded = await controller.resetTheme();
      expect(reseeded.palette?.series).toEqual(["#seed"]);
   });

   it("resetTheme throws FrozenConfigError when frozenConfig is true", async () => {
      const { controller } = await build({
         frozenConfig: true,
         environments: [],
      });
      await expect(controller.resetTheme()).rejects.toBeInstanceOf(
         FrozenConfigError,
      );
   });
});
