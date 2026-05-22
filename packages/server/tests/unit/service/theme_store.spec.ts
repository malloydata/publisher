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
import { ThemeStore } from "../../../src/service/theme_store";
import { StorageManager } from "../../../src/storage/StorageManager";

const TEST_ROOT = path.join(process.cwd(), "test-temp-theme-store");

function writeConfig(json: object) {
   if (!fs.existsSync(TEST_ROOT)) fs.mkdirSync(TEST_ROOT, { recursive: true });
   fs.writeFileSync(
      path.join(TEST_ROOT, PUBLISHER_CONFIG_NAME),
      JSON.stringify(json),
   );
}

async function makeStorage(): Promise<StorageManager> {
   const sm = new StorageManager({
      type: "duckdb",
      duckdb: { path: ":memory:" },
   });
   await sm.initialize(true);
   return sm;
}

describe("ThemeStore", () => {
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

   it("returns undefined when no theme is set and no boot seed exists", async () => {
      const sm = await makeStorage();
      writeConfig({ frozenConfig: false, environments: [] });
      const store = new ThemeStore(sm, TEST_ROOT);
      expect(await store.get()).toBeUndefined();
   });

   it("seeds from publisher.config.json on first read", async () => {
      const sm = await makeStorage();
      writeConfig({
         frozenConfig: false,
         theme: { palette: { series: ["#abc"] } },
         environments: [],
      });
      const store = new ThemeStore(sm, TEST_ROOT);
      const seeded = await store.get();
      expect(seeded?.palette?.series).toEqual(["#abc"]);
   });

   it("set then get round-trips", async () => {
      const sm = await makeStorage();
      writeConfig({ frozenConfig: false, environments: [] });
      const store = new ThemeStore(sm, TEST_ROOT);
      const next = { palette: { series: ["#ff0080", "#00d4ff"] } };
      await store.set(next);
      const after = await store.get();
      expect(after?.palette?.series).toEqual(["#ff0080", "#00d4ff"]);
   });

   it("second set overwrites the first", async () => {
      const sm = await makeStorage();
      writeConfig({ frozenConfig: false, environments: [] });
      const store = new ThemeStore(sm, TEST_ROOT);
      await store.set({ palette: { series: ["#111"] } });
      await store.set({ palette: { series: ["#222"] } });
      expect((await store.get())?.palette?.series).toEqual(["#222"]);
   });

   it("reset falls back to the boot seed", async () => {
      const sm = await makeStorage();
      writeConfig({
         frozenConfig: false,
         theme: { palette: { series: ["#seed"] } },
         environments: [],
      });
      const store = new ThemeStore(sm, TEST_ROOT);
      await store.set({ palette: { series: ["#edited"] } });
      const reseeded = await store.reset();
      expect(reseeded?.palette?.series).toEqual(["#seed"]);
   });

   it("reset clears completely when no boot seed exists", async () => {
      const sm = await makeStorage();
      writeConfig({ frozenConfig: false, environments: [] });
      const store = new ThemeStore(sm, TEST_ROOT);
      await store.set({ palette: { series: ["#edited"] } });
      const reseeded = await store.reset();
      expect(reseeded).toBeUndefined();
   });

   it("sanitises stale schema shapes on read", async () => {
      // Simulates an upgraded install where the persisted payload
      // still uses the old per-mode `series` object plus a per-mode
      // font block. Both are dropped by the sanitiser; the per-mode
      // colour keys (background) survive.
      const sm = await makeStorage();
      writeConfig({ frozenConfig: false, environments: [] });
      const now = new Date().toISOString();
      const stalePayload = JSON.stringify({
         palette: {
            // Old shape — must be dropped on read.
            series: { light: ["#stale"], dark: ["#stale-dark"] },
            // Current shape — must survive.
            background: { light: "#aabbcc" },
         },
         font: {
            // Old shape — must be dropped.
            family: { light: "Inter", dark: "Inter" },
         },
      });
      await sm.getDuckDbConnection().run(
         `INSERT INTO themes (id, payload, created_at, updated_at)
          VALUES (?, ?, ?, ?)`,
         ["default", stalePayload, now, now],
      );
      const store = new ThemeStore(sm, TEST_ROOT);
      const loaded = await store.get();
      // Stale keys gone.
      expect(loaded?.palette?.series).toBeUndefined();
      expect(loaded?.font).toBeUndefined();
      // Current-shape key carried over.
      expect(loaded?.palette?.background?.light).toBe("#aabbcc");
   });
});
