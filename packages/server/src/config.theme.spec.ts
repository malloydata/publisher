import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import {
   getInstanceTheme,
   getProcessedPublisherConfig,
   mergeThemes,
   type Theme,
} from "./config";
import { PUBLISHER_CONFIG_NAME } from "./constants";

const TEST_ROOT = path.join(process.cwd(), "test-temp-theme-config");
const CONFIG_PATH = path.join(TEST_ROOT, PUBLISHER_CONFIG_NAME);

function writeConfig(json: object) {
   if (!fs.existsSync(TEST_ROOT)) {
      fs.mkdirSync(TEST_ROOT, { recursive: true });
   }
   fs.writeFileSync(CONFIG_PATH, JSON.stringify(json));
}

describe("Theme cascade in publisher.config.json", () => {
   beforeEach(() => {
      if (fs.existsSync(CONFIG_PATH)) fs.unlinkSync(CONFIG_PATH);
   });

   afterEach(() => {
      if (fs.existsSync(CONFIG_PATH)) fs.unlinkSync(CONFIG_PATH);
      if (fs.existsSync(TEST_ROOT)) {
         fs.rmdirSync(TEST_ROOT, { recursive: true });
      }
   });

   it("returns no theme when none is set", () => {
      writeConfig({
         frozenConfig: false,
         environments: [
            {
               name: "default",
               packages: [{ name: "pkg", location: "/tmp/pkg" }],
            },
         ],
      });
      const cfg = getProcessedPublisherConfig(TEST_ROOT);
      expect(cfg.theme).toBeUndefined();
      expect(cfg.environments[0].theme).toBeUndefined();
   });

   it("exposes the instance theme on the top-level config", () => {
      writeConfig({
         frozenConfig: false,
         theme: {
            defaultMode: "dark",
            palette: { series: { light: ["#ff0080"] } },
         },
         environments: [
            {
               name: "default",
               packages: [{ name: "pkg", location: "/tmp/pkg" }],
            },
         ],
      });
      const cfg = getProcessedPublisherConfig(TEST_ROOT);
      expect(cfg.theme?.defaultMode).toBe("dark");
      expect(cfg.theme?.palette?.series?.light).toEqual(["#ff0080"]);
      expect(cfg.environments[0].theme?.palette?.series?.light).toEqual([
         "#ff0080",
      ]);
   });

   it("environment theme overrides instance per key", () => {
      writeConfig({
         frozenConfig: false,
         theme: {
            defaultMode: "light",
            palette: {
               series: { light: ["#aaa"] },
               background: { light: "#fff" },
            },
            font: { family: { light: "Inter" } },
         },
         environments: [
            {
               name: "marketing",
               packages: [{ name: "pkg", location: "/tmp/pkg" }],
               theme: {
                  palette: { series: { light: ["#cc0000"] } },
               },
            },
         ],
      });
      const cfg = getProcessedPublisherConfig(TEST_ROOT);
      expect(cfg.environments[0].theme?.palette?.series?.light).toEqual([
         "#cc0000",
      ]);
      // Font and background fall through from instance default.
      expect(cfg.environments[0].theme?.font?.family?.light).toBe("Inter");
      expect(cfg.environments[0].theme?.palette?.background?.light).toBe(
         "#fff",
      );
   });

   it("drops invalid defaultMode values without losing the rest", () => {
      writeConfig({
         frozenConfig: false,
         theme: {
            defaultMode: "nonsense",
            palette: { series: { light: ["#a"] } },
         },
         environments: [
            {
               name: "default",
               packages: [{ name: "pkg", location: "/tmp/pkg" }],
            },
         ],
      });
      const cfg = getProcessedPublisherConfig(TEST_ROOT);
      expect(cfg.theme?.defaultMode).toBeUndefined();
      expect(cfg.theme?.palette?.series?.light).toEqual(["#a"]);
   });

   it("getInstanceTheme returns the same instance theme", () => {
      writeConfig({
         frozenConfig: false,
         theme: { palette: { series: { light: ["#abc"] } } },
         environments: [],
      });
      const t = getInstanceTheme(TEST_ROOT);
      expect(t?.palette?.series?.light).toEqual(["#abc"]);
   });
});

describe("mergeThemes", () => {
   it("returns undefined when both sides are missing", () => {
      expect(mergeThemes(undefined, undefined)).toBeUndefined();
   });

   it("returns the override when base is missing", () => {
      const o: Theme = { palette: { series: { light: ["#x"] } } };
      expect(mergeThemes(undefined, o)).toEqual(o);
   });

   it("merges palette sub-objects per mode", () => {
      const base: Theme = {
         palette: { background: { light: "#fff", dark: "#000" } },
      };
      const o: Theme = {
         palette: { background: { dark: "#111" } },
      };
      const merged = mergeThemes(base, o);
      expect(merged?.palette?.background?.light).toBe("#fff");
      expect(merged?.palette?.background?.dark).toBe("#111");
   });
});
