// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   getPersistStorageMode,
   getPublisherConfig,
   getVersionPromotionMode,
   isVersioningEnabled,
} from "./config";

describe("package versioning settings", () => {
   let root: string;
   const saved = {
      versioning: process.env.PUBLISHER_PACKAGE_VERSIONING,
      promotion: process.env.PUBLISHER_VERSION_PROMOTION,
   };

   beforeEach(() => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "versioning-config-"));
      delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      delete process.env.PUBLISHER_VERSION_PROMOTION;
   });

   afterEach(() => {
      fs.rmSync(root, { recursive: true, force: true });
      for (const [name, value] of [
         ["PUBLISHER_PACKAGE_VERSIONING", saved.versioning],
         ["PUBLISHER_VERSION_PROMOTION", saved.promotion],
      ] as const) {
         if (value === undefined) delete process.env[name];
         else process.env[name] = value;
      }
   });

   const writeConfig = (body: unknown) =>
      fs.writeFileSync(
         path.join(root, "publisher.config.json"),
         JSON.stringify(body),
      );

   it("defaults to versioning off, promoted on publish", () => {
      writeConfig({ frozenConfig: false, environments: [] });
      expect(isVersioningEnabled()).toBe(false);
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });

   it("turns versioning on only from the environment", () => {
      // A transition switch, read from the environment alone: a
      // publisher.config.json key does nothing.
      writeConfig({
         frozenConfig: false,
         packageVersioning: "on",
         environments: [],
      });
      expect(isVersioningEnabled()).toBe(false);
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      expect(isVersioningEnabled()).toBe(true);
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      expect(isVersioningEnabled()).toBe(false);
      process.env.PUBLISHER_PACKAGE_VERSIONING = "";
      expect(isVersioningEnabled()).toBe(false);
   });

   it("reads versionPromotion from publisher.config.json", () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      writeConfig({
         frozenConfig: false,
         versionPromotion: "explicit",
         environments: [],
      });
      expect(getVersionPromotionMode(root)).toBe("explicit");
      expect(getPublisherConfig(root)).toMatchObject({
         versionPromotion: "explicit",
      });
   });

   it("lets PUBLISHER_VERSION_PROMOTION override the file", () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      writeConfig({
         frozenConfig: false,
         versionPromotion: "explicit",
         environments: [],
      });
      process.env.PUBLISHER_VERSION_PROMOTION = "on-publish";
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });

   it("is case- and whitespace-insensitive", () => {
      writeConfig({ frozenConfig: false, environments: [] });
      process.env.PUBLISHER_PACKAGE_VERSIONING = " ON ";
      process.env.PUBLISHER_VERSION_PROMOTION = "Explicit";
      expect(isVersioningEnabled()).toBe(true);
      expect(getVersionPromotionMode(root)).toBe("explicit");
   });

   it("refuses a value outside the set, naming where it came from", () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "yes";
      expect(() => isVersioningEnabled()).toThrow(
         'PUBLISHER_PACKAGE_VERSIONING must be one of off | on (got "yes")',
      );
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      writeConfig({
         frozenConfig: false,
         versionPromotion: "manual",
         environments: [],
      });
      expect(() => getVersionPromotionMode(root)).toThrow(
         '"versionPromotion" in publisher.config.json must be one of on-publish | explicit (got "manual")',
      );
   });

   it("ignores versionPromotion entirely with versioning off", () => {
      writeConfig({
         frozenConfig: false,
         versionPromotion: "manual",
         environments: [],
      });
      expect(() => getPublisherConfig(root)).not.toThrow();
      expect(getPublisherConfig(root)).not.toHaveProperty("versionPromotion");
   });

   it("gives the promotion default for a config file that cannot be parsed", () => {
      fs.writeFileSync(path.join(root, "publisher.config.json"), "{ not json");
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });
});

// PERSIST_STORAGE_MODE goes through the same closed-set parser, so it keeps the
// behaviour it had before sharing it: blank is off, case is ignored, and a typo
// throws with the same message.
describe("PERSIST_STORAGE_MODE", () => {
   const saved = process.env.PERSIST_STORAGE_MODE;

   afterEach(() => {
      if (saved === undefined) delete process.env.PERSIST_STORAGE_MODE;
      else process.env.PERSIST_STORAGE_MODE = saved;
   });

   it("defaults to off when unset or blank", () => {
      delete process.env.PERSIST_STORAGE_MODE;
      expect(getPersistStorageMode()).toBe("off");
      process.env.PERSIST_STORAGE_MODE = "  ";
      expect(getPersistStorageMode()).toBe("off");
   });

   it("ignores case and surrounding space", () => {
      process.env.PERSIST_STORAGE_MODE = " Write-Only ";
      expect(getPersistStorageMode()).toBe("write-only");
   });

   it("refuses a value outside the set", () => {
      process.env.PERSIST_STORAGE_MODE = "read-only";
      expect(() => getPersistStorageMode()).toThrow(
         'PERSIST_STORAGE_MODE must be one of off | write-only | on (got "read-only")',
      );
   });
});
