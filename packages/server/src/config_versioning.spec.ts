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
} from "./config";

describe("versionPromotion setting", () => {
   let root: string;
   const saved = process.env.PUBLISHER_VERSION_PROMOTION;

   beforeEach(() => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "versioning-config-"));
      delete process.env.PUBLISHER_VERSION_PROMOTION;
   });

   afterEach(() => {
      fs.rmSync(root, { recursive: true, force: true });
      if (saved === undefined) delete process.env.PUBLISHER_VERSION_PROMOTION;
      else process.env.PUBLISHER_VERSION_PROMOTION = saved;
   });

   const writeConfig = (body: unknown) =>
      fs.writeFileSync(
         path.join(root, "publisher.config.json"),
         JSON.stringify(body),
      );

   it("defaults to promoting on publish", () => {
      writeConfig({ frozenConfig: false, environments: [] });
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });

   it("reads versionPromotion from publisher.config.json", () => {
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
      process.env.PUBLISHER_VERSION_PROMOTION = " Explicit ";
      expect(getVersionPromotionMode(root)).toBe("explicit");
   });

   it("refuses a value outside the set, naming where it came from", () => {
      writeConfig({
         frozenConfig: false,
         versionPromotion: "manual",
         environments: [],
      });
      expect(() => getVersionPromotionMode(root)).toThrow(
         '"versionPromotion" in publisher.config.json must be one of on-publish | explicit (got "manual")',
      );
      process.env.PUBLISHER_VERSION_PROMOTION = "later";
      expect(() => getVersionPromotionMode(root)).toThrow(
         'PUBLISHER_VERSION_PROMOTION must be one of on-publish | explicit (got "later")',
      );
   });

   it("gives the default for a config file that cannot be parsed", () => {
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
