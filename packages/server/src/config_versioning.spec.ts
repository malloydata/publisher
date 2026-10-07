// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   getPackageVersioningMode,
   getPublisherConfig,
   getVersionPromotionMode,
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

   it("defaults to unversioned, promoted on publish", () => {
      writeConfig({ frozenConfig: false, environments: [] });
      expect(getPackageVersioningMode(root)).toBe("off");
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });

   it("reads both settings from publisher.config.json", () => {
      writeConfig({
         frozenConfig: false,
         packageVersioning: "on",
         versionPromotion: "explicit",
         environments: [],
      });
      expect(getPackageVersioningMode(root)).toBe("on");
      expect(getVersionPromotionMode(root)).toBe("explicit");
      expect(getPublisherConfig(root)).toMatchObject({
         packageVersioning: "on",
         versionPromotion: "explicit",
      });
   });

   it("lets the environment variable override the file", () => {
      writeConfig({
         frozenConfig: false,
         packageVersioning: "on",
         versionPromotion: "explicit",
         environments: [],
      });
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      process.env.PUBLISHER_VERSION_PROMOTION = "on-publish";
      expect(getPackageVersioningMode(root)).toBe("off");
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });

   it("is case- and whitespace-insensitive", () => {
      writeConfig({ frozenConfig: false, environments: [] });
      process.env.PUBLISHER_PACKAGE_VERSIONING = " ON ";
      process.env.PUBLISHER_VERSION_PROMOTION = "Explicit";
      expect(getPackageVersioningMode(root)).toBe("on");
      expect(getVersionPromotionMode(root)).toBe("explicit");
   });

   it("refuses a value outside the set, naming where it came from", () => {
      writeConfig({
         frozenConfig: false,
         packageVersioning: "yes",
         environments: [],
      });
      expect(() => getPackageVersioningMode(root)).toThrow(
         '"packageVersioning" in publisher.config.json must be one of off | on (got "yes")',
      );
      process.env.PUBLISHER_VERSION_PROMOTION = "manual";
      expect(() => getVersionPromotionMode(root)).toThrow(
         'PUBLISHER_VERSION_PROMOTION must be one of on-publish | explicit (got "manual")',
      );
   });

   it("gives the defaults for a config file that cannot be parsed", () => {
      fs.writeFileSync(path.join(root, "publisher.config.json"), "{ not json");
      expect(getPackageVersioningMode(root)).toBe("off");
      expect(getVersionPromotionMode(root)).toBe("on-publish");
   });
});
