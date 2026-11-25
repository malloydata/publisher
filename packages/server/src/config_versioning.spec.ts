// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   getPersistStorageMode,
   getVersionPromotionMode,
   isPackageInlineFieldsEnabled,
   isVersioningEnabled,
} from "./config";
import { Settings } from "./settings";
import { withSettings } from "./test_helpers/settings_env";

describe("versionPromotion setting", () => {
   it("defaults to promoting on publish", () => {
      withSettings({ file: { frozenConfig: false, environments: [] } }, () =>
         expect(getVersionPromotionMode()).toBe("on-publish"),
      );
   });

   it("reads versionPromotion from publisher.config.json", () => {
      withSettings(
         {
            file: {
               frozenConfig: false,
               versionPromotion: "explicit",
               environments: [],
            },
         },
         () => expect(getVersionPromotionMode()).toBe("explicit"),
      );
   });

   it("lets PUBLISHER_VERSION_PROMOTION override the file", () => {
      withSettings(
         {
            env: { PUBLISHER_VERSION_PROMOTION: "on-publish" },
            file: {
               frozenConfig: false,
               versionPromotion: "explicit",
               environments: [],
            },
         },
         () => expect(getVersionPromotionMode()).toBe("on-publish"),
      );
   });

   it("is case- and whitespace-insensitive", () => {
      withSettings({ env: { PUBLISHER_VERSION_PROMOTION: " Explicit " } }, () =>
         expect(getVersionPromotionMode()).toBe("explicit"),
      );
   });

   it("refuses a value outside the set, naming where it came from", () => {
      withSettings(
         {
            file: {
               frozenConfig: false,
               versionPromotion: "manual",
               environments: [],
            },
         },
         () =>
            expect(() => getVersionPromotionMode()).toThrow(
               /^versionPromotion from .*publisher\.config\.json: expected one of "on-publish" or "explicit", got "manual"$/,
            ),
      );
      withSettings({ env: { PUBLISHER_VERSION_PROMOTION: "later" } }, () =>
         expect(() => getVersionPromotionMode()).toThrow(
            'versionPromotion from PUBLISHER_VERSION_PROMOTION: expected one of "on-publish" or "explicit", got "later"',
         ),
      );
   });

   it("fails startup, rather than giving the default, for a config file that cannot be parsed", () => {
      withSettings({ file: "{ not json" }, () =>
         expect(Settings.current().fileError?.message).toContain(
            "Failed to parse",
         ),
      );
   });
});

// PERSIST_STORAGE_MODE: blank is off, case is ignored, and a typo throws.
describe("PERSIST_STORAGE_MODE", () => {
   it("defaults to off when unset or blank", () => {
      withSettings({}, () => expect(getPersistStorageMode()).toBe("off"));
      withSettings({ env: { PERSIST_STORAGE_MODE: "  " } }, () =>
         expect(getPersistStorageMode()).toBe("off"),
      );
   });

   it("ignores case and surrounding space", () => {
      withSettings({ env: { PERSIST_STORAGE_MODE: " Write-Only " } }, () =>
         expect(getPersistStorageMode()).toBe("write-only"),
      );
   });

   it("refuses a value outside the set", () => {
      withSettings({ env: { PERSIST_STORAGE_MODE: "read-only" } }, () =>
         expect(() => getPersistStorageMode()).toThrow(
            'persist.storageMode from PERSIST_STORAGE_MODE: expected one of "off", "write-only" or "on", got "read-only"',
         ),
      );
   });
});

// The transition flag: on unless set to off, so a server that sets nothing
// publishes versions.
describe("PUBLISHER_PACKAGE_VERSIONING", () => {
   it("is on when unset or blank", () => {
      withSettings({}, () => expect(isVersioningEnabled()).toBe(true));
      withSettings({ env: { PUBLISHER_PACKAGE_VERSIONING: " " } }, () =>
         expect(isVersioningEnabled()).toBe(true),
      );
   });

   it("is on only for on, ignoring case and surrounding space", () => {
      withSettings({ env: { PUBLISHER_PACKAGE_VERSIONING: " On " } }, () =>
         expect(isVersioningEnabled()).toBe(true),
      );
      withSettings({ env: { PUBLISHER_PACKAGE_VERSIONING: "off" } }, () =>
         expect(isVersioningEnabled()).toBe(false),
      );
   });

   it("refuses a value outside on and off, naming the setting", () => {
      withSettings({ env: { PUBLISHER_PACKAGE_VERSIONING: "true" } }, () =>
         expect(() => isVersioningEnabled()).toThrow(
            'packageVersioning from PUBLISHER_PACKAGE_VERSIONING: expected one of "off" or "on", got "true"',
         ),
      );
   });
});

// The transition flag for the fields that moved off Package: on unless set to
// off, so a server that sets nothing answers as it did before they moved.
describe("PUBLISHER_PACKAGE_INLINE_FIELDS", () => {
   it("is on when unset, empty or on, in any case", () => {
      withSettings({}, () => expect(isPackageInlineFieldsEnabled()).toBe(true));
      for (const value of ["", "on", " ON "]) {
         withSettings({ env: { PUBLISHER_PACKAGE_INLINE_FIELDS: value } }, () =>
            expect(isPackageInlineFieldsEnabled()).toBe(true),
         );
      }
   });

   it("is off when off", () => {
      withSettings({ env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "off" } }, () =>
         expect(isPackageInlineFieldsEnabled()).toBe(false),
      );
   });

   it("refuses any other value, naming the setting and the choices", () => {
      withSettings({ env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "false" } }, () =>
         expect(() => isPackageInlineFieldsEnabled()).toThrow(
            'packageInlineFields from PUBLISHER_PACKAGE_INLINE_FIELDS: expected one of "off" or "on", got "false"',
         ),
      );
   });
});
