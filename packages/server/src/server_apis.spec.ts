// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { getServerApiSettings, type ServerApiSettings } from "./config";
import { assertServerApis } from "./server_apis";

const BOTH: ServerApiSettings = {
   servingApiEnabled: true,
   adminApiEnabled: true,
   declaresEnvironments: false,
};

describe("getServerApiSettings", () => {
   const roots: string[] = [];
   afterEach(() => {
      while (roots.length)
         rmSync(roots.pop()!, { recursive: true, force: true });
   });
   function rootWith(config?: string): string {
      const root = mkdtempSync(join(tmpdir(), "publisher-apis-"));
      roots.push(root);
      if (config !== undefined) {
         writeFileSync(join(root, "publisher.config.json"), config);
      }
      return root;
   }

   it("enables both when the config does not mention them, or there is no config", () => {
      expect(getServerApiSettings(rootWith('{"environments": []}'))).toEqual(
         BOTH,
      );
      expect(getServerApiSettings(rootWith())).toEqual(BOTH);
   });

   it("reads each flag and whether environments are declared", () => {
      expect(
         getServerApiSettings(
            rootWith(
               JSON.stringify({
                  servingApiEnabled: false,
                  adminApiEnabled: true,
                  environments: [{ name: "sales", packages: [] }],
               }),
            ),
         ),
      ).toEqual({
         servingApiEnabled: false,
         adminApiEnabled: true,
         declaresEnvironments: true,
      });
   });

   it("refuses a flag that is not a boolean, rather than reading it as on", () => {
      expect(() =>
         getServerApiSettings(rootWith('{"adminApiEnabled": "false"}')),
      ).toThrow(
         'Invalid publisher.config.json: "adminApiEnabled" must be true or false, got "false".',
      );
   });

   it("answers the defaults for a config it cannot read, which the main server reports", () => {
      expect(getServerApiSettings(rootWith("{ not json"))).toEqual(BOTH);
   });
});

describe("assertServerApis", () => {
   it("lets both run, the admin API beside the main API", () => {
      expect(() =>
         assertServerApis(BOTH, {
            PUBLISHER_ADMIN_PORT: "4100",
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).not.toThrow();
   });

   it("refuses both off", () => {
      expect(() =>
         assertServerApis(
            { ...BOTH, servingApiEnabled: false, adminApiEnabled: false },
            { PUBLISHER_ADMIN_PORT: "4100" },
         ),
      ).toThrow("nothing to run");
   });

   it("refuses the admin API off with the admin port or the catalog database set", () => {
      const off = { ...BOTH, adminApiEnabled: false };
      expect(() =>
         assertServerApis(off, { PUBLISHER_ADMIN_PORT: "4100" }),
      ).toThrow("adminApiEnabled is false, but PUBLISHER_ADMIN_PORT is set.");
      expect(() =>
         assertServerApis(off, {
            PUBLISHER_ADMIN_PORT: "4100",
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).toThrow(
         "but PUBLISHER_ADMIN_PORT and PUBLISHER_ADMIN_CATALOG_DATABASE are set.",
      );
      expect(() =>
         assertServerApis(off, { PUBLISHER_ADMIN_PORT: "" }),
      ).not.toThrow();
   });

   it("requires the admin port with serving off, and no declared environments", () => {
      const off = { ...BOTH, servingApiEnabled: false };
      expect(() => assertServerApis(off, {})).toThrow(
         "PUBLISHER_ADMIN_PORT is required.",
      );
      expect(() =>
         assertServerApis(
            { ...off, declaresEnvironments: true },
            { PUBLISHER_ADMIN_PORT: "4100" },
         ),
      ).toThrow("but the config declares some.");
      expect(() =>
         assertServerApis(off, { PUBLISHER_ADMIN_PORT: "4100" }),
      ).not.toThrow();
   });
});
