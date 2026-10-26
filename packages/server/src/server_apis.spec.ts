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

/** Runs `fn` with `vars` set, then puts the environment back. */
function withEnv(vars: Record<string, string>, fn: () => void): void {
   const before = Object.fromEntries(
      Object.keys(vars).map((k) => [k, process.env[k]]),
   );
   Object.assign(process.env, vars);
   try {
      fn();
   } finally {
      for (const [k, v] of Object.entries(before)) {
         if (v === undefined) delete process.env[k];
         else process.env[k] = v;
      }
   }
}

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

   it("lets the variable override the file, read as any variable boolean is", () => {
      const root = rootWith(
         JSON.stringify({ servingApiEnabled: true, adminApiEnabled: false }),
      );
      withEnv(
         {
            PUBLISHER_SERVING_API_ENABLED: "off",
            PUBLISHER_ADMIN_API_ENABLED: "1",
         },
         () =>
            expect(getServerApiSettings(root)).toEqual({
               servingApiEnabled: false,
               adminApiEnabled: true,
               declaresEnvironments: false,
            }),
      );
      withEnv({ PUBLISHER_ADMIN_API_ENABLED: "maybe" }, () =>
         expect(() => getServerApiSettings(root)).toThrow(
            'Invalid value for PUBLISHER_ADMIN_API_ENABLED: expected a boolean (true/false), got "maybe"',
         ),
      );
   });

   it("reads a ${VAR} in the file as a boolean, and refuses one that is not", () => {
      const root = rootWith('{"adminApiEnabled": "${TEST_ADMIN_API}"}');
      withEnv({ TEST_ADMIN_API: "false" }, () =>
         expect(getServerApiSettings(root).adminApiEnabled).toBe(false),
      );
      withEnv({ TEST_ADMIN_API: "nope" }, () =>
         expect(() => getServerApiSettings(root)).toThrow(
            'expected a boolean (true/false), got "nope"',
         ),
      );
   });

   it("applies the variable when the config cannot be read", () => {
      withEnv({ PUBLISHER_ADMIN_API_ENABLED: "false" }, () =>
         expect(
            getServerApiSettings(rootWith("{ not json")).adminApiEnabled,
         ).toBe(false),
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
