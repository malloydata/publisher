// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { join } from "path";
import { getServerApiSettings, type ServerApiSettings } from "./config";
import { assertServerApis } from "./server_apis";
import { withSettings } from "./test_helpers/settings_env";

const BOTH: ServerApiSettings = {
   servingApiEnabled: true,
   adminApiEnabled: true,
   declaresEnvironments: false,
};

/**
 * The API settings of a server started on a root holding `config` (no config
 * file when undefined), with the variables `env`.
 */
function apisFrom(
   config?: string,
   env: Record<string, string> = {},
): ServerApiSettings {
   return withSettings({ file: config, env }, () => getServerApiSettings());
}

/**
 * Expect reading those settings to throw `message`, given the config file's
 * path, since a file value's error names the file it came from.
 */
function expectRefusal(
   config: string,
   env: Record<string, string>,
   message: (configPath: string) => string,
): void {
   withSettings({ file: config, env }, ({ serverRoot }) =>
      expect(() => getServerApiSettings()).toThrow(
         message(join(serverRoot, "publisher.config.json")),
      ),
   );
}

describe("getServerApiSettings", () => {
   it("enables both when the config does not mention them, or there is no config", () => {
      expect(apisFrom('{"environments": []}')).toEqual(BOTH);
      expect(apisFrom()).toEqual(BOTH);
   });

   it("reads each flag and whether environments are declared", () => {
      expect(
         apisFrom(
            JSON.stringify({
               servingApiEnabled: false,
               adminApiEnabled: true,
               environments: [{ name: "sales", packages: [] }],
            }),
         ),
      ).toEqual({
         servingApiEnabled: false,
         adminApiEnabled: true,
         declaresEnvironments: true,
      });
   });

   it("refuses a flag that is not a boolean, rather than reading it as on", () => {
      expectRefusal(
         '{"adminApiEnabled": "false"}',
         {},
         (file) =>
            `adminApiEnabled from ${file}: expected a boolean (true/false, 1/0, yes/no or on/off), got "false"`,
      );
   });

   it("lets the variable override the file, read as any variable boolean is", () => {
      const config = JSON.stringify({
         servingApiEnabled: true,
         adminApiEnabled: false,
      });
      expect(
         apisFrom(config, {
            PUBLISHER_SERVING_API_ENABLED: "off",
            PUBLISHER_ADMIN_API_ENABLED: "1",
         }),
      ).toEqual({
         servingApiEnabled: false,
         adminApiEnabled: true,
         declaresEnvironments: false,
      });
      expectRefusal(
         config,
         { PUBLISHER_ADMIN_API_ENABLED: "maybe" },
         () =>
            'adminApiEnabled from PUBLISHER_ADMIN_API_ENABLED: expected a boolean (true/false, 1/0, yes/no or on/off), got "maybe"',
      );
   });

   it("reads a ${VAR} in the file as a boolean, and refuses one that is not", () => {
      const config = '{"adminApiEnabled": "${TEST_ADMIN_API}"}';
      expect(
         apisFrom(config, { TEST_ADMIN_API: "false" }).adminApiEnabled,
      ).toBe(false);
      expectRefusal(
         config,
         { TEST_ADMIN_API: "nope" },
         (file) =>
            `adminApiEnabled from ${file}: expected a boolean (true/false, 1/0, yes/no or on/off), got "\${TEST_ADMIN_API}"`,
      );
   });

   it("applies the variable when the config cannot be read", () => {
      expect(
         apisFrom("{ not json", { PUBLISHER_ADMIN_API_ENABLED: "false" })
            .adminApiEnabled,
      ).toBe(false);
   });

   it("answers the defaults for a config it cannot read, which the main server reports", () => {
      expect(apisFrom("{ not json")).toEqual(BOTH);
   });
});

/** Whether the APIs `apis` names may run with the variables `env` sets. */
function assertWith(apis: ServerApiSettings, env: Record<string, string>) {
   withSettings({ env }, () => assertServerApis(apis));
}

describe("assertServerApis", () => {
   it("lets both run, the admin API beside the main API", () => {
      expect(() =>
         assertWith(BOTH, {
            PUBLISHER_ADMIN_PORT: "4100",
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).not.toThrow();
   });

   it("refuses both off", () => {
      expect(() =>
         assertWith(
            { ...BOTH, servingApiEnabled: false, adminApiEnabled: false },
            { PUBLISHER_ADMIN_PORT: "4100" },
         ),
      ).toThrow("nothing to run");
   });

   it("refuses the admin API off with the admin port or the catalog database set", () => {
      const off = { ...BOTH, adminApiEnabled: false };
      expect(() => assertWith(off, { PUBLISHER_ADMIN_PORT: "4100" })).toThrow(
         "adminApiEnabled is false, but PUBLISHER_ADMIN_PORT is set.",
      );
      expect(() =>
         assertWith(off, {
            PUBLISHER_ADMIN_PORT: "4100",
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).toThrow(
         "but PUBLISHER_ADMIN_PORT and PUBLISHER_ADMIN_CATALOG_DATABASE are set.",
      );
      expect(() => assertWith(off, { PUBLISHER_ADMIN_PORT: "" })).not.toThrow();
   });

   it("requires the admin port with serving off, and no declared environments", () => {
      const off = { ...BOTH, servingApiEnabled: false };
      expect(() => assertWith(off, {})).toThrow(
         "PUBLISHER_ADMIN_PORT is required.",
      );
      expect(() =>
         assertWith(
            { ...off, declaresEnvironments: true },
            { PUBLISHER_ADMIN_PORT: "4100" },
         ),
      ).toThrow("but the config declares some.");
      expect(() =>
         assertWith(off, { PUBLISHER_ADMIN_PORT: "4100" }),
      ).not.toThrow();
   });
});
