// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ServerApiSettings } from "./config";
import { settings } from "./settings";

/**
 * Throws when the settings contradict which APIs the config enables
 * (`servingApiEnabled`, `adminApiEnabled`; config.ts). The admin API's catalog
 * credential reaches every catalog in its catalog database, so a deployment that
 * serves other people's environments turns the admin API off, and one that holds
 * the credential turns serving off; a setting that says otherwise refuses to start
 * rather than running what was turned off.
 */
export function assertServerApis(apis: ServerApiSettings): void {
   const { servingApiEnabled, adminApiEnabled, declaresEnvironments } = apis;
   if (!servingApiEnabled && !adminApiEnabled) {
      throw new Error(
         "servingApiEnabled and adminApiEnabled are both false, so there is nothing to run.",
      );
   }
   if (!adminApiEnabled) {
      const adminSettings = [
         settings.isSet("admin.port") ? "PUBLISHER_ADMIN_PORT" : undefined,
         settings.isSet("admin.catalogDatabase")
            ? "PUBLISHER_ADMIN_CATALOG_DATABASE"
            : undefined,
      ].filter((name) => name !== undefined);
      if (adminSettings.length > 0) {
         throw new Error(
            `adminApiEnabled is false, but ${adminSettings.join(" and ")} ${adminSettings.length > 1 ? "are" : "is"} set.`,
         );
      }
   }
   if (!servingApiEnabled) {
      if (!settings.isSet("admin.port")) {
         throw new Error(
            "servingApiEnabled is false, so only the admin API runs, and PUBLISHER_ADMIN_PORT is required.",
         );
      }
      if (declaresEnvironments) {
         throw new Error(
            "servingApiEnabled is false, so no environments are loaded, but the config declares some.",
         );
      }
   }
}
