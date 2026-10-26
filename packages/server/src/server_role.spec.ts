// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { assertServerRole, resolveServerRole } from "./server_role";

describe("resolveServerRole", () => {
   it("is all when unset or blank", () => {
      expect(resolveServerRole({})).toBe("all");
      expect(resolveServerRole({ PUBLISHER_ROLE: "  " })).toBe("all");
   });

   it("reads each role", () => {
      for (const role of ["all", "serve", "admin"] as const) {
         expect(resolveServerRole({ PUBLISHER_ROLE: role })).toBe(role);
      }
   });

   it("refuses a value that is not a role, rather than falling back to all", () => {
      expect(() => resolveServerRole({ PUBLISHER_ROLE: "server" })).toThrow(
         "PUBLISHER_ROLE must be one of all, serve, admin, got 'server'.",
      );
   });
});

describe("assertServerRole", () => {
   it("lets all run the admin API beside the main API", () => {
      expect(() =>
         assertServerRole("all", {
            PUBLISHER_ADMIN_PORT: "4100",
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).not.toThrow();
   });

   it("refuses serve with the admin port or the catalog database", () => {
      expect(() =>
         assertServerRole("serve", { PUBLISHER_ADMIN_PORT: "4100" }),
      ).toThrow(
         "PUBLISHER_ROLE=serve never runs the admin API, but PUBLISHER_ADMIN_PORT is set.",
      );
      expect(() =>
         assertServerRole("serve", {
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).toThrow("but PUBLISHER_ADMIN_CATALOG_DATABASE is set.");
      expect(() =>
         assertServerRole("serve", {
            PUBLISHER_ADMIN_PORT: "4100",
            PUBLISHER_ADMIN_CATALOG_DATABASE: "postgres://h/ducklake",
         }),
      ).toThrow(
         "but PUBLISHER_ADMIN_PORT and PUBLISHER_ADMIN_CATALOG_DATABASE are set.",
      );
   });

   it("lets serve run with the admin settings empty", () => {
      expect(() =>
         assertServerRole("serve", { PUBLISHER_ADMIN_PORT: "" }),
      ).not.toThrow();
   });

   it("requires admin to have the admin port", () => {
      expect(() => assertServerRole("admin", {})).toThrow(
         "PUBLISHER_ROLE=admin runs only the admin API, so PUBLISHER_ADMIN_PORT is required.",
      );
      expect(() =>
         assertServerRole("admin", { PUBLISHER_ADMIN_PORT: "4100" }),
      ).not.toThrow();
   });
});
