// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What this process runs, from PUBLISHER_ROLE (`--role`). The admin API's
 * catalog credential reaches every catalog in its catalog database, so a
 * deployment that serves other people's environments states that it never
 * holds it, and a deployment that holds it states that it serves nothing; the
 * checks below refuse to start a process whose configuration says otherwise.
 *
 * - `all`, the default: the main API and MCP, and the admin API when its port
 *   is set. One operator owns the server and every catalog, so one process may
 *   hold both.
 * - `serve`: the main API and MCP, never the admin API.
 * - `admin`: only the admin API, with no environments loaded.
 */
export type ServerRole = "all" | "serve" | "admin";

const ROLES: readonly ServerRole[] = ["all", "serve", "admin"];

/** The role, `all` when unset. A value that is not a role throws, so a typo fails startup. */
export function resolveServerRole(env: NodeJS.ProcessEnv): ServerRole {
   const raw = env.PUBLISHER_ROLE?.trim();
   if (!raw) return "all";
   if (!(ROLES as readonly string[]).includes(raw)) {
      throw new Error(
         `PUBLISHER_ROLE must be one of ${ROLES.join(", ")}, got '${raw}'.`,
      );
   }
   return raw as ServerRole;
}

/** Throws when the configuration contradicts the role. */
export function assertServerRole(
   role: ServerRole,
   env: NodeJS.ProcessEnv,
): void {
   const adminSettings = [
      "PUBLISHER_ADMIN_PORT",
      "PUBLISHER_ADMIN_CATALOG_DATABASE",
   ].filter((name) => env[name]?.trim());
   if (role === "serve" && adminSettings.length > 0) {
      throw new Error(
         `PUBLISHER_ROLE=serve never runs the admin API, but ${adminSettings.join(" and ")} ${adminSettings.length > 1 ? "are" : "is"} set.`,
      );
   }
   if (role === "admin" && !env.PUBLISHER_ADMIN_PORT?.trim()) {
      throw new Error(
         "PUBLISHER_ROLE=admin runs only the admin API, so PUBLISHER_ADMIN_PORT is required.",
      );
   }
}
