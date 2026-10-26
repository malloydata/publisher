// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Imported first and importing only node:fs, so the Node version check pulls
// no application code in. ESM still evaluates every dependency's top level
// before the call below; it precedes everything this process chooses to do.
import { assertSupportedNodeVersion } from "./node_version_check";
import { applyCliArgs } from "./cli_args";
import { assertServerRole, resolveServerRole } from "./server_role";

// Exits non-zero on an unsupported Node before argument parsing, storage init
// or any listener; otherwise the failure surfaces only as an unexplained 500
// on the first query. See node_version_check.ts.
assertSupportedNodeVersion();

applyCliArgs(process.argv.slice(2));

// Checked before either server's module loads, so a contradiction fails startup
// before anything binds or opens storage.
const role = resolveServerRole(process.env);
assertServerRole(role, process.env);

if (role === "admin") {
   await import("./admin_server");
} else {
   await import("./main_server");
}
