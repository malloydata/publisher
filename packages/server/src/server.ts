// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Imported first and importing only node:fs, so the Node version check pulls
// no application code in. ESM still evaluates every dependency's top level
// before the call below; it precedes everything this process chooses to do.
import { assertSupportedNodeVersion } from "./node_version_check";
import * as path from "path";
import { applyCliArgs } from "./cli_args";

// Exits non-zero on an unsupported Node before argument parsing, storage init
// or any listener; otherwise the failure surfaces only as an unexplained 500
// on the first query. See node_version_check.ts.
assertSupportedNodeVersion();

applyCliArgs(process.argv.slice(2));

// Checked before either server's module loads, so a contradiction fails startup
// before anything binds or opens storage. Imported here, after the version check,
// because config.ts brings the logger and the rest of the application with it.
const { getServerApiSettings } = await import("./config");
const { assertServerApis } = await import("./server_apis");
const apis = getServerApiSettings(
   path.resolve(process.cwd(), process.env.SERVER_ROOT || "."),
);
assertServerApis(apis, process.env);

if (apis.servingApiEnabled) {
   await import("./main_server");
} else {
   await import("./admin_server");
}
