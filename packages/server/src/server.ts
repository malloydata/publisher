// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Imported first and importing only node:fs, so the Node version check pulls
// no application code in. ESM still evaluates every dependency's top level
// before the call below; it precedes everything this process chooses to do.
import { assertSupportedNodeVersion } from "./node_version_check";
import { printHelpIfAsked } from "./cli_args";
import { Settings } from "./settings";

// Exits non-zero on an unsupported Node before argument parsing, storage init
// or any listener; otherwise the failure surfaces only as an unexplained 500
// on the first query. See node_version_check.ts.
assertSupportedNodeVersion();

const argv = process.argv.slice(2);
printHelpIfAsked(argv);

// Every setting, read once from the flags, the variables and the config file,
// before any module that reads one is loaded: main_server.ts and
// admin_server.ts read theirs when they load.
Settings.install(Settings.load({ argv }));

// Checked before either server's module loads, so a contradiction fails startup
// before anything binds or opens storage. Imported here, after the version check,
// because config.ts brings the logger and the rest of the application with it.
const { getServerApiSettings } = await import("./config");
const { assertServerApis } = await import("./server_apis");
const { logger } = await import("./logger");
for (const warning of Settings.current().warnings) {
   logger.warn(warning.message);
}
const apis = getServerApiSettings();
assertServerApis(apis);

if (apis.servingApiEnabled) {
   await import("./main_server");
} else {
   await import("./admin_server");
}
