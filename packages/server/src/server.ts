// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Imported first and importing only node:fs, so the Node version check pulls
// no application code in. ESM still evaluates every dependency's top level
// before the call below; it precedes everything this process chooses to do.
import { assertSupportedNodeVersion } from "./node_version_check";
import { printHelpIfAsked } from "./cli_args";
import { Settings, SettingsError } from "./settings";

// Exits non-zero on an unsupported Node before argument parsing, storage init
// or any listener; otherwise the failure surfaces only as an unexplained 500
// on the first query. See node_version_check.ts.
assertSupportedNodeVersion();

const argv = process.argv.slice(2);
printHelpIfAsked(argv);

// Every setting, read once from the flags, the variables and the config file,
// before any module that reads one is loaded: main_server.ts and
// admin_server.ts read theirs when they load.
const loaded = Settings.load({ argv });
Settings.install(loaded);

// A value that does not parse fails startup, every one of them at once, and
// before anything binds. A config file that cannot be read fails it alone,
// since none of its settings can be trusted to have their defaults. Checked
// before the logger loads, because the logger reads two settings when it
// loads, and one of them may be the bad value.
const failures = loaded.fileError ? [loaded.fileError] : loaded.errors();
if (failures.length > 0) {
   for (const warning of loaded.warnings) {
      process.stderr.write(`warn: ${warning.message}\n`);
   }
   const message = new SettingsError(failures).message;
   process.stderr.write(
      `${message}\nPUBLISHER_INIT_FAILED error=${JSON.stringify(message)}\n`,
   );
   process.exit(1);
}

const { logger } = await import("./logger");
for (const warning of loaded.warnings) {
   logger.warn(warning.message);
}

// One line naming each setting given and where it came from; secrets show as (set).
const { settingsLine } = await import("./settings/reference");
logger.info(settingsLine(loaded));

// Checked before either server's module loads, so a contradiction fails startup
// before anything binds or opens storage. Imported here, after the version check,
// because config.ts brings the logger and the rest of the application with it.
const { getServerApiSettings } = await import("./config");
const { assertServerApis } = await import("./server_apis");
const apis = getServerApiSettings();
assertServerApis(apis);

if (apis.servingApiEnabled) {
   await import("./main_server");
} else {
   await import("./admin_server");
}

// Counted once the server's telemetry is up, which loading it starts.
const { recordDeprecatedUsage } = await import("./deprecated_usage_metrics");
for (const name of loaded.deprecatedNames) {
   recordDeprecatedUsage(`config:${name}`);
}
