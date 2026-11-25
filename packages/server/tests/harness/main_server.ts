// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type BuiltApp,
   type RunningServers,
   startMainServer,
} from "../../src/main_server";
import { Settings } from "../../src/settings";

let booted: Promise<{ app: BuiltApp; servers: RunningServers }> | undefined;

/**
 * The main server as server.ts starts it (built, listening on the configured
 * ports, SIGTERM handler installed), once per test process: every suite that
 * calls this shares the one server, as they shared the one module that booted
 * it on import before buildApp. Reads the settings installed at the first
 * call, so a suite that installs its own does so before calling.
 */
export async function bootMainServer(): Promise<BuiltApp> {
   booted ??= startMainServer(Settings.current());
   return (await booted).app;
}
