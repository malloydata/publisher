// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A named import, so the bundle inlines the one string rather than the whole
// package.json.
import { version } from "../package.json";

/**
 * This server's release version, inlined at build time. Reported on /status,
 * get_status and the MCP handshake so a bug report can name the build, and a
 * stale `npx` cache shows.
 */
export const SERVER_VERSION: string = version;
