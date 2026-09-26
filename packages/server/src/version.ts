// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import packageJson from "../package.json";

/**
 * This server's release version, inlined from package.json at build time.
 *
 * The release workflow stamps package.json before building, so the published
 * artifact carries the version it shipped as. Reported on /status, get_status
 * and the MCP initialize handshake, so a trace or a bug report can name the
 * build it ran on. Without it, a stale `npx` cache is invisible.
 */
export const SERVER_VERSION: string = packageJson.version;
