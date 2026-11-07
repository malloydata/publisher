// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as http from "http";
import { logger } from "./logger";
import { resolveBoundPort } from "./mcp_config";

const LONG_REQUEST_MS = 600_000;

/**
 * A query, a package load or a catalog file load can run for minutes, so every
 * listener keeps a request, its headers and an idle keep-alive open that long.
 */
export function longTimeouts<T extends http.Server>(server: T): T {
   server.timeout = LONG_REQUEST_MS;
   server.keepAliveTimeout = LONG_REQUEST_MS;
   server.headersTimeout = LONG_REQUEST_MS;
   return server;
}

/**
 * Log `<label> listening at http://<host>:<port>` with the address really
 * bound, which is how a reader catches a mistyped host falling back to
 * 0.0.0.0, and the port read back, so port 0 reports the ephemeral one.
 * Returns that port.
 */
export function logListening(
   label: string,
   server: Pick<http.Server, "address">,
   requestedHost: string,
   requestedPort: number,
): number {
   const bound = server.address();
   const host =
      typeof bound === "object" && bound ? bound.address : requestedHost;
   const port = resolveBoundPort(bound, requestedPort);
   logger.info(
      `${label} listening at http://${host.includes(":") ? `[${host}]` : host}:${port}`,
   );
   return port;
}
