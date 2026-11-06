// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type http from "http";

import { DEFAULT_SOCKET_TIMEOUT_MS } from "./constants";

/**
 * Sets the REST and MCP servers' socket limits: `timeout` from
 * `getServerSocketTimeoutMs`, and the keep-alive and header limits at
 * {@link DEFAULT_SOCKET_TIMEOUT_MS}.
 *
 * Node and Bun 1.4 read `timeout` differently. Node destroys a socket that has
 * sent nothing for that long, so a response that keeps writing lives on. Bun
 * 1.4 ends a request that long after it started, however recently it wrote.
 * Either way the clock restarts for each request on a kept-alive socket. A
 * response meant to stay open must opt out with
 * {@link exemptFromSocketTimeout}.
 */
export function applyServerTimeouts(
   server: http.Server,
   socketTimeoutMs: number,
): void {
   server.timeout = socketTimeoutMs;
   server.keepAliveTimeout = DEFAULT_SOCKET_TIMEOUT_MS;
   server.headersTimeout = DEFAULT_SOCKET_TIMEOUT_MS;
}

/**
 * Lifts the socket timeout for one response that stays open by design, such
 * as an event stream. Without it Bun 1.4 ends the stream at `server.timeout`
 * even though it writes a heartbeat.
 */
export function exemptFromSocketTimeout(res: http.ServerResponse): void {
   res.setTimeout(0);
}
