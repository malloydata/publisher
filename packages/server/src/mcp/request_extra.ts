// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The part of the MCP request context the tools read: the HTTP headers and the client's abort. */
export interface RequestExtra {
   requestInfo?: { headers?: Record<string, string | string[] | undefined> };
   signal?: AbortSignal;
}
