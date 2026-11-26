// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The part of the MCP request context the tools read: the HTTP headers, the client's abort and the JSON-RPC id its answer carries. */
export interface RequestExtra {
   requestId?: string | number;
   requestInfo?: { headers?: Record<string, string | string[] | undefined> };
   signal?: AbortSignal;
}
