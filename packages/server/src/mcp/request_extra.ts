// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { traceIdOf } from "../logger";
import { callerHeaderName } from "../service/access_record";

/** The part of the MCP request context the tools read: the HTTP headers and the client's abort. */
export interface RequestExtra {
   requestInfo?: { headers?: Record<string, string | string[] | undefined> };
   signal?: AbortSignal;
}

/** Who the gateway says is asking and the trace the request belongs to, for the access record. */
export function accessIdentityOf(extra: RequestExtra | undefined): {
   caller?: string;
   traceId?: string;
} {
   const headers = extra?.requestInfo?.headers ?? {};
   const raw = headers[callerHeaderName()];
   const caller = (Array.isArray(raw) ? raw[0] : raw) || undefined;
   const traceId = traceIdOf({ headers });
   return {
      ...(caller ? { caller } : {}),
      ...(traceId ? { traceId } : {}),
   };
}
