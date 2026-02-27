/**
 * Per-request context using AsyncLocalStorage.
 *
 * Carries request-scoped data (e.g., OAuth tokens) through the call stack
 * without threading parameters through every function. Used by the MCP
 * endpoint to propagate database tokens for per-user connection creation.
 */

import { AsyncLocalStorage } from "node:async_hooks";

export interface RequestContext {
   /** OAuth token for per-request database authentication. */
   databaseToken?: string;
}

export const requestContext = new AsyncLocalStorage<RequestContext>();

/** Returns the database token from the current request context, if any. */
export function getDatabaseToken(): string | undefined {
   return requestContext.getStore()?.databaseToken;
}
