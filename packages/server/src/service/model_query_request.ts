// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a caller asks {@link Model.runQuery} for, split by trust:
 * {@link QueryRequest} is what any caller may supply, and
 * {@link QueryPrivileges} holds every flag that relaxes a governance check.
 * The two never mix, so a request field cannot widen what a query may read;
 * only an adapter that has decided the caller holds a privilege grants it.
 */

import type { GivenValue } from "@malloydata/malloy";
import type { FilterParams } from "./filter";
import type { ModelQueryMetadataInput } from "./query_metadata";

/** One query against one model: a named view on a source, a named query, or ad-hoc Malloy text. */
export interface QueryRequest {
   sourceName?: string;
   queryName?: string;
   query?: string;
   /** Values for the model's deprecated `#(filter)` annotations. */
   filterParams?: FilterParams;
   givens?: Record<string, GivenValue>;
   /**
    * Passed to `runnable.run` so a query timeout (`runWithQueryTimeout`)
    * cancels the work in flight. Absent means no timeout.
    */
   abortSignal?: AbortSignal;
   /**
    * Per-query metadata inputs from the request boundary (see
    * {@link ModelQueryMetadataInput}). Omitting it still tags the query with
    * the server context; what is lost is the caller's own properties, the
    * connection default, and the correlation id.
    */
   queryMetadataInput?: ModelQueryMetadataInput;
   /**
    * Which shape the caller will send, so exactly that one is built and
    * measured: `"compact"` (the `compactJson` rows) or `"full"` (the wrapped
    * result, the default). Every production caller passes it.
    */
   responseShape?: "full" | "compact";
}

/**
 * The governance relaxations a caller holds for one query. Each is false
 * unless an adapter that authenticated the reason sets it; {@link
 * QueryPrivileges.none} is the default everywhere.
 */
export interface QueryPrivileges {
   /**
    * Skip the deprecated `#(filter)` requirements (required filters, filter
    * values). Touches neither `#(authorize)` nor `#(access_filter)`.
    */
   readonly bypassFilters: boolean;
   /**
    * Skip `#(authorize)` gate evaluation — the private data-management path
    * (the router's `dataManagementQuery`). Disables ONLY authorize gates;
    * `where:`, `#(filter)` and {@link bypassFilters} are untouched. Never
    * settable from the ingress-exposed surface: `/private/**` is not routed
    * externally and the router pins an M2M identity before forwarding.
    */
   readonly bypassAuthorize: boolean;
   /**
    * Lift the package's surface for this request (like `queryableSources:
    * "all"`), for package editors; the gateway decides who that is. Gates
    * still apply: this lifts curation, never the lock.
    */
   readonly includeHiddenFilesAndSources: boolean;
}

const NO_PRIVILEGES: QueryPrivileges = Object.freeze({
   bypassFilters: false,
   bypassAuthorize: false,
   includeHiddenFilesAndSources: false,
});

export const QueryPrivileges = {
   /** No relaxation: every check applies. */
   none: NO_PRIVILEGES,
   /**
    * The privileges named `true`; anything else (absent, `false`, `undefined`)
    * is not granted.
    */
   of(granted: {
      bypassFilters?: boolean;
      bypassAuthorize?: boolean;
      includeHiddenFilesAndSources?: boolean;
   }): QueryPrivileges {
      return Object.freeze({
         bypassFilters: granted.bypassFilters === true,
         bypassAuthorize: granted.bypassAuthorize === true,
         includeHiddenFilesAndSources:
            granted.includeHiddenFilesAndSources === true,
      });
   },
};
