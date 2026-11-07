// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Route and tool specs stub a model's `runQuery` and assert on what it was
 * called with. They were written against the positional
 * `Model.getQueryResults`, so this spreads a `runQuery(audit, request,
 * privileges)` call back into that argument order: audit, sourceName,
 * queryName, query, filterParams, bypassFilters, givens, abortSignal,
 * queryMetadataInput, responseShape, bypassAuthorize,
 * includeHiddenFilesAndSources.
 */

import {
   QueryPrivileges,
   type QueryRequest,
} from "../service/model_query_request";

export function positionalQueryArgs(
   audit: unknown,
   request: QueryRequest,
   privileges: QueryPrivileges = QueryPrivileges.none,
): unknown[] {
   return [
      audit,
      request.sourceName,
      request.queryName,
      request.query,
      request.filterParams,
      privileges.bypassFilters,
      request.givens,
      request.abortSignal,
      request.queryMetadataInput,
      request.responseShape,
      privileges.bypassAuthorize,
      privileges.includeHiddenFilesAndSources,
   ];
}

/** A `runQuery` stub that hands `impl` the positional arguments. */
export function runQueryPositional<T>(
   impl: (...args: unknown[]) => T,
): (audit: unknown, request: QueryRequest, privileges?: QueryPrivileges) => T {
   return (audit, request, privileges) =>
      impl(...positionalQueryArgs(audit, request, privileges));
}
