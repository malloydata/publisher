// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryKey } from "@tanstack/react-query";
import { useServer } from "../components/ServerProvider";
import { CHART_RESULT_QUERY_OPTIONS } from "../utils/queryClient";
import { useQueryWithApiError } from "./useQueryWithApiError";

/**
 * One query against one model, as every result surface asks for it.
 *
 * A dashboard tile, a model page's named query and an embedded `QueryResult`
 * each ran a query through `executeQueryModel` with their own copy of the
 * request, their own cache key and their own loading and error states — three
 * spellings of one thing, and one of them (the model cell) had no error state
 * at all. This is the request and the key; `ResultPanel` is the states.
 */
export interface QueryRequestSpec {
   environmentName: string;
   packageName: string;
   modelPath: string;
   /** The package version the caller's URI named, when it named one. */
   versionId?: string;
   /** A named query on the model. */
   queryName?: string;
   /** A run expression: `run: source -> view`. */
   query?: string;
   sourceName?: string;
   /** The applied control values this query reads, already encoded. */
   givens?: Record<string, unknown>;
}

/**
 * The cache key: every value the request is built from, in a fixed order, so
 * the key cannot drift out of step with the request. A version that reached
 * the request alone would leave two versions of one query on a single cache
 * entry, each able to serve the other's result. `useQueryWithApiError` appends
 * the server, so one key never spans two of them.
 */
export function queryResultKey(spec: QueryRequestSpec): QueryKey {
   return [
      "queryResult",
      spec.environmentName,
      spec.packageName,
      spec.versionId,
      spec.modelPath,
      spec.queryName,
      spec.query,
      spec.sourceName,
      // Re-runs when the applied values change, which is the whole point of a
      // control row.
      JSON.stringify(spec.givens ?? {}),
   ];
}

export function useQueryResult(
   spec: QueryRequestSpec,
   { enabled = true }: { enabled?: boolean } = {},
) {
   const { apiClients } = useServer();
   return useQueryWithApiError({
      queryKey: queryResultKey(spec),
      queryFn: () =>
         apiClients.models.executeQueryModel(
            spec.environmentName,
            spec.packageName,
            spec.modelPath,
            {
               queryName: spec.queryName,
               query: spec.query,
               sourceName: spec.sourceName,
               givens: spec.givens,
               versionId: spec.versionId,
            },
         ),
      enabled,
      ...CHART_RESULT_QUERY_OPTIONS,
   });
}

/** What a result surface holds while a query runs: hand it to `ResultPanel`. */
export type QueryResultState = ReturnType<typeof useQueryResult>;
