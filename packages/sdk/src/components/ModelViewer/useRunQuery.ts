// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQuery } from "@tanstack/react-query";
import { useServer, type ApiClients } from "../ServerProvider";
import { givensKeyFor, type GivenInfo, type GivenRequest } from "./modelGivens";
import type { SourceAccess } from "./sourceAccess";

/** How one run ended, for a host that counts them. */
export interface QueryRunEvent {
   outcome: "success" | "error";
   durationMs: number;
}

/** What a run needs to address the served model: the same coordinates the
 *  explorer already reads the model's metadata with. */
export interface RunContext {
   projectName: string;
   packageName: string;
   versionId?: string;
   /** The model's declared givens — the declarations runGate reads to name a
    *  default a run fell back to, or to refuse where a blank one has none. The
    *  values a run sends are `givenValues` below. */
   givens?: ReadonlyArray<GivenInfo>;
   /** Source name → the givens a run on that source can use (givensForSource):
    *  what its declaration reads, plus the model-wide ones no source reads. The
    *  page's Givens section shows the same set, so a gate never asks for a given
    *  that section can't set. */
   givensBySource?: ReadonlyMap<string, GivenInfo[]>;
   /** Source name → labels of its `#(filter … required)` filters; a run can't
    *  supply them, so it refuses rather than 400. */
   requiredFiltersBySource?: ReadonlyMap<string, string[]>;
   /** Given values the reader set in the page's Givens section, in the request
    *  form a dashboard sends (`givensRequest`: typed, unset ones left out), sent
    *  with every run and part of its cache key (givensKeyFor). */
   givenValues?: GivenRequest;
   /** Run a file the package's index.malloy hides, for someone who can edit the
    *  package (offSurface.tsx). Anyone else is refused. */
   includeHiddenFilesAndSources?: boolean;
   /** Source name → its access declarations, so a refused run can say which
    *  rule refused it rather than a bare "no access". */
   accessBySource?: ReadonlyMap<string, SourceAccess>;
   /** Labels every run carries (`QueryRequest.queryMetadata`), for a host that
    *  tags its traffic by the surface it came from. */
   queryMetadata?: Readonly<Record<string, string>>;
   /** Called once per run that settles; a run the reader left is not reported. */
   onQueryRun?: (event: QueryRunEvent) => void;
   /** A result larger than this (bytes) is held behind a warning before it renders. */
   maxResultSize?: number;
}

/** The givens a run on `sourceName` reads — the model's own for a named query. */
export function givensOfRun(
   run: Pick<RunContext, "givens" | "givensBySource">,
   sourceName: string | undefined,
): ReadonlyArray<GivenInfo> | undefined {
   return (
      (sourceName ? run.givensBySource?.get(sourceName) : undefined) ??
      run.givens
   );
}

/**
 * Run one Malloy query against a published model and hand back the publisher's
 * `Result` JSON for `RenderedResult`: `executeQueryModel` with the query text,
 * keyed by everything that decides its answer.
 *
 * Idle until `enabled`: browsing a source must cost nothing; a run happens on a
 * click. And a result, once fetched, is final for this query text and version:
 * it never goes stale and never refetches on focus or remount. Re-running is
 * the card's own explicit control (`refetch`), because a warehouse query is
 * billed.
 */
export function useRunQuery({
   run,
   modelPath,
   query,
   enabled,
}: {
   run: RunContext;
   modelPath: string;
   query: string;
   enabled: boolean;
}) {
   const { server, apiClients } = useServer();
   const result = useQuery({
      ...runQueryOptions({ server, apiClients, run, modelPath, query }),
      enabled,
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
   });
   return {
      result: result.data?.data.result,
      isSuccess: result.isSuccess,
      isLoading: result.isLoading,
      isFetching: result.isFetching,
      isError: result.isError,
      error: result.error,
      refetch: result.refetch,
   };
}

/**
 * The viewer's cache key for one run (a view's, a named query's), for a host
 * that refreshes it or runs the same query itself (a download, say).
 */
export function runQueryKey(
   server: string,
   run: Pick<
      RunContext,
      | "projectName"
      | "packageName"
      | "versionId"
      | "givenValues"
      | "includeHiddenFilesAndSources"
   >,
   modelPath: string,
   query: string,
): readonly unknown[] {
   return [
      "model-viewer-run",
      server,
      run.projectName,
      run.packageName,
      modelPath,
      run.versionId ?? "",
      query,
      // The same serialization a notebook cell uses for its key (givensKeyFor).
      givensKeyFor(run.givenValues),
      // A hidden file's run is its own entry: the curated run's 404 must
      // never be served in its place.
      ...(run.includeHiddenFilesAndSources ? ["off-surface"] : []),
   ];
}

/**
 * A viewer run's cache key, request and caching, apart from the hook, so a host
 * that runs the same query (a download, say) reuses a result the reader already
 * ran, and one it runs is cached for the reader to open without paying again.
 *
 * A run is kept until the page goes (`staleTime: Infinity`): its inputs are all in
 * the key. A failed run is reported in place with its own Retry, so it never
 * retries silently — that would re-bill a query the reader can see failed.
 */
export function runQueryOptions({
   server,
   apiClients,
   run,
   modelPath,
   query,
}: {
   server: string;
   apiClients: ApiClients;
   run: RunContext;
   modelPath: string;
   query: string;
}) {
   const givens =
      givensKeyFor(run.givenValues) === "" ? undefined : run.givenValues;
   return {
      queryKey: runQueryKey(server, run, modelPath, query),
      queryFn: async ({ signal }: { signal?: AbortSignal }) => {
         const started = performance.now();
         const durationMs = () => Math.round(performance.now() - started);
         try {
            const result = await apiClients.models.executeQueryModel(
               run.projectName,
               run.packageName,
               modelPath,
               {
                  query,
                  versionId: run.versionId,
                  ...(givens && { givens }),
                  ...(run.queryMetadata && {
                     queryMetadata: { ...run.queryMetadata },
                  }),
               },
               run.includeHiddenFilesAndSources ? true : undefined,
               { signal },
            );
            run.onQueryRun?.({ outcome: "success", durationMs: durationMs() });
            return result;
         } catch (error) {
            // A run the reader left (or replaced) is not a failed one.
            if (!signal?.aborted)
               run.onQueryRun?.({ outcome: "error", durationMs: durationMs() });
            throw error;
         }
      },
      staleTime: Infinity,
      retry: false,
   } as const;
}
