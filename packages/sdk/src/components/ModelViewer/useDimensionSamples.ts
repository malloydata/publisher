// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { useQuery } from "@tanstack/react-query";
import { useServer } from "../ServerProvider";
import { httpStatusOf } from "./httpStatus";
import { sampleValuesQuery } from "./malloyRunText";
import { runGate } from "./runGate";
import type { ExplorerField } from "./schemaModel";
import { givensOfRun, type RunContext } from "./useRunQuery";

export const SAMPLE_LIMIT = 12;
/** A dimension's common values move slowly; keep them for the page. */
const SAMPLES_STALE_MS = 5 * 60_000;

/** What a host's sample fetcher is asked for. */
export interface SampleValuesRequest {
   modelPath: string;
   sourceName: string;
   fieldName: string;
   /** At most this many values are shown. */
   limit: number;
   signal?: AbortSignal;
}

/** What a host's sample fetcher can say beyond the values themselves. */
export interface SampleValuesPage {
   /** The most common values, most common first. */
   values: string[];
   /** How many distinct values the dimension has, for the card's caption. */
   total?: number | null;
   /** Why there are none to show ("the index is still building"), said in their place. */
   unavailable?: string;
}

/**
 * A host's way to read a dimension's sample values (an index it maintains, say)
 * instead of the viewer's own query: the values alone, or a
 * {@link SampleValuesPage}. Rejecting with a 4xx that says the field has nothing
 * to sample reads as "no values" (see {@link isExpectedNoSamples}).
 */
export type SampleValuesFetcher = (
   request: SampleValuesRequest,
) => Promise<string[] | SampleValuesPage>;

interface UseDimensionSamplesArgs {
   run: RunContext;
   /** Model path the source belongs to (e.g. "flights.malloy"). */
   modelPath: string;
   /** Source the selected field belongs to. */
   sourceName: string;
   /** The field to sample, or null while the card is collapsed (hook idles). */
   field: ExplorerField | null;
   /** The host's fetcher; the viewer runs {@link sampleValuesQuery} without one. */
   fetchSamples?: SampleValuesFetcher;
   /** A host's build of the values it serves: a new one is a new cache entry,
    *  so a reopened row never shows the old values. */
   generation?: string;
}

/**
 * Whether a rejected sample read is an answer that the dimension simply has
 * nothing to sample, so the card says "no values" rather than "couldn't be
 * loaded": a client error, except the ones that say nothing about the field (a
 * lapsed session 401, a timeout 408, a rate limit 429). Every 5xx, and every
 * rejection with no status (the network), is a read that failed.
 */
export function isExpectedNoSamples(error: unknown): boolean {
   const status = httpStatusOf(error);
   if (status === undefined) return false;
   return (
      status >= 400 &&
      status < 500 &&
      status !== 401 &&
      status !== 408 &&
      status !== 429
   );
}

/** The first column of each row of a result, as text. */
export function firstColumnValues(resultJson: string | undefined): string[] {
   if (!resultJson) return [];
   const { data } = JSON.parse(resultJson) as Pick<Malloy.Result, "data">;
   if (data?.kind !== "array_cell") return [];
   return data.array_value.flatMap((row) => {
      if (row.kind !== "record_cell") return [];
      const cell = row.record_value[0];
      if (!cell) return [];
      switch (cell.kind) {
         case "string_cell":
            return [cell.string_value];
         case "number_cell":
            return [String(cell.number_value)];
         case "boolean_cell":
            return [String(cell.boolean_value)];
         case "date_cell":
            return [cell.date_value];
         case "timestamp_cell":
            return [cell.timestamp_value];
         default:
            return [];
      }
   });
}

/**
 * Lazily fetch sample values for a dimension. Only for dimensions the model tags
 * `#(index)` / `#(index_values)`: the tag is the modeler saying its values are
 * worth listing, and without it opening a row costs nothing. Everything else
 * resolves to "no samples", as does a host's refusal that answers the question. Any
 * other error is reported as `failed`, never as an empty list: the card must
 * not say there are no values when it could not ask.
 *
 * Without a host fetcher this is one capped `group_by` query through the
 * Publisher, with the page's given values, cached per dimension per version and
 * shared by every mount: opening, shutting and reopening a row reads it once.
 */
export function useDimensionSamples({
   run,
   modelPath,
   sourceName,
   field,
   fetchSamples,
   generation,
}: UseDimensionSamplesArgs): {
   samples: string[];
   /** How many distinct values there are, when a host says. */
   total?: number | null;
   /** Why there are none: a host's reason, or the given a run needs first. */
   unavailable?: string;
   loading: boolean;
   failed: boolean;
} {
   const { server, apiClients } = useServer();
   const eligible = field?.kind === "dimension" && field.indexed === true;
   // The viewer's own query gates as a view's run does: one that would be
   // refused for a missing given says so, rather than sending a doomed query.
   const gate =
      eligible && !fetchSamples
         ? runGate(
              givensOfRun(run, sourceName),
              run.requiredFiltersBySource?.get(sourceName),
              run.givenValues,
           )
         : undefined;
   const blocked = gate?.kind === "blocked" ? gate.reason : undefined;
   const fieldName = eligible && !blocked ? field.name : undefined;
   const query = useQuery({
      queryKey: [
         "model-viewer-samples",
         server,
         run.projectName,
         run.packageName,
         run.versionId ?? "",
         modelPath,
         sourceName,
         fieldName,
         fetchSamples
            ? (generation ?? "host")
            : JSON.stringify(run.givenValues ?? {}),
      ],
      enabled: !!fieldName,
      staleTime: SAMPLES_STALE_MS,
      retry: false,
      refetchOnWindowFocus: false,
      queryFn: async ({ signal }): Promise<SampleValuesPage> => {
         if (fetchSamples) {
            try {
               const page = await fetchSamples({
                  modelPath,
                  sourceName,
                  fieldName: fieldName!,
                  limit: SAMPLE_LIMIT,
                  signal,
               });
               const read = Array.isArray(page) ? { values: page } : page;
               return { ...read, values: read.values.slice(0, SAMPLE_LIMIT) };
            } catch (error) {
               // A host's refusal can be its answer that there is nothing to
               // sample. The viewer's own query has no such answer, so a 4xx
               // from it (a query that does not compile) is a failure.
               if (isExpectedNoSamples(error)) return { values: [] };
               throw error;
            }
         }
         const response = await apiClients.models.executeQueryModel(
            run.projectName,
            run.packageName,
            modelPath,
            {
               query: sampleValuesQuery(sourceName, fieldName!, SAMPLE_LIMIT),
               versionId: run.versionId,
               ...(run.givenValues && { givens: { ...run.givenValues } }),
               ...(run.queryMetadata && {
                  queryMetadata: { ...run.queryMetadata },
               }),
            },
            run.includeHiddenFilesAndSources ? true : undefined,
            { signal },
         );
         return { values: firstColumnValues(response.data.result) };
      },
   });
   return {
      samples: query.data?.values ?? [],
      total: query.data?.total,
      unavailable: blocked ?? query.data?.unavailable,
      loading: !!fieldName && query.isPending,
      failed: query.isError,
   };
}
