// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The lookup query behind value search: "which string does the data store for
 * what the user typed?" ("Auto Intenders" -> `Auto Intenders (In-Market)` on
 * `segment_name`). It reads a dimension's distinct values with a bounded
 * `group_by` through the NORMAL query path (`Model.getQueryResults`, the call
 * `execute_query` makes). The engine's value index (../context/values_index.ts)
 * and its response step (../context/respond.ts) both read through it.
 *
 * Why that shape: a value is data, and data has a gate. Going through the
 * query path means the gate is enforced by the one code that already enforces
 * it. An `#(authorize)` lock the caller's givens do not pass answers
 * AccessDeniedError and the source contributes nothing. An `#(access_filter)`
 * is grafted as a `where:`, so values come only from rows the caller can see.
 * No gate logic is repeated here.
 */

import type { GivenValue } from "@malloydata/malloy";
import { quoteMalloyIdentifier } from "../../service/authorize";
import type { EnvironmentStore } from "../../service/environment_store";
import { tryAcquireQuerySlot } from "../../query_concurrency";
import { runWithQueryTimeout } from "../../query_timeout";
import { getModelForQuery } from "../handler_utils";

/** What one lookup query returns. */
export interface DistinctValues {
   values: string[];
   /** More distinct values exist than were read. */
   truncated: boolean;
}

/** One lookup query. A seam so a test can stand in for the warehouse. */
export interface ValueFetchRequest {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   modelPath: string;
   source: string;
   dimension: string;
   /** Distinct values to read; one more is asked for to learn whether there are more. */
   limit: number;
   /** When set, only values containing this text (case-insensitive) are read. */
   stem?: string;
   givens?: Record<string, unknown>;
   filterParams?: Record<string, string | string[]>;
   timeoutMs: number;
}

export type ValueFetcher = (
   request: ValueFetchRequest,
) => Promise<DistinctValues>;

/** Values longer than this are free text, not a category worth matching. */
const MAX_VALUE_CHARS = 200;

/**
 * The lookup query: read up to `limit + 1` distinct non-null values of one
 * dimension, through the normal query path with the caller's givens.
 */
export const defaultValueFetcher: ValueFetcher = async (request) => {
   const found = await getModelForQuery(
      request.environmentStore,
      request.environmentName,
      request.packageName,
      request.modelPath,
   );
   if ("error" in found) throw new Error(found.error.message);
   const dim = quoteMalloyIdentifier(request.dimension);
   // The stem is letters and digits only (see filterStem), so it needs no escaping.
   const where = request.stem
      ? `${dim} != null and lower(${dim}) ~ '%${request.stem}%'`
      : `${dim} != null`;
   const query = `run: ${quoteMalloyIdentifier(request.source)} -> { where: ${where} group_by: v is ${dim} limit: ${request.limit + 1} }`;
   const slot = tryAcquireQuerySlot("mcp:getContext:values");
   try {
      const { compactResult } = await runWithQueryTimeout(
         (signal) =>
            found.model.getQueryResults(
               undefined,
               undefined,
               query,
               request.filterParams,
               undefined,
               request.givens as Record<string, GivenValue> | undefined,
               signal,
               undefined,
               "compact",
               // MCP sends no #(authorize) bypass, ever; nor does value search.
               false,
               false,
            ),
         request.timeoutMs,
      );
      const rows = compactResult as unknown as Array<Record<string, unknown>>;
      const values: string[] = [];
      for (const row of rows) {
         const raw = row.v;
         if (raw === null || raw === undefined) continue;
         const text = String(raw);
         if (text.trim() === "" || text.length > MAX_VALUE_CHARS) continue;
         values.push(text);
      }
      return {
         values: values.slice(0, request.limit),
         truncated: rows.length > request.limit,
      };
   } finally {
      slot.release();
   }
};
