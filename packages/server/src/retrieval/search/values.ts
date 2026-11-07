// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The lookup query behind value search: "which string does the data store for
 * what the user typed?" ("Home Gardeners" -> `Home Gardeners (In-Season)` on
 * `segment_name`). It reads a dimension's distinct values with a bounded
 * `group_by` through the NORMAL query path (`Model.runQuery`, the call
 * `execute_query` makes). The engine's value index (../index/values_index.ts)
 * and its response step (./respond.ts) both read through it.
 *
 * Why that shape: a value is data, and data has a gate. Going through the
 * query path means the gate is enforced by the one code that already enforces
 * it. An `#(authorize)` lock the caller's givens do not pass answers
 * AccessDeniedError and the source contributes nothing. An `#(access_filter)`
 * is grafted as a `where:`, so values come only from rows the caller can see.
 * No gate logic is repeated here.
 */

import type { GivenValue } from "@malloydata/malloy";
import {
   type AccessIdentity,
   type QueryAudit,
   runUnrecorded,
   withAccessRecord,
} from "../../service/access_record";
import { quoteMalloyIdentifier } from "../../service/authorize";
import type { EnvironmentStore } from "../../service/environment_store";
import { QueryPrivileges } from "../../service/model_query_request";
import { tryAcquireQuerySlot } from "../../query_concurrency";
import { runWithQueryTimeout } from "../../query_timeout";
import { ModelNotFoundError } from "../../errors";
import type { Environment } from "../../service/environment";
import type { Model } from "../../service/model";
import type { Package } from "../../service/package";
import { resolveQueryModel } from "../../service/run_model_query";
import type { SearchErrorDetails } from "../model/types";

/**
 * The model a value lookup queries, or why there is none: `error` is the
 * account the caller reads, `cause` what the access record classifies.
 */
export type QueryModelResolution =
   | { model: Model; environment: Environment; pkg: Package }
   | { error: SearchErrorDetails; cause: unknown };

/** Resolves the model a value lookup queries (the package's `latest`). */
export type QueryModelResolver = (
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
   modelPath: string,
) => Promise<QueryModelResolution>;

/**
 * The resolver with no adapter installed: a failure is its own error, unworded.
 * The server installs the MCP tools' resolver (setQueryModelResolver), whose
 * refusal is worded the way execute_query words it, because a value lookup's
 * refusal reaches the agent in a get_context warning.
 */
const plainQueryModelResolver: QueryModelResolver = async (
   environmentStore,
   environmentName,
   packageName,
   modelPath,
) => {
   const { environment, pkg, model } = await resolveQueryModel(
      environmentStore,
      environmentName,
      packageName,
      modelPath,
   );
   if (!model || model.getType() === "notebook") {
      throw new ModelNotFoundError(
         `model '${modelPath}' in package '${packageName}' for environment '${environmentName}' not found`,
      );
   }
   await model.getModel();
   return { model, environment, pkg };
};

let queryModelResolver: QueryModelResolver = plainQueryModelResolver;

/**
 * Install the resolver value lookups use (the composition root passes the MCP
 * tools' getModelForQuery); `undefined` restores the plain one.
 */
export function setQueryModelResolver(
   resolver: QueryModelResolver | undefined,
): void {
   queryModelResolver = resolver ?? plainQueryModelResolver;
}

/** What one lookup query returns. */
interface DistinctValues {
   values: string[];
   /** More distinct values exist than were read. */
   truncated: boolean;
}

/** One lookup query. A seam so a test can stand in for the warehouse. */
interface ValueFetchRequest {
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
   /** Who asked, from the request headers, for the access record. */
   identity?: AccessIdentity;
}

export type ValueFetcher = (
   request: ValueFetchRequest,
) => Promise<DistinctValues>;

/** Values longer than this are free text, not a category worth matching. */
const MAX_VALUE_CHARS = 200;

/** A row's value as text, or undefined for a null, a blank or free text. */
export function usableValue(raw: unknown): string | undefined {
   if (raw === null || raw === undefined) return undefined;
   const text = String(raw);
   if (text.trim() === "" || text.length > MAX_VALUE_CHARS) return undefined;
   return text;
}

/** One read of a dimension's distinct values: the query and how it is run. */
interface DistinctValueQuery {
   environmentStore: EnvironmentStore;
   environmentName: string;
   packageName: string;
   modelPath: string;
   source: string;
   dimension: string;
   /** Rows to read; one more is asked for to learn whether there are more. */
   limit: number;
   timeoutMs: number;
   /** When set, only values containing this text (case-insensitive) are read. */
   stem?: string;
   givens?: Record<string, unknown>;
   filterParams?: Record<string, string | string[]>;
   /** Also count each value's rows (`n`) and read the most frequent first. */
   counted: boolean;
   /** Labels a refusal at the query cap; see tryAcquireQuerySlot. */
   routeLabel: string;
   /** Who asked, from the request headers, for the access record. */
   identity?: AccessIdentity;
   /** The system's own read (the index build): no caller, so no access record. */
   system?: boolean;
}

/**
 * The one query that reads a dimension's distinct values: up to `limit + 1`
 * non-null values as rows of `v` (and `n` when counted), through the normal
 * query path, with the caller's givens and no `#(authorize)` bypass. The
 * value index and the query-time lookup both read through it.
 */
export async function queryDistinctValues(
   request: DistinctValueQuery,
): Promise<Array<Record<string, unknown>>> {
   let refusal: SearchErrorDetails | undefined;
   try {
      const run = async (audit: QueryAudit) => {
         const found = await queryModelResolver(
            request.environmentStore,
            request.environmentName,
            request.packageName,
            request.modelPath,
         );
         if ("error" in found) {
            refusal = found.error;
            throw found.cause;
         }
         const version = found.pkg.getVersionId();
         if (version) audit.version = version;
         const dim = quoteMalloyIdentifier(request.dimension);
         // The stem is letters and digits only (see filterStem), so it needs no escaping.
         const where = request.stem
            ? `${dim} != null and lower(${dim}) ~ '%${request.stem}%'`
            : `${dim} != null`;
         const body = request.counted
            ? `where: ${where} group_by: v is ${dim} aggregate: n is count() order_by: n desc limit: ${request.limit + 1}`
            : `where: ${where} group_by: v is ${dim} limit: ${request.limit + 1}`;
         const query = `run: ${quoteMalloyIdentifier(request.source)} -> { ${body} }`;
         const slot = tryAcquireQuerySlot(request.routeLabel);
         try {
            const { compactResult } = await runWithQueryTimeout(
               (signal) =>
                  found.model.runQuery(
                     audit,
                     {
                        query,
                        filterParams: request.filterParams,
                        givens: request.givens as
                           | Record<string, GivenValue>
                           | undefined,
                        abortSignal: signal,
                        responseShape: "compact",
                     },
                     // MCP sends no #(authorize) bypass, ever; nor does value search.
                     QueryPrivileges.none,
                  ),
               request.timeoutMs,
            );
            return compactResult as unknown as Array<Record<string, unknown>>;
         } finally {
            slot.release();
         }
      };
      return request.system
         ? await runUnrecorded(run)
         : await withAccessRecord(
              {
                 environment: request.environmentName,
                 package: request.packageName,
                 modelPath: request.modelPath,
                 requestedSource: request.source,
                 adHoc: true,
                 ...request.identity,
              },
              undefined,
              run,
           );
   } catch (error) {
      if (refusal) throw new Error(refusal.message);
      throw error;
   }
}

/**
 * The lookup query: read up to `limit + 1` distinct non-null values of one
 * dimension, through the normal query path with the caller's givens.
 */
export const defaultValueFetcher: ValueFetcher = async (request) => {
   const rows = await queryDistinctValues({
      ...request,
      counted: false,
      routeLabel: "mcp:getContext:values",
   });
   const values: string[] = [];
   for (const row of rows) {
      const text = usableValue(row.v);
      if (text !== undefined) values.push(text);
   }
   return {
      values: values.slice(0, request.limit),
      truncated: rows.length > request.limit,
   };
};
