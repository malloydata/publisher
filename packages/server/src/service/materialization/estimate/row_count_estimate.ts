// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { randomUUID } from "crypto";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { components } from "../../../api";
import { getQueryTimeoutMs } from "../../../config";
import { BadRequestError } from "../../../errors";
import { logger } from "../../../logger";
import { runWithQueryTimeout } from "../../../query_timeout";
import { errMessage } from "../../../utils";
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
import { withStatementUsage } from "../../statement_usage";
import type { SourceEligibility } from "../plan/build_plan";

type BuildPlan = components["schemas"]["BuildPlan"];
type PersistSourcePlan = components["schemas"]["PersistSourcePlan"];
type RefusedSource = components["schemas"]["RefusedSource"];
export type BuildPlanEstimates = components["schemas"]["BuildPlanEstimates"];
export type RowCountEstimate = components["schemas"]["RowCountEstimate"];
export type BuildPlanEstimateRequest =
   components["schemas"]["BuildPlanEstimateRequest"];

/**
 * Row-count estimates for a build plan (materialization v2, telemetry T4, the
 * phase 2 part): how many rows each planned table would hold, with the method
 * that produced the number and its error band.
 *
 * Nothing here runs at load. The only caller is the estimate route, so a count
 * is a warehouse statement somebody asked for; see row_count_estimate_load.spec.ts.
 *
 * The one method today is an exact count over the build query. It costs what
 * the build's read costs, which is why it is opt-in. Cheaper methods with wider
 * bands (table metadata, sampled or approximate counts, dry runs) are the T5
 * adapters' work and add `method` values; `method` is an open string on the wire
 * so they can.
 */

/** An exact `COUNT(*)` over the source's build query. */
export const COUNT_QUERY_METHOD = "count_query";

/** Why a source was not counted. An open set on the wire. */
export type EstimateRefusalReason =
   | "not_buildable"
   | "gated"
   | "caller_scoped"
   | "not_examined"
   /** Its dialect cannot run the count over its build query as written. */
   | "dialect"
   /** Past the request's source cap, so not attempted. */
   | "limit"
   /** Past the request's time budget, so not attempted. */
   | "deadline";

/**
 * The most sources one request counts. Each count costs what its build's read
 * costs and the request holds one query slot throughout, so a request is
 * bounded twice: by this cap, and by one query timeout for all its counts.
 */
export const MAX_SOURCES_PER_REQUEST = 25;

/**
 * Dialects whose build query cannot be counted as a derived table: SQL Server
 * forbids a `WITH` inside one and an `ORDER BY` without `TOP`, both of which
 * Malloy's build SQL carries. Refused with a reason rather than sent to fail.
 */
const UNCOUNTABLE_DIALECTS: ReadonlySet<string> = new Set(["sqlserver"]);

/** The alias the count's subquery takes; also how a test finds the statement. */
export const ROW_ESTIMATE_ALIAS = "publisher_row_estimate";

/**
 * The count statement for one build query. A trailing `;` is dropped because a
 * subquery cannot carry one. The alias is a plain lower-case identifier every
 * supported dialect accepts unquoted.
 */
export function rowCountSQL(buildSQL: string): string {
   const body = buildSQL.trim().replace(/;+\s*$/, "");
   return `SELECT COUNT(*) AS row_count FROM (\n${body}\n) AS ${ROW_ESTIMATE_ALIAS}`;
}

/**
 * The count from the statement's first row. The column is found
 * case-insensitively (Snowflake folds an unquoted alias to upper case), and a
 * bigint or numeric string is accepted, since drivers differ.
 *
 * @throws {Error} when the row holds no non-negative integer count.
 */
export function readRowCount(rows: readonly unknown[]): {
   rows: number;
   /** The count exceeds 2^53, so `rows` is the nearest double, not exact. */
   rounded: boolean;
} {
   const row = rows[0];
   if (row === null || typeof row !== "object") {
      throw new Error("the count statement returned no row");
   }
   const entry = Object.entries(row as Record<string, unknown>).find(
      ([key]) => key.toLowerCase() === "row_count",
   );
   const raw = entry?.[1];
   const notACount = () =>
      new Error(
         `the count statement returned ${JSON.stringify(String(raw))}, not a row count`,
      );
   // Read as a BigInt first, so a count past 2^53 is still recognized as one;
   // an empty or whitespace string is not a count (Number("") is 0).
   let count: bigint;
   if (typeof raw === "bigint") {
      count = raw;
   } else if (typeof raw === "number" && Number.isInteger(raw)) {
      count = BigInt(raw);
   } else if (typeof raw === "string" && /^\s*\d+\s*$/.test(raw)) {
      count = BigInt(raw.trim());
   } else {
      throw notACount();
   }
   if (count < 0n) throw notACount();
   return {
      rows: Number(count),
      rounded: count > BigInt(Number.MAX_SAFE_INTEGER),
   };
}

/**
 * Why `source` may not be counted, or undefined when it may.
 *
 * A count over the build query reads every row the build would, which is every
 * caller's rows, so a source is counted only when nothing scopes it per caller:
 *
 * - the plan itself refuses it (any tier), so there is no build to size;
 * - the STORAGE gate, the strictest one, does not admit it by name: it refuses
 *   any `#(authorize)` or `#(access_filter)` on the source, its base or a joined
 *   source, a free parameter, and a given in the persisted query. Keyed by name
 *   as that gate is, so a name refused anywhere in the package refuses every
 *   source of that name; failing closed is the point;
 * - the build strips a caller-scoped term from it (a given in its extend-block
 *   `where:`, or a join to a given-scoped source), which the storage gate admits
 *   because the read re-applies it, but which leaves the build query unscoped.
 *
 * Eligibility that was never computed proves nothing, so it refuses everything.
 */
export function estimateRefusal(
   sourceID: string,
   source: PersistSourcePlan | undefined,
   refused: RefusedSource | undefined,
   eligibility: SourceEligibility | undefined,
): { reason: EstimateRefusalReason; message: string } | undefined {
   const name = source?.name ?? refused?.name ?? sourceID;
   // In both `sources` and `refusedSources`: a rollup the authorize gate
   // refuses keeps its plan entry, so its table exists but is gated.
   if (refused && source) {
      return {
         reason: "gated",
         message: `'${name}' is not counted: the build plan refuses it (${refused.reason}): ${refused.message}`,
      };
   }
   if (refused || !source) {
      return {
         reason: "not_buildable",
         message:
            `The build plan refuses '${name}'` +
            (refused ? ` (${refused.reason}): ${refused.message}` : ".") +
            " There is no table to size.",
      };
   }
   if (eligibility === undefined) {
      return {
         reason: "not_examined",
         message: `The package's materialization eligibility was not computed, so nothing proves '${name}' is ungated. Reload the package.`,
      };
   }
   if (
      !eligibility.eligible.includes(name) ||
      Object.hasOwn(eligibility.refused, name)
   ) {
      const why = Object.hasOwn(eligibility.refused, name)
         ? eligibility.refused[name]
         : undefined;
      return {
         reason: "gated",
         message:
            `'${name}' is not counted: a count over its build query reads every caller's rows, and the strictest materialization gate does not admit it` +
            (why ? ` (${why})` : "") +
            ".",
      };
   }
   if (source.dialect && UNCOUNTABLE_DIALECTS.has(source.dialect)) {
      return {
         reason: "dialect",
         message: `'${name}' is not counted: its dialect (${source.dialect}) cannot count its build query as a derived table.`,
      };
   }
   if (
      (source.strippedTerms?.length ?? 0) > 0 ||
      (source.joinedTerms?.length ?? 0) > 0
   ) {
      return {
         reason: "caller_scoped",
         message: `'${name}' is not counted: its build leaves out a caller-scoped term the read re-applies, so a count over the build query would report every caller's rows.`,
      };
   }
   return undefined;
}

/** The plan's sourceIDs a request selects, in plan order. */
export function selectSources(
   plan: BuildPlan,
   selection: readonly string[] | undefined,
): string[] {
   const all = [
      ...Object.keys(plan.sources),
      ...Object.keys(plan.refusedSources ?? {}).filter(
         (id) => !Object.hasOwn(plan.sources, id),
      ),
   ];
   if (selection === undefined) return all;
   const nameOf = (id: string) =>
      plan.sources[id]?.name ?? plan.refusedSources?.[id]?.name;
   const wanted = new Set(selection);
   const unknown = selection.filter(
      (entry) => !all.some((id) => id === entry || nameOf(id) === entry),
   );
   if (unknown.length > 0) {
      throw new BadRequestError(
         `The build plan holds no source ${unknown.map((u) => `'${u}'`).join(", ")}. Name a sourceID or a source name from GET …/build-plan.`,
      );
   }
   return all.filter((id) => wanted.has(id) || wanted.has(nameOf(id) ?? ""));
}

/** Read a request body into a request, refusing a malformed one with a 400. */
export function parseEstimateRequest(body: unknown): BuildPlanEstimateRequest {
   if (body === undefined || body === null) return {};
   if (typeof body !== "object" || Array.isArray(body)) {
      throw new BadRequestError("The estimate request body must be an object.");
   }
   const { sources, maxRows, maxSources } = body as Record<string, unknown>;
   const request: BuildPlanEstimateRequest = {};
   if (sources !== undefined) {
      if (
         !Array.isArray(sources) ||
         !sources.every((s) => typeof s === "string" && s.length > 0)
      ) {
         throw new BadRequestError(
            "`sources` must be an array of sourceIDs or source names.",
         );
      }
      request.sources = sources as string[];
   }
   if (maxRows !== undefined) {
      if (
         typeof maxRows !== "number" ||
         !Number.isSafeInteger(maxRows) ||
         maxRows < 0
      ) {
         throw new BadRequestError("`maxRows` must be a non-negative integer.");
      }
      request.maxRows = maxRows;
   }
   if (maxSources !== undefined) {
      if (
         typeof maxSources !== "number" ||
         !Number.isSafeInteger(maxSources) ||
         maxSources < 1 ||
         maxSources > MAX_SOURCES_PER_REQUEST
      ) {
         throw new BadRequestError(
            `\`maxSources\` must be an integer from 1 to ${MAX_SOURCES_PER_REQUEST}.`,
         );
      }
      request.maxSources = maxSources;
   }
   return request;
}

/**
 * Runs one count for one planned source and returns its rows, within
 * `budgetMs` (the request's remaining time, never more than a query timeout).
 */
export type CountRunner = (
   source: PersistSourcePlan,
   sql: string,
   budgetMs: number,
) => Promise<readonly unknown[]>;

/**
 * Estimate the selected sources of `plan`, one statement at a time so a
 * request never holds more than one warehouse slot. A refused source issues no
 * statement; a failed one is reported and the rest still run.
 */
export async function estimateRowCounts(args: {
   plan: BuildPlan;
   eligibility: SourceEligibility | undefined;
   request: BuildPlanEstimateRequest;
   countRows: CountRunner;
   now?: () => Date;
   clock?: () => number;
   /** The whole request's time budget; defaults to one query timeout. */
   budgetMs?: number;
   /** Aborted when the client goes away; no further statement is issued. */
   signal?: AbortSignal;
}): Promise<BuildPlanEstimates> {
   const { plan, eligibility, request, countRows } = args;
   const now = args.now ?? (() => new Date());
   const clock = args.clock ?? (() => performance.now());
   const measuredAt = now().toISOString();
   const estimates: Record<string, RowCountEstimate> = {};
   const cap = request.maxSources ?? MAX_SOURCES_PER_REQUEST;
   // A timeout of zero or less is no timeout, so no budget: the cap alone
   // bounds the request then.
   const timeoutMs = getQueryTimeoutMs();
   const budgetMs =
      args.budgetMs ?? (timeoutMs > 0 ? timeoutMs : Number.POSITIVE_INFINITY);
   const deadline = clock() + budgetMs;
   let counted = 0;

   for (const sourceID of selectSources(plan, request.sources)) {
      const source = plan.sources[sourceID];
      const refused = plan.refusedSources?.[sourceID];
      const base: RowCountEstimate = {
         sourceID,
         name: source?.name ?? refused?.name ?? sourceID,
         status: "refused",
         ...describeSource(source, refused),
      };
      const refusal = estimateRefusal(sourceID, source, refused, eligibility);
      if (refusal || !source) {
         estimates[sourceID] = {
            ...base,
            reason: refusal?.reason ?? "not_buildable",
            message: refusal?.message,
         };
         continue;
      }
      args.signal?.throwIfAborted();
      if (counted >= cap) {
         estimates[sourceID] = {
            ...base,
            reason: "limit",
            message: `Not attempted: the request counts at most ${cap} sources. Name the rest in \`sources\` in another request.`,
         };
         continue;
      }
      const remaining = deadline - clock();
      if (remaining <= 0) {
         estimates[sourceID] = {
            ...base,
            reason: "deadline",
            message: `Not attempted: the request's time budget (one query timeout, ${budgetMs} ms) ran out. Name it in \`sources\` in another request.`,
         };
         continue;
      }
      counted++;
      const started = clock();
      try {
         const { rows, rounded } = readRowCount(
            await countRows(source, rowCountSQL(source.sql), remaining),
         );
         const estimate: RowCountEstimate = {
            ...base,
            status: "estimated",
            rows,
            band: { low: rows, high: rows },
            method: COUNT_QUERY_METHOD,
            methodDetail:
               source.origin === "preaggregate"
                  ? "Exact COUNT(*) over the rollup's build query, which groups by its grain, so the count is the number of distinct grain tuples."
                  : "Exact COUNT(*) over the source's build query, on its own connection.",
            elapsedMs: Math.round(clock() - started),
            ...(rounded ? { rowsRounded: true } : {}),
         };
         if (request.maxRows !== undefined) {
            estimate.exceedsMaxRows = rows > request.maxRows;
            if (estimate.exceedsMaxRows) {
               logger.warn(
                  "A planned table's row-count estimate exceeds the requested bound",
                  { sourceID, rows, maxRows: request.maxRows },
               );
            }
         }
         estimates[sourceID] = estimate;
      } catch (err) {
         // A client that went away is not a failed estimate: stop, and let
         // the route answer the aborted request as it answers any other.
         if (args.signal?.aborted) throw err;
         estimates[sourceID] = {
            ...base,
            status: "failed",
            message: errMessage(err),
            elapsedMs: Math.round(clock() - started),
         };
      }
   }

   return {
      measuredAt,
      ...(request.maxRows !== undefined ? { maxRows: request.maxRows } : {}),
      estimates,
   };
}

function describeSource(
   source: PersistSourcePlan | undefined,
   refused: RefusedSource | undefined,
): Partial<RowCountEstimate> {
   const modelPath = source?.modelPath ?? refused?.modelPath;
   return {
      ...(modelPath !== undefined ? { modelPath } : {}),
      ...(source ? { origin: source.origin ?? "persist" } : {}),
      ...(source?.preaggregate
         ? {
              baseSourceName: source.preaggregate.baseSourceName,
              grainDimensions: source.preaggregate.grainDimensions,
           }
         : {}),
   };
}

/**
 * One count's timeout: the query timeout, cut to the request's remaining
 * budget. Zero (no timeout) when the server sets none.
 */
export function countTimeoutMs(
   queryTimeoutMs: number,
   budgetMs: number,
): number {
   if (queryTimeoutMs <= 0) return 0;
   return Math.max(1, Math.min(queryTimeoutMs, Math.ceil(budgetMs)));
}

/** What the estimate needs from a loaded package. */
export interface EstimatablePackage {
   getPackageName(): string;
   getVersionId(): string | undefined;
   getSourceEligibility(): SourceEligibility | undefined;
   getMalloyConnection(connectionName: string): Promise<MalloyConnection>;
}

/**
 * Estimate a loaded package's plan, running each count on the source's own
 * connection, tagged like a build of that source (`class` materialize,
 * `trigger` estimate, one `run_id` per request) and metered like one.
 *
 * `plan` is the package's build plan as `GET …/build-plan` answers it, so the
 * plan-unavailable rule is that route's, in one place.
 *
 * @throws {BadRequestError} for a malformed request or an unknown source.
 */
export async function estimatePackageRowCounts(args: {
   pkg: EstimatablePackage;
   plan: BuildPlan;
   environmentName: string;
   body: unknown;
   /** The connection's default and enforced query-metadata layers. */
   connectionMetadata?: (connectionName: string) => {
      default?: QueryMetadata | null;
      enforced?: QueryMetadata | null;
   } | null;
   /** Whether a statement on this connection is metered (see statement_usage). */
   meteredConnection?: (connectionName: string) => boolean;
   /** Aborted when the client goes away. */
   signal?: AbortSignal;
}): Promise<BuildPlanEstimates> {
   const { pkg, plan, environmentName } = args;
   const request = parseEstimateRequest(args.body);
   const runId = randomUUID();
   const version = pkg.getVersionId();
   return estimateRowCounts({
      plan,
      eligibility: pkg.getSourceEligibility(),
      request,
      signal: args.signal,
      countRows: async (source, sql, budgetMs) => {
         const connection = await pkg.getMalloyConnection(
            source.connectionName,
         );
         const layers = args.connectionMetadata?.(source.connectionName);
         const { metadata, drops } = mergeQueryMetadata({
            connection: layers?.default,
            enforced: layers?.enforced,
            model: source.queryMetadata,
            context: {
               queryClass: "materialize",
               environment: environmentName,
               package: pkg.getPackageName(),
               ...(version ? { version } : {}),
               source: source.name,
               trigger: "estimate",
               runId,
            },
         });
         if (drops.length > 0) {
            logger.warn("Dropped query-metadata properties for an estimate", {
               sourceName: source.name,
               drops,
            });
         }
         const run = () =>
            runWithQueryTimeout(
               (abortSignal) =>
                  connection.runSQL(sql, {
                     ...(metadata ? { queryMetadata: metadata } : {}),
                     abortSignal,
                  }),
               countTimeoutMs(getQueryTimeoutMs(), budgetMs),
               args.signal,
            );
         let metered = false;
         try {
            metered = args.meteredConnection?.(source.connectionName) ?? false;
         } catch {
            // A metering check never fails a statement.
         }
         const result = metered
            ? await withStatementUsage(
                 {
                    environment: environmentName,
                    connection: source.connectionName,
                    surface: "build",
                    package: pkg.getPackageName(),
                 },
                 run,
                 // The count's own one row, not the table's; a scan's cost is
                 // its duration.
                 (r) => r.rows.length,
              )
            : await run();
         return result.rows;
      },
   });
}
