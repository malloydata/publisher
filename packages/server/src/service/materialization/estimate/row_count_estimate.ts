// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { randomUUID } from "crypto";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { components } from "../../../api";
import { getQueryTimeoutMs } from "../../../config";
import { BadRequestError, BuildPlanUnavailableError } from "../../../errors";
import { logger } from "../../../logger";
import { runWithQueryTimeout } from "../../../query_timeout";
import { errMessage } from "../../../utils";
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
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
   | "not_examined";

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
export function readRowCount(rows: readonly unknown[]): number {
   const row = rows[0];
   if (row === null || typeof row !== "object") {
      throw new Error("the count statement returned no row");
   }
   const entry = Object.entries(row as Record<string, unknown>).find(
      ([key]) => key.toLowerCase() === "row_count",
   );
   const raw = entry?.[1];
   const value =
      typeof raw === "bigint"
         ? Number(raw)
         : typeof raw === "string"
           ? Number(raw)
           : raw;
   if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
      throw new Error(
         `the count statement returned ${JSON.stringify(String(raw))}, not a row count`,
      );
   }
   return value;
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
   if (!eligibility.eligible.includes(name) || name in eligibility.refused) {
      const why = eligibility.refused[name];
      return {
         reason: "gated",
         message:
            `'${name}' is not counted: a count over its build query reads every caller's rows, and the strictest materialization gate does not admit it` +
            (why ? ` (${why})` : "") +
            ".",
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
         (id) => !(id in plan.sources),
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
   const { sources, maxRows } = body as Record<string, unknown>;
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
   return request;
}

/** Runs one count for one planned source and returns its rows. */
export type CountRunner = (
   source: PersistSourcePlan,
   sql: string,
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
   /** Aborted when the client goes away; no further statement is issued. */
   signal?: AbortSignal;
}): Promise<BuildPlanEstimates> {
   const { plan, eligibility, request, countRows } = args;
   const now = args.now ?? (() => new Date());
   const clock = args.clock ?? (() => performance.now());
   const measuredAt = now().toISOString();
   const estimates: Record<string, RowCountEstimate> = {};

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
      const started = clock();
      try {
         const rows = readRowCount(
            await countRows(source, rowCountSQL(source.sql)),
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

/** What the estimate needs from a loaded package. */
export interface EstimatablePackage {
   getPackageName(): string;
   getBuildPlan(): BuildPlan | null;
   isBuildPlanUnavailable(): boolean;
   getSourceEligibility(): SourceEligibility | undefined;
   getMalloyConnection(connectionName: string): Promise<MalloyConnection>;
}

/**
 * Estimate a loaded package's plan, running each count on the source's own
 * connection under the server's query timeout and tagged like a build of that
 * source (`class` materialize, `trigger` estimate, one `run_id` per request).
 *
 * @throws {BuildPlanUnavailableError} when the plan could not be derived at load.
 * @throws {BadRequestError} for a malformed request or an unknown source.
 */
export async function estimatePackageRowCounts(args: {
   pkg: EstimatablePackage;
   environmentName: string;
   body: unknown;
   /** The connection's default and enforced query-metadata layers. */
   connectionMetadata?: (connectionName: string) => {
      default?: QueryMetadata | null;
      enforced?: QueryMetadata | null;
   } | null;
   /** Aborted when the client goes away. */
   signal?: AbortSignal;
}): Promise<BuildPlanEstimates> {
   const { pkg, environmentName } = args;
   const request = parseEstimateRequest(args.body);
   if (pkg.isBuildPlanUnavailable()) {
      throw new BuildPlanUnavailableError(
         `The build plan of package ${pkg.getPackageName()} could not be derived at load, so nothing can be estimated. Reload the package to derive it again.`,
      );
   }
   const plan = pkg.getBuildPlan() ?? { graphs: [], sources: {} };
   const runId = randomUUID();
   return estimateRowCounts({
      plan,
      eligibility: pkg.getSourceEligibility(),
      request,
      signal: args.signal,
      countRows: async (source, sql) => {
         const connection = await pkg.getMalloyConnection(
            source.connectionName,
         );
         const layers = args.connectionMetadata?.(source.connectionName);
         const { metadata } = mergeQueryMetadata({
            connection: layers?.default,
            enforced: layers?.enforced,
            model: source.queryMetadata,
            context: {
               queryClass: "materialize",
               environment: environmentName,
               package: pkg.getPackageName(),
               source: source.name,
               trigger: "estimate",
               runId,
            },
         });
         const result = await runWithQueryTimeout(
            (abortSignal) =>
               connection.runSQL(sql, {
                  ...(metadata ? { queryMetadata: metadata } : {}),
                  abortSignal,
               }),
            getQueryTimeoutMs(),
            args.signal,
         );
         return result.rows;
      },
   });
}
