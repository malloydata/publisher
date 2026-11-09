// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { InfoConnection, LookupConnection } from "@malloydata/malloy";
import { logger } from "../../logger";
import { recordEntitiesDegraded } from "../../package_load_metrics";
import { guardHolds, holdsFor, type QueryabilityGuard } from "./explore_walk";
import type { ModelCompileResult, SourceAddress } from "./internal_types";
import { withModelTextCache } from "./model_text";
import {
   type CompiledEntitiesModelFile,
   type EntitiesFinding,
   type ModelEntitiesPlan,
   planModelFileEntities,
} from "./plan";
import { sourceExtraction } from "./source_extraction";

export type { EntitiesFinding, ModelEntitiesPlan } from "./plan";

/** A `.malloy` file of the package: compiled, or the reason it did not. */
export type EntitiesModelFile =
   | CompiledEntitiesModelFile
   | { relativePath: string; absolutePath: string; error: string };

/**
 * The query route's answers the entities report, asked of the package that
 * serves them (`Package.entitiesQueryRoute`). The extraction never re-derives
 * the queryability rule; it asks.
 */
export interface EntitiesQueryRoute {
   /** Whether the package's query boundary restricts anything: a surface, and `queryableSources` not `"all"`. */
   boundaryActive: boolean;
   /** Whether the query route runs a query naming `sourceName` in `modelPath` (`Model.admitsSourceByName`). */
   admitsSource(modelPath: string, sourceName: string): boolean;
   /** Whether `modelPath` is a served report, of either layout. */
   isServedReport(modelPath: string): boolean;
   /**
    * The served source `sourceName` in `modelPath` is, or is built on: the
    * listed non-report file that exports that declaration, and the name it
    * exports it under. Undefined when there is none.
    */
   servedSourceOf(
      modelPath: string,
      sourceName: string,
   ): SourceAddress | undefined;
}

export interface ExtractPackageEntitiesInput {
   /** Absolute path of the package root. */
   packageRoot: string;
   /** The package's name, for the extraction's logs. */
   packageName?: string;
   /** Every `.malloy` file of the package, listed files or not. */
   modelFiles: EntitiesModelFile[];
   /** Which sources the query route runs, asked per source. */
   queryRoute: EntitiesQueryRoute;
   /**
    * Connections the `#(index)` fingerprint queries compile against. They read
    * the compiled model's schemas, so a compile asks a connection for its
    * dialect and nothing else.
    */
   connections: LookupConnection<InfoConnection>;
   /**
    * Plans the package-load worker made, by model path. A compiled file
    * without one, or whose plan does not hold for the query route, is planned
    * here, on this thread.
    */
   plans?: ReadonlyMap<string, ModelEntitiesPlan>;
}

/**
 * Compute every model file's entities: its fields, the facts about each of its
 * sources, and the schema of each source.
 *
 * Each file is planned once ({@link planModelFileEntities}, usually by the
 * load worker), then resolved against the query route here
 * ({@link resolveModelEntities}). Whether a source is queryable is the query
 * route's answer ({@link EntitiesQueryRoute}), with one deliberate difference:
 * see {@link advertisedQueryability}.
 *
 * An annotation an index cannot honor (a required `#(filter)` on a dimension
 * that does not exist or is not public, an `#(index)` the index cannot use) is
 * reported as a finding and left out of the entities; the load warns with it.
 * Any other failure is reported on that file's result, so one file never hides
 * the rest.
 */
export function extractPackageEntities(
   input: ExtractPackageEntitiesInput,
): Promise<PackageEntitiesExtraction> {
   const texts = new Map<string, string>();
   for (const file of input.modelFiles) {
      if ("text" in file) texts.set(file.absolutePath, file.text);
   }
   return withModelTextCache(() => extractAll(input), texts);
}

export interface PackageEntitiesExtraction {
   results: ModelCompileResult[];
   findings: EntitiesFinding[];
   /** The compiled files planned on this thread rather than taken from a worker plan. */
   plannedHere: string[];
}

async function extractAll(
   input: ExtractPackageEntitiesInput,
): Promise<PackageEntitiesExtraction> {
   const results: ModelCompileResult[] = [];
   const findings: EntitiesFinding[] = [];
   const plannedHere: string[] = [];
   const route = input.queryRoute;
   const planHere = (file: CompiledEntitiesModelFile, provisional: boolean) =>
      planModelFileEntities(file, {
         packageRoot: input.packageRoot,
         connections: input.connections,
         provisional,
      });
   for (const file of input.modelFiles) {
      if (!("model" in file)) {
         results.push(errorResult(file.relativePath, file.error));
         continue;
      }
      const workerPlan = input.plans?.get(file.relativePath);
      let resolved =
         workerPlan?.packageRoot === input.packageRoot
            ? resolveModelEntities(workerPlan, route, input.packageName)
            : undefined;
      if (!resolved) {
         plannedHere.push(file.relativePath);
         // A plan that left the boundary's descents untaken holds unless the
         // route makes a source unqueryable that one of them reaches; the
         // provisional plan always holds.
         resolved =
            resolveModelEntities(
               await planHere(file, route.boundaryActive),
               route,
               input.packageName,
            ) ??
            resolveModelEntities(
               await planHere(file, true),
               route,
               input.packageName,
            )!;
      }
      results.push(resolved.result);
      findings.push(...resolved.findings);
   }
   return { results, findings, plannedHere };
}

/**
 * A file's entities from its plan, with the query route's answers applied:
 * which sources are advertised as queryable ({@link advertisedQueryability}),
 * and of what the plan found below a provisional descent, what holds for
 * them. Undefined when the plan left a descent untaken that these answers
 * take, so the file must be planned again, provisionally.
 *
 * Records the degradations it reports (`publisher_package_entities_degraded_total`).
 */
export function resolveModelEntities(
   plan: ModelEntitiesPlan,
   route: EntitiesQueryRoute,
   packageName: string | undefined,
): { result: ModelCompileResult; findings: EntitiesFinding[] } | undefined {
   if (plan.status === "failed") {
      return extractionFailed(plan.relativePath, plan.error, packageName);
   }
   try {
      // Asked of the query route, never re-derived (see advertisedQueryability).
      const { notQueryable, servedFrom } = advertisedQueryability(
         route,
         plan.relativePath,
         plan.metadata.map((source) => source.sourceName),
      );
      if (!plan.provisional && !holdsFor(plan.untaken, notQueryable)) {
         return undefined;
      }
      const holds = ({ guard }: { guard?: QueryabilityGuard }) =>
         guardHolds(guard, notQueryable);
      for (const { value: failure } of plan.fingerprintFailures.filter(holds)) {
         logger.warn(
            "Could not compile an #(index) fingerprint query; the dimension reports no fingerprint",
            {
               packageName,
               modelPath: plan.relativePath,
               sourceName: failure.sourceName,
               fieldName: failure.fieldName,
               error: failure.error,
            },
         );
         recordEntitiesDegraded("fingerprint_failed");
      }
      const result: ModelCompileResult = {
         modelFilePath: plan.relativePath,
         entities: plan.entities.filter(holds).map(({ entity }) => entity),
         sourceMetadataEntries: sourceExtraction.resolveMetadataPlan(
            plan.metadata,
            plan.relativePath,
            notQueryable,
            servedFrom,
         ),
         compileStatus: "SUCCESS",
         givens: plan.givens,
         sourceInfos: plan.sourceInfos,
      };
      return {
         result,
         findings: plan.findings.filter(holds).map(({ value }) => value),
      };
   } catch (error) {
      return extractionFailed(
         plan.relativePath,
         error instanceof Error ? error.message : String(error),
         packageName,
      );
   }
}

/** A file the server could not extract: logged, counted, and reported on its result. */
function extractionFailed(
   relativePath: string,
   message: string,
   packageName: string | undefined,
): { result: ModelCompileResult; findings: EntitiesFinding[] } {
   logger.warn("Could not extract a model's entities", {
      packageName,
      modelPath: relativePath,
      error: message,
   });
   recordEntitiesDegraded("file_extract_failed");
   return {
      result: errorResult(relativePath, message, "extraction"),
      findings: [],
   };
}

/**
 * The package warning code for a finding's reason: one stable code per kind of
 * annotation, which a host that indexes can refuse a publish on.
 */
export function entitiesFindingCode(reason: string): string {
   return `entities-${reason.replaceAll("_", "-")}`;
}

/**
 * A file with no entities: it did not compile (`compile`, the author's to
 * fix), or the server could not extract it (`extraction`, the server's).
 */
function errorResult(
   relativePath: string,
   message?: string,
   errorKind: "compile" | "extraction" = "compile",
): ModelCompileResult {
   return {
      modelFilePath: relativePath,
      entities: [],
      sourceMetadataEntries: [],
      compileStatus: "ERROR",
      errorKind,
      ...(message !== undefined && { errorMessage: message }),
   };
}

/**
 * Which of the sources in `modelPath` the entities advertise as queryable,
 * and, for those a served report reads, the served source to advertise in
 * each one's place.
 *
 * A source is advertised exactly when the query route runs it, with ONE
 * deliberate difference: under an active query boundary, nothing a served
 * report declares or imports is advertised, although the route runs a query
 * addressed to the report that names its re-import of a served source
 * (`orders`) or a source it builds on one (`open_orders is orders extend
 * {...}`). Each such source is marked NOT_QUERYABLE and names its served
 * source (`servedFrom`), so a search shows one `orders`, from the file that
 * serves it, rather than one per report that reads it. A report is a view onto
 * the served surface, not an addition to it. Without a boundary no file is
 * read as a report, and everything compiled is advertised.
 */
export function advertisedQueryability(
   route: EntitiesQueryRoute,
   modelPath: string,
   sourceNames: readonly string[],
): {
   notQueryable: Set<string>;
   servedFrom: Map<string, SourceAddress>;
} {
   const servedReport = route.boundaryActive && route.isServedReport(modelPath);
   const notQueryable = new Set<string>();
   const servedFrom = new Map<string, SourceAddress>();
   for (const name of sourceNames) {
      if (servedReport) {
         notQueryable.add(name);
         const served = route.servedSourceOf(modelPath, name);
         if (served) servedFrom.set(name, served);
      } else if (!route.admitsSource(modelPath, name)) {
         notQueryable.add(name);
      }
   }
   return { notQueryable, servedFrom };
}
