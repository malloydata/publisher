// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The heavy half of the entities extraction: everything about a compiled
 * model file that does not depend on which of its sources the query route
 * runs. The package-load worker runs it right after each compile, so a load
 * spends none of it on the main thread; the main thread runs it only for a
 * file the worker left (`extract.ts`). Either way it is this one function.
 *
 * What does depend on the query route is left to `resolveModelEntities` in
 * `extract.ts`: which sources are advertised as queryable, and the descents
 * that depend on that (see `BoundaryDescents`). Under a query boundary, the
 * plan takes those descents provisionally and guards what it finds below
 * them; without one, it leaves them untaken and says which it left.
 *
 * A plan crosses the worker boundary by structured clone, so it holds plain
 * data only. It records no metrics: the main thread records them as it
 * resolves the plan, once per file it serves.
 */

import {
   type InfoConnection,
   type LookupConnection,
   Malloy,
   type Model,
} from "@malloydata/malloy";
import {
   assertAtMostOneRequiredFilterPerIndexedDimension,
   assertNoRequiredParametersOnIndexedDimension,
   collectDimIndexCandidates,
   DimIndexRejectionError,
   type DimIndexCandidate,
   type Refuse,
   givenTypeMap,
   makeDimIndexLookupKey,
} from "./dim_index_discovery";
import {
   buildFingerprintQuery,
   computeSqlFingerprint,
   type DimIndexFingerprintInputs,
} from "./dim_index_entity_id";
import { errMessage } from "../../utils";
import {
   BoundaryDescents,
   type QueryabilityGuard,
   splitSourcePath,
} from "./explore_walk";
import type { Given, SourceInfo } from "./internal_types";
import { FileURLReader } from "./field_helpers";
import { withModelTextCache } from "./model_text";
import {
   type ExploreMetadataPlan,
   type GuardedEntity,
   sourceExtraction,
   RequiredFilterMissingDimensionError,
   RequiredFilterNonPublicDimensionError,
} from "./source_extraction";
import { serializeExploreToSourceInfo } from "./source_info_serializer";

/** A compiled `.malloy` file of the package. */
export interface CompiledEntitiesModelFile {
   /** Package-relative path, `/`-separated. */
   relativePath: string;
   /** Absolute path on disk; its `file://` URL is the model's URL. */
   absolutePath: string;
   model: Model;
   /** The text the model compiled from. */
   text: string;
}

/** An annotation the extraction left out because an index cannot honor it. */
export interface EntitiesFinding {
   modelPath: string;
   message: string;
   /** A short code for the kind of annotation, for the load's warning and metric. */
   reason: string;
}

/** A value found below a provisional descent holds only where its guard does. */
export interface Guarded<T> {
   value: T;
   guard?: QueryabilityGuard;
}

/** An `#(index)` dimension whose fingerprint query did not compile. */
export interface FingerprintFailure {
   sourceName: string;
   fieldName: string;
   error: string;
}

/**
 * One model file's entities, planned before which of its sources the query
 * route runs is known. `failed` is a file the server could not extract.
 */
export type ModelEntitiesPlan =
   | {
        status: "planned";
        relativePath: string;
        /** The package root the plan's paths were resolved against. */
        packageRoot: string;
        /**
         * Whether descents that depend on queryability were taken
         * provisionally. Otherwise they were left untaken, and `untaken` names
         * them by `(root, target)`.
         */
        provisional: boolean;
        untaken: [string, string][];
        /** The model's own givens, whether or not any source carries them. */
        givens: Given[];
        sourceInfos: SourceInfo[];
        /** Per top-level source, in model order. */
        metadata: ExploreMetadataPlan[];
        entities: GuardedEntity[];
        findings: Guarded<EntitiesFinding>[];
        fingerprintFailures: Guarded<FingerprintFailure>[];
        /** Wall-clock ms the plan took. */
        durationMs: number;
     }
   | {
        status: "failed";
        relativePath: string;
        packageRoot: string;
        error: string;
     };

export interface PlanModelEntitiesOptions {
   /** Absolute path of the package root. */
   packageRoot: string;
   /**
    * Connections the `#(index)` fingerprint queries compile against. They read
    * the compiled model's schemas, so a compile asks a connection for its
    * dialect and nothing else.
    */
   connections: LookupConnection<InfoConnection>;
   /** Take the descents that depend on queryability provisionally (see `BoundaryDescents`). */
   provisional: boolean;
   /**
    * The texts the compile read, by absolute path, so a field's code is sliced
    * from the revision its range describes, never read again. Without it the
    * plan reads through the caller's `withModelTextCache`.
    */
   texts?: ReadonlyMap<string, string>;
}

/**
 * Plan a compiled file's entities: its fields, the facts about each of its
 * sources, and the schema of each source, without its sources' queryability.
 * Never throws: a failure is the plan's `failed` status.
 */
export async function planModelFileEntities(
   file: CompiledEntitiesModelFile,
   options: PlanModelEntitiesOptions,
): Promise<ModelEntitiesPlan> {
   const run = () => planOrFail(file, options);
   return options.texts ? withModelTextCache(run, options.texts) : run();
}

async function planOrFail(
   file: CompiledEntitiesModelFile,
   options: PlanModelEntitiesOptions,
): Promise<ModelEntitiesPlan> {
   try {
      return await plan(file, options);
   } catch (error) {
      return {
         status: "failed",
         relativePath: file.relativePath,
         packageRoot: options.packageRoot,
         error: errMessage(error),
      };
   }
}

async function plan(
   file: CompiledEntitiesModelFile,
   options: PlanModelEntitiesOptions,
): Promise<ModelEntitiesPlan> {
   const started = performance.now();
   const { packageRoot } = options;
   const compiledModel = file.model;
   const explores = compiledModel.explores;
   const source = file.text;
   const modelUrl = new URL("file://" + file.absolutePath);
   const boundary = new BoundaryDescents(options.provisional);
   // `##(agent-hidden)` on the model file, in every bracket pair Malloy
   // routes: `#[agent-hidden]` routes exactly as `#(agent-hidden)` does.
   const isAgentHiddenTagPresentOnModelFile = compiledModel
      .getTaglines()
      .some((t) => /^##\|?\s*[([{<]?\s*agent-hidden\b/.test(t));

   // Every annotation the index refuses, so the load names them all.
   const findings: Guarded<EntitiesFinding>[] = [];
   const refuse: Refuse = (error, guard) => {
      const value: EntitiesFinding = {
         modelPath: file.relativePath,
         message: errMessage(error),
         reason: findingReason(error),
      };
      findings.push(guard ? { value, guard } : { value });
   };
   const modelGivens = sourceExtraction.extractGivensFromModel(compiledModel);
   const modelAuthorize = sourceExtraction.extractAuthorizeFromModel(
      compiledModel,
      file.relativePath,
   );
   const metadata = sourceExtraction.planMetadataFromExplores(
      explores,
      file.relativePath,
      isAgentHiddenTagPresentOnModelFile,
      modelUrl.href,
      modelGivens,
      modelAuthorize,
      source,
      packageRoot,
      refuse,
      boundary,
   );

   // The `#(index)`-tagged dimensions. A non-public tagged field is skipped,
   // never refused: an inherited tag internalized via `internal: *` cannot be
   // removed in Malloy.
   const candidates = collectDimIndexCandidates(
      explores,
      file.relativePath,
      givenTypeMap(modelGivens),
      refuse,
      sourceExtraction.joinDescent(
         explores,
         file.relativePath,
         packageRoot,
         source,
         boundary,
      ),
   );
   const { fingerprints, failures } = await buildDimIndexFingerprintLookup({
      compiledModel,
      candidates,
      connections: options.connections,
      modelFilePath: file.relativePath,
      refuse,
   });

   const entities = sourceExtraction.planEntitiesFromExplores(
      explores,
      file.relativePath,
      packageRoot,
      source,
      fingerprints,
      boundary,
   );
   return {
      status: "planned",
      relativePath: file.relativePath,
      packageRoot,
      provisional: boundary.provisional,
      untaken: boundary.untaken,
      givens: modelGivens,
      sourceInfos: explores.map((explore) =>
         serializeExploreToSourceInfo(explore, source),
      ),
      metadata,
      entities,
      findings,
      fingerprintFailures: failures,
      durationMs: performance.now() - started,
   };
}

/** The reason code of an annotation an index cannot honor. */
function findingReason(error: Error): string {
   if (error instanceof DimIndexRejectionError) return error.reason;
   if (error instanceof RequiredFilterMissingDimensionError)
      return "required_filter_missing_dimension";
   if (error instanceof RequiredFilterNonPublicDimensionError)
      return "required_filter_non_public_dimension";
   return "unknown";
}

/**
 * The definitional fingerprint of every `#(index)`-tagged dimension, keyed
 * `${sourceName}::${fieldName}`: the SHA-256 of the SQL its canonical
 * fingerprint query compiles to, plus its required-filter set. Two versions of
 * a package that compile a dimension to the same SQL report the same
 * fingerprint, which is what lets a host reuse the dimension's index across
 * them.
 *
 * The query compiles against the model as loaded, before any materialization
 * manifest is bound, so the fingerprint names the dimension's logical
 * definition and not the table that serves it today.
 *
 * A fragment that fails to compile is left out, and reported in `failures`:
 * that dimension reports no fingerprint.
 */
async function buildDimIndexFingerprintLookup(args: {
   compiledModel: Model;
   candidates: DimIndexCandidate[];
   connections: LookupConnection<InfoConnection>;
   modelFilePath: string;
   refuse: Refuse;
}): Promise<{
   fingerprints: Map<string, DimIndexFingerprintInputs>;
   failures: Guarded<FingerprintFailure>[];
}> {
   const fingerprints = new Map<string, DimIndexFingerprintInputs>();
   const failures: Guarded<FingerprintFailure>[] = [];
   const { candidates } = args;
   if (candidates.length === 0) return { fingerprints, failures };

   // Build the model's QueryModel once: a compile that only adds a query
   // shares it, rather than building one per fingerprint. It is a function
   // of the model alone, so the SQL is the same either way.
   try {
      void args.compiledModel.queryModel;
   } catch {
      // Each compile below builds its own.
   }
   const urlReader = new FileURLReader();
   for (const candidate of candidates) {
      try {
         assertAtMostOneRequiredFilterPerIndexedDimension(
            [candidate],
            args.modelFilePath,
         );
         assertNoRequiredParametersOnIndexedDimension(
            [candidate],
            args.modelFilePath,
         );
      } catch (error) {
         if (!(error instanceof DimIndexRejectionError)) throw error;
         args.refuse(error, candidate.guard);
         continue;
      }
      // A dotted candidate source (`analytics.order_items`) is not a runnable
      // source: compile against the runnable root with a join-qualified field
      // path, as the index queries it.
      const { rootSourceName, joinPath } = splitSourcePath(
         candidate.sourceName,
      );
      const dimensionPath = joinPath
         ? `${joinPath}.${candidate.fieldName}`
         : candidate.fieldName;
      const fragment = buildFingerprintQuery(
         rootSourceName,
         dimensionPath,
         candidate.requiredFilterDimensions.filter(
            (column) => !candidate.gateDimensions.includes(column),
         ),
         candidate.gateDimensions,
      );
      let sql: string;
      try {
         const queryModel = await Malloy.compile({
            urlReader,
            connections: args.connections,
            source: fragment,
            model: args.compiledModel,
         });
         // Extraction runs before any manifest is bound, so this SQL names the
         // source's own tables, never materialized ones that churn.
         sql = queryModel.preparedQuery.preparedResult.sql;
      } catch (err) {
         const value: FingerprintFailure = {
            sourceName: candidate.sourceName,
            fieldName: candidate.fieldName,
            error: errMessage(err),
         };
         failures.push(
            candidate.guard ? { value, guard: candidate.guard } : { value },
         );
         continue;
      }
      fingerprints.set(
         makeDimIndexLookupKey(candidate.sourceName, candidate.fieldName),
         {
            sqlFingerprint: computeSqlFingerprint(sql),
            requiredFilterDimensions: candidate.requiredFilterDimensions,
         },
      );
   }
   return { fingerprints, failures };
}
