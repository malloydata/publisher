// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type InfoConnection,
   type LookupConnection,
   Malloy,
   type Model,
} from "@malloydata/malloy";
import { logger } from "../../logger";
import {
   assertAtMostOneRequiredFilterPerIndexedDimension,
   assertNoRequiredParametersOnIndexedDimension,
   collectDimIndexCandidates,
   DimIndexRejectionError,
   type DimIndexCandidate,
   givenTypeMap,
   makeDimIndexLookupKey,
} from "./dim_index_discovery";
import {
   buildFingerprintQuery,
   computeSqlFingerprint,
   type DimIndexFingerprintInputs,
} from "./dim_index_entity_id";
import { errorMessage } from "./errors";
import { splitSourcePath } from "./explore_walk";
import type { ModelCompileResult } from "./internal_types";
import { FileURLReader } from "./field_helpers";
import { withModelTextCache } from "./model_text";
import {
   DuplicateRequiredFilterError,
   sourceExtraction,
   RequiredFilterMissingDimensionError,
   RequiredFilterNonPublicDimensionError,
} from "./source_extraction";
import {
   buildPackageQuerySurface,
   nonQueryableSources,
   type PackageManifest,
   type PackageQuerySurface,
   servedFrom,
   type SurfaceModelFile,
} from "./query_surface";
import { serializeExploreToSourceInfo } from "./source_info_serializer";

/** A `.malloy` file of the package: compiled, or the reason it did not. */
export type EntitiesModelFile =
   | {
        /** Package-relative path, `/`-separated. */
        relativePath: string;
        /** Absolute path on disk; its `file://` URL is the model's URL. */
        absolutePath: string;
        model: Model;
        /** The text the model compiled from. */
        text: string;
     }
   | { relativePath: string; absolutePath: string; error: string };

export interface ExtractPackageEntitiesInput {
   /** Absolute path of the package root. */
   packageRoot: string;
   /** Every `.malloy` file of the package, listed files or not. */
   modelFiles: EntitiesModelFile[];
   /** The package's discovery surface; absent when it curates nothing. */
   manifest?: PackageManifest;
   /**
    * Connections the `#(index)` fingerprint queries compile against. They read
    * the compiled model's schemas, so a compile asks a connection for its
    * dialect and nothing else.
    */
   connections: LookupConnection<InfoConnection>;
}

/**
 * Compute every model file's entities: its fields, the facts about each of its
 * sources, and the schema of each source.
 *
 * Two passes, because whether a source is queryable in one file depends on
 * what the other files export (see `query_surface.ts`): the package's query
 * surface is resolved from every compiled file first, then each file is
 * extracted against it.
 *
 * Throws an {@link EntitiesValidationError} when an annotation the extraction
 * reads is invalid (a required `#(filter)` on a dimension that does not exist
 * or is not public, two required filters on one dimension, an `#(index)` the
 * index cannot honor). The load refuses the package with every such message.
 * Any other failure is reported on that file's result, so one file never hides
 * the rest.
 */
export function extractPackageEntities(
   input: ExtractPackageEntitiesInput,
): Promise<ModelCompileResult[]> {
   return withModelTextCache(() => extractAll(input));
}

async function extractAll(
   input: ExtractPackageEntitiesInput,
): Promise<ModelCompileResult[]> {
   const compiled = input.modelFiles.filter(
      (f): f is Extract<EntitiesModelFile, { model: Model }> => "model" in f,
   );
   // A file whose part of the surface cannot be read is reported on its own,
   // as a compile failure is, rather than failing the whole package.
   const surfaceErrors = new Map<string, string>();
   const surface = buildPackageQuerySurface(
      input.manifest,
      compiled as SurfaceModelFile[],
      (file, error) => {
         const message = `Error reading the query surface of ${file.relativePath}: ${errorMessage(error)}`;
         logger.warn(message);
         surfaceErrors.set(file.relativePath, message);
      },
   );
   const results: ModelCompileResult[] = [];
   const rejections: { message: string; reason: string }[] = [];
   for (const file of input.modelFiles) {
      if (!("model" in file)) {
         results.push(errorResult(file.relativePath, file.error));
         continue;
      }
      const surfaceError = surfaceErrors.get(file.relativePath);
      if (surfaceError !== undefined) {
         results.push(errorResult(file.relativePath, surfaceError));
         continue;
      }
      try {
         results.push(
            await extractModelFile(
               file,
               input.packageRoot,
               surface,
               input.connections,
            ),
         );
      } catch (error) {
         const reason = refusalReason(error);
         if (reason !== undefined) {
            rejections.push({ message: errorMessage(error), reason });
            continue;
         }
         const message = errorMessage(error);
         logger.warn(
            `Failed to extract the entities of ${file.relativePath}: ${message}`,
            { error },
         );
         results.push(errorResult(file.relativePath, message));
      }
   }
   if (rejections.length > 0) {
      throw new EntitiesValidationError(
         rejections.map((r) => r.message),
         rejections.map((r) => r.reason),
      );
   }
   return results;
}

/**
 * One or more model files declare an annotation the index cannot honor.
 * `reasons` holds one short code per refusal, parallel to `messages`.
 */
export class EntitiesValidationError extends Error {
   constructor(
      readonly messages: string[],
      readonly reasons: string[],
   ) {
      super(messages.join("\n"));
      this.name = "EntitiesValidationError";
   }
}

/** The reason code of an annotation the index refuses, or undefined for any other error. */
function refusalReason(error: unknown): string | undefined {
   if (error instanceof DimIndexRejectionError) return error.reason;
   if (error instanceof RequiredFilterMissingDimensionError)
      return "required_filter_missing_dimension";
   if (error instanceof RequiredFilterNonPublicDimensionError)
      return "required_filter_non_public_dimension";
   if (error instanceof DuplicateRequiredFilterError)
      return "duplicate_required_filter";
   return undefined;
}

function errorResult(
   relativePath: string,
   message?: string,
): ModelCompileResult {
   return {
      modelFilePath: relativePath,
      entities: [],
      sourceMetadataEntries: [],
      compileStatus: "ERROR",
      ...(message !== undefined && { errorMessage: message }),
   };
}

/** Extract a compiled file's entities and source metadata. */
async function extractModelFile(
   file: Extract<EntitiesModelFile, { model: Model }>,
   packageRoot: string,
   surface: PackageQuerySurface,
   connections: LookupConnection<InfoConnection>,
): Promise<ModelCompileResult> {
   const result = errorResult(file.relativePath);
   const compiledModel = file.model;
   const explores = compiledModel.explores;
   const source = file.text;
   const modelUrl = new URL("file://" + file.absolutePath);
   // `##(agent-hidden)` on the model file, in every bracket pair Malloy
   // routes: `#[agent-hidden]` routes exactly as `#(agent-hidden)` does.
   const isAgentHiddenTagPresentOnModelFile = compiledModel
      .getTaglines()
      .some((t) => /^##\|?\s*[([{<]?\s*agent-hidden\b/.test(t));

   const modelGivens = sourceExtraction.extractGivensFromModel(compiledModel);
   const modelAuthorize = sourceExtraction.extractAuthorizeFromModel(
      compiledModel,
      file.relativePath,
   );
   // The sources this server refuses to run a query against, read from the
   // same rule the query path enforces (see query_surface.ts).
   const nonQueryableSourceNames = nonQueryableSources(file, surface);
   const sourceMetadataEntries = sourceExtraction.extractMetadataFromExplores(
      explores,
      file.relativePath,
      isAgentHiddenTagPresentOnModelFile,
      modelUrl.href,
      modelGivens,
      modelAuthorize,
      nonQueryableSourceNames,
      source,
      packageRoot,
      servedFrom(file, surface),
   );

   // The `#(index)`-tagged dimensions. A non-public tagged field is skipped,
   // never refused: an inherited tag internalized via `internal: *` cannot be
   // removed in Malloy.
   const candidates = collectDimIndexCandidates(
      explores,
      file.relativePath,
      givenTypeMap(modelGivens),
   );
   const dimIndexFingerprints = await buildDimIndexFingerprintLookup({
      compiledModel,
      candidates,
      connections,
      modelFilePath: file.relativePath,
   });

   const entities = sourceExtraction.extractEntitiesFromExplores(
      explores,
      file.relativePath,
      packageRoot,
      source,
      dimIndexFingerprints,
      nonQueryableSourceNames,
   );
   result.entities = entities;
   result.sourceMetadataEntries = sourceMetadataEntries;
   result.sourceInfos = explores.map((explore) =>
      serializeExploreToSourceInfo(explore, source),
   );
   result.compileStatus = "SUCCESS";
   return result;
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
 * A fragment that fails to compile is logged and left out: that dimension
 * reports no fingerprint.
 */
async function buildDimIndexFingerprintLookup(args: {
   compiledModel: Model;
   candidates: DimIndexCandidate[];
   connections: LookupConnection<InfoConnection>;
   modelFilePath: string;
}): Promise<Map<string, DimIndexFingerprintInputs>> {
   const out = new Map<string, DimIndexFingerprintInputs>();
   const { candidates } = args;
   if (candidates.length === 0) return out;

   assertAtMostOneRequiredFilterPerIndexedDimension(
      candidates,
      args.modelFilePath,
   );
   assertNoRequiredParametersOnIndexedDimension(candidates, args.modelFilePath);

   const urlReader = new FileURLReader();
   for (const candidate of candidates) {
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
         logger.warn(
            `Failed to compile the index fingerprint query for ` +
               `${args.modelFilePath}::${candidate.sourceName}.${candidate.fieldName}; ` +
               `it reports no fingerprint`,
            { error: errorMessage(err) },
         );
         continue;
      }
      out.set(
         makeDimIndexLookupKey(candidate.sourceName, candidate.fieldName),
         {
            sqlFingerprint: computeSqlFingerprint(sql),
            requiredFilterDimensions: candidate.requiredFilterDimensions,
         },
      );
   }
   return out;
}
