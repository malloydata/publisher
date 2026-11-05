// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   type InfoConnection,
   type LookupConnection,
   Malloy,
   type Model,
} from "@malloydata/malloy";
import { logger } from "../../logger";
import { recordEntitiesDegraded } from "../../package_load_metrics";
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
import { splitSourcePath } from "./explore_walk";
import type { ModelCompileResult } from "./internal_types";
import { FileURLReader } from "./field_helpers";
import { withModelTextCache } from "./model_text";
import {
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
   /** The package's name, for the extraction's logs. */
   packageName?: string;
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

/** An annotation the extraction left out because an index cannot honor it. */
export interface EntitiesFinding {
   modelPath: string;
   message: string;
   /** A short code for the kind of annotation, for the load's warning and metric. */
   reason: string;
}

export interface PackageEntitiesExtraction {
   results: ModelCompileResult[];
   findings: EntitiesFinding[];
}

async function extractAll(
   input: ExtractPackageEntitiesInput,
): Promise<PackageEntitiesExtraction> {
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
         const detail = errMessage(error);
         logger.warn("Could not read a model's part of the query surface", {
            packageName: input.packageName,
            modelPath: file.relativePath,
            error: detail,
         });
         recordEntitiesDegraded("surface_read_failed");
         surfaceErrors.set(
            file.relativePath,
            `Error reading the query surface of ${file.relativePath}: ${detail}`,
         );
      },
   );
   const results: ModelCompileResult[] = [];
   const findings: EntitiesFinding[] = [];
   for (const file of input.modelFiles) {
      if (!("model" in file)) {
         results.push(errorResult(file.relativePath, file.error));
         continue;
      }
      const surfaceError = surfaceErrors.get(file.relativePath);
      if (surfaceError !== undefined) {
         results.push(
            errorResult(file.relativePath, surfaceError, "extraction"),
         );
         continue;
      }
      try {
         const extracted = await extractModelFile(
            file,
            input.packageRoot,
            surface,
            input.connections,
            input.packageName,
         );
         results.push(extracted.result);
         findings.push(...extracted.findings);
      } catch (error) {
         const message = errMessage(error);
         logger.warn("Could not extract a model's entities", {
            packageName: input.packageName,
            modelPath: file.relativePath,
            error: message,
         });
         recordEntitiesDegraded("file_extract_failed");
         results.push(errorResult(file.relativePath, message, "extraction"));
      }
   }
   return { results, findings };
}

/**
 * The package warning code for a finding's reason: one stable code per kind of
 * annotation, which a host that indexes can refuse a publish on.
 */
export function entitiesFindingCode(reason: string): string {
   return `entities-${reason.replaceAll("_", "-")}`;
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

/** Extract a compiled file's entities and source metadata. */
async function extractModelFile(
   file: Extract<EntitiesModelFile, { model: Model }>,
   packageRoot: string,
   surface: PackageQuerySurface,
   connections: LookupConnection<InfoConnection>,
   packageName: string | undefined,
): Promise<{ result: ModelCompileResult; findings: EntitiesFinding[] }> {
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

   // Every annotation the index refuses, so the load names them all.
   const refusals: Error[] = [];
   const refuse: Refuse = (error) => {
      refusals.push(error);
   };
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
      refuse,
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
         nonQueryableSourceNames,
      ),
   );
   const dimIndexFingerprints = await buildDimIndexFingerprintLookup({
      compiledModel,
      candidates,
      connections,
      modelFilePath: file.relativePath,
      packageName,
      refuse,
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
   result.givens = modelGivens;
   result.sourceMetadataEntries = sourceMetadataEntries;
   result.sourceInfos = explores.map((explore) =>
      serializeExploreToSourceInfo(explore, source),
   );
   result.compileStatus = "SUCCESS";
   delete result.errorKind;
   return {
      result,
      findings: refusals.map((error) => ({
         modelPath: file.relativePath,
         message: errMessage(error),
         reason: findingReason(error),
      })),
   };
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
   packageName: string | undefined;
   refuse: Refuse;
}): Promise<Map<string, DimIndexFingerprintInputs>> {
   const out = new Map<string, DimIndexFingerprintInputs>();
   const { candidates } = args;
   if (candidates.length === 0) return out;

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
         args.refuse(error);
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
         logger.warn(
            "Could not compile an #(index) fingerprint query; the dimension reports no fingerprint",
            {
               packageName: args.packageName,
               modelPath: args.modelFilePath,
               sourceName: candidate.sourceName,
               fieldName: candidate.fieldName,
               error: errMessage(err),
            },
         );
         recordEntitiesDegraded("fingerprint_failed");
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
