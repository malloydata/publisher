// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   GivenValue,
   LogMessage,
   Model as MalloyModel,
   ModelDef,
} from "@malloydata/malloy";
import { MalloyError, Runtime } from "@malloydata/malloy";

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { recordAuthorizeGuardRejection } from "../authorize_metrics";
import { NOTEBOOK_FILE_SUFFIX } from "../constants";
import {
   AccessDeniedError,
   BadRequestError,
   CompileRefusedError,
   ModelCompilationError,
   NotQueryableError,
   PackageManifestError,
   RenderTagRefusedError,
   ServiceUnavailableError,
   UnparseableTextError,
} from "../errors";
import { logger } from "../logger";
import { getPackageLoadPool } from "../package_load/package_load_pool";
import {
   assertSafePackageName,
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "../path_safety";
import { UNCODED_REPORT_FINDING } from "../report_metrics";
import { lazyCounter } from "../telemetry";
import { URL_READER } from "../utils";
import { assertNoCallerAuthorizeAnnotation } from "./authorize";
import type { CallerRegion } from "./caller_joins";
import { compileDocument, type CompiledDocument } from "./compile_document";
import {
   assertNoRenderTags,
   assertNoRestrictedConstructs,
   exceedsDocumentBudget,
} from "./compile_restriction";
import {
   assertFilterGivensParse,
   malloyGivenToApi,
   type MalloyGiven,
} from "./given";
import { Model } from "./model";
import { notebookReaderProblem } from "./notebook";
import { notebookLintProblems, reportedByDashboardLint } from "./notebook_lint";
import { Package } from "./package";
import { carriesArtifactTag, isReportEligiblePath } from "./report";
import { translatorMalloyError } from "./translator_error";

/**
 * Append-scope compile refusals, by reason.
 *
 * Labelled so a dependency outage is distinguishable from a caller sending
 * forbidden text. `restricted_construct` and `render_tag` (a URL-producing
 * render tag or markup in a label) are the caller's text;
 * `base_model_load_failed` is the named model failing to load.
 */
const getCompileRefusalsCounter = lazyCounter(
   "publisher_compile_refusals_total",
   "Compiles refused at append scope, labelled by reason and environment",
);

/**
 * What the submitted source means to /compile, and how far the check reaches.
 *
 * - "append" (the default): the source is appended to the target model and compiled in its namespace. Right for
 *   validating NEW definitions and queries; an edit to an existing definition
 *   collides ("Cannot redefine"), and diagnostics are positioned in the
 *   concatenated virtual file.
 * - "file": the source is compiled AS the target model file, replacing its
 *   on-disk content for this check. Right for validating an edit before
 *   saving; diagnostics land at true file coordinates.
 * - "package": a dry-run of every .malloy file in the package as saved —
 *   validation with reload's reach but none of its effects on the served
 *   model. An optional source replaces the target file's content, so
 *   importers compile against the edit ("what breaks if I save this?").
 */
export const COMPILE_SCOPES = ["append", "file", "package"] as const;
export type CompileScope = (typeof COMPILE_SCOPES)[number];

/** A compiler diagnostic tagged with the package-relative model it belongs
 *  to, resolvable from `at.url` — load-bearing at scope "package", where
 *  problems from every file share one array. */
export type TaggedLogMessage = LogMessage & { model?: string };

/** The package-relative model path of a file inside the package, `/`-separated on every platform; undefined outside it. */
export function packageRelativeModelPath(
   packagePath: string,
   filePath: string,
   pathModule: Pick<typeof path, "relative" | "isAbsolute" | "sep"> = path,
): string | undefined {
   const rel = pathModule.relative(packagePath, filePath);
   if (rel === "" || rel.startsWith("..") || pathModule.isAbsolute(rel)) {
      return undefined;
   }
   return rel.split(pathModule.sep).join("/");
}

/**
 * Run a /compile authorize gate, converting an access denial on a
 * boundary-hidden target into the boundary's generic 404.
 *
 * /compile is exempt from the query boundary so a curated package stays
 * authorable, but must not become an existence oracle: a 403 naming a source
 * that is both boundary-hidden and `#(authorize)`-gated proves it exists, where
 * the query surface answers a flat 404. Re-running the boundary on the denial
 * path only keeps "hidden is indistinguishable from nonexistent"; a target the
 * boundary does not hide keeps its informative 403.
 */
async function denyHiddenAsNotQueryable(
   convert: () => void | Promise<void>,
   gate: () => Promise<void>,
): Promise<void> {
   try {
      await gate();
   } catch (error) {
      if (error instanceof AccessDeniedError) {
         // The conversion must resolve the target at least as well as the gate
         // that denied it: the pre-compile text gate converts on surface
         // syntax, but the compiled gate must convert on the COMPILED run
         // target, or a multi-statement decoy / derivation alias keeps a 403
         // that names the hidden source. Each call site passes the matching
         // boundary check.
         await convert();
      }
      throw error;
   }
}

/** What /compile needs from the Environment it runs in. */
export interface CompileSourceHost {
   environmentName: string;
   environmentPath: string;
   withPackageLock<T>(packageName: string, fn: () => Promise<T>): Promise<T>;
   /** Load (or return) the package; the caller already holds its lock. */
   loadPackageLocked(packageName: string): Promise<Package>;
}

/** What /compile answers, at every scope. */
export type CompileSourceResult = {
   problems: TaggedLogMessage[];
   sql?: string;
   document?: CompiledDocument;
};

type ModelMaterializer = ReturnType<Runtime["loadModel"]>;
type QueryMaterializer = ReturnType<ModelMaterializer["loadFinalQuery"]>;

interface CompileRequest {
   packageName: string;
   modelName: string;
   source: string | undefined;
   includeSql: boolean;
   givens: Record<string, GivenValue> | undefined;
   scope: CompileScope;
}

/**
 * Everything the three scopes share, opened once under the package lock: the
 * paths, the virtual file and its text, the package, the authorize gate model
 * and the runtime that compiles against it.
 */
interface CompileContext extends CompileRequest {
   environmentName: string;
   modelPath: string;
   packagePath: string;
   virtualUrl: URL;
   virtualUri: string;
   /** The text served for the virtual file. */
   fullSource: string;
   /** Where the caller's own text sits in the compiled file ("append" only). */
   callerRegion: CallerRegion | undefined;
   pkg: Package;
   gateModel: Model | undefined;
   hasExactGateModel: boolean;
   boundManifestEntries: ReturnType<Package["getBuildManifestEntries"]>;
   runtime: Runtime;
   /** Append-scope text that may be a document, gated cell by cell. */
   documentCandidate: boolean;
   runEarlyGate: () => Promise<void>;
   tagProblems: (problems: LogMessage[]) => TaggedLogMessage[];
}

export async function compileSource(
   host: CompileSourceHost,
   packageName: string,
   modelName: string,
   source: string | undefined,
   includeSql: boolean = false,
   givens?: Record<string, GivenValue>,
   scope: CompileScope = "append",
): Promise<CompileSourceResult> {
   const request: CompileRequest = {
      packageName,
      modelName,
      source,
      includeSql,
      givens,
      scope,
   };
   assertValidCompileRequest(request);
   // Hold the per-package mutex across every disk read (the model file and
   // the imports `runtime.loadModel` resolves), which excludes install's
   // rename swap and delete's rename-to-retired, so a compile never observes
   // a half-rewritten tree.
   return host.withPackageLock(packageName, async () => {
      const ctx = await openCompileContext(host, request);
      if (scope === "package") return compilePackageDryRun(ctx);
      if (scope === "file") return compileFile(ctx);
      return compileAppend(ctx);
   });
}

/** The argument checks every scope runs before taking the package lock. */
function assertValidCompileRequest({
   packageName,
   modelName,
   source,
   includeSql,
   scope,
}: CompileRequest): void {
   assertSafePackageName(packageName);
   assertSafeRelativeModelPath(modelName);
   if (!COMPILE_SCOPES.includes(scope)) {
      throw new BadRequestError(
         `Invalid compile scope "${String(scope)}": expected one of ` +
            `${COMPILE_SCOPES.map((s) => `"${s}"`).join(", ")}.`,
      );
   }
   // Scope decides what `source` means, so it decides whether one is
   // required: "append" and "file" compile the submitted text (nothing to
   // do without it), while "package" is a dry-run of the files as saved and
   // takes source only as an optional what-if replacement for modelPath.
   if (source === undefined && scope !== "package") {
      throw new BadRequestError(
         `Compile scope "${scope}" requires a source to compile. ` +
            `Fix: pass the Malloy text in "source", or use scope "package" ` +
            `to validate the package's files as saved.`,
      );
   }
   if (scope === "package" && includeSql) {
      throw new BadRequestError(
         `includeSql is not available at scope "package": the dry-run has ` +
            `no single runnable query to extract SQL from. Fix: compile ` +
            `the runnable text at scope "append" or "file" instead.`,
      );
   }
   // The submitted source lands in the package's namespace, so an authorize
   // annotation in it would sit alongside — or displace — the author's. Same
   // rejection as the query path, on every scope.
   if (source !== undefined) {
      try {
         assertNoCallerAuthorizeAnnotation(source);
      } catch (err) {
         recordAuthorizeGuardRejection("compile_source");
         throw err;
      }
   }
   // A notebook (.malloynb) is markdown + cells, not a model, so compiling
   // against it only yields a confusing parse error.
   if (
      modelName.endsWith(NOTEBOOK_FILE_SUFFIX) &&
      (scope !== "package" || source !== undefined)
   ) {
      throw new BadRequestError(
         `Cannot compile against a notebook ("${modelName}"). ` +
            `/compile takes a .malloy model path.`,
      );
   }
}

/**
 * Resolve the paths and the virtual file, load the package, settle the
 * authorize gate model, run the early text gate (unless the text may be a
 * document, which is gated per cell) and build the runtime. The caller holds
 * the package lock.
 */
async function openCompileContext(
   host: CompileSourceHost,
   request: CompileRequest,
): Promise<CompileContext> {
   const { packageName, modelName, source, givens, scope } = request;
   // Sanitized join: input segments are allowlisted above; the
   // resolve-and-contain check here is the secondary guard CodeQL's
   // path-injection sanitizer recognises.
   const modelPath = safeJoinUnderRoot(
      host.environmentPath,
      packageName,
      modelName,
   );
   const packagePath = safeJoinUnderRoot(host.environmentPath, packageName);
   // "append": a virtual file beside the model (so relative imports resolve)
   // holding the model plus the source. "file" (and "package" with a
   // source): the virtual file IS modelPath, so importers compile against
   // the new text. `pathToFileURL`, not a hand-built `file://`: on Windows
   // that round-trips differently from the runtime's URL and breaks the
   // intercepting reader's string comparison.
   const modelDir = path.dirname(modelPath);
   const virtualUrl =
      scope === "append"
         ? pathToFileURL(path.join(modelDir, "__compile_check.malloy"))
         : pathToFileURL(modelPath);
   const virtualUri = virtualUrl.toString();

   const { fullSource, callerRegion } =
      scope === "append"
         ? await appendToSavedModel(modelPath, source, virtualUri)
         : { fullSource: source ?? "", callerRegion: undefined };

   // Serve the source for the virtual file and read everything else from
   // disk; at "package" with no source, nothing is substituted.
   const substitute = scope !== "package" || source !== undefined;
   const interceptingReader = {
      readURL: async (url: URL) => {
         if (substitute && url.toString() === virtualUri) {
            return fullSource;
         }
         return URL_READER.readURL(url);
      },
   };

   const pkg = await host.loadPackageLocked(packageName);

   const { gateModel, hasExactGateModel } = await resolveGateModel(
      pkg,
      request,
      modelPath,
      packagePath,
   );
   // A document is gated cell by cell, so one restricted cell does not refuse
   // the cells the caller may read. Text over the document budget is not
   // parsed to find out: naming `artifact` at all makes it a candidate, and
   // the budget refuses it below.
   const documentCandidate =
      scope === "append" &&
      source !== undefined &&
      (exceedsDocumentBudget(source)
         ? source.includes("artifact")
         : carriesArtifactTag(source));
   const runEarlyGate = async (): Promise<void> => {
      if (gateModel && hasExactGateModel && source !== undefined) {
         // Only the authorize gate (the *who* axis) applies to /compile, not
         // the query boundary (the *what* axis): gating the authoring loop
         // would make a curated package un-authorable. The boundary is
         // discovery curation, not access control; the accepted trade is
         // that /compile can reveal a non-exported source's schema (and,
         // with includeSql, SQL). Confidential sources use `#(authorize)`,
         // which applies here in full.
         await denyHiddenAsNotQueryable(
            () => {
               gateModel.assertQueryBoundaryEarly(undefined, undefined, source);
            },
            () =>
               gateModel.assertAuthorizedForText(source, givens ?? {}, {
                  // File and package scope compile a whole author file: a
                  // locked name that is not the run statement must not
                  // refuse it, and its joins are author joins.
                  wholeFile: scope !== "append",
               }),
         );
      }
   };
   if (!documentCandidate) await runEarlyGate();

   // The package's MalloyConfig (borrowed; the package lifecycle releases it)
   // and bound build manifest, so compile sees the same duckdb and the same
   // persist routing as execution — otherwise includeSql would show
   // base-table SQL that diverges from what runs.
   const boundManifestEntries = pkg.getBuildManifestEntries();
   const runtime = new Runtime({
      urlReader: interceptingReader,
      config: pkg.getMalloyConfig(),
      buildManifest: boundManifestEntries
         ? { entries: boundManifestEntries, strict: false }
         : undefined,
      // The package's own cache, so an unchanged import is not translated again.
      cacheManager: pkg.compileCacheManagerFor(virtualUrl),
   });

   // Tag each diagnostic with the package-relative model `at.url` points at;
   // the append-mode virtual file reports as the model it extends.
   const tagProblems = (problems: LogMessage[]): TaggedLogMessage[] =>
      problems.map((problem) => {
         const url = (problem as { at?: { url?: string } }).at?.url;
         let model: string | undefined;
         if (url && url.startsWith("file:")) {
            try {
               model = packageRelativeModelPath(
                  packagePath,
                  fileURLToPath(url),
               );
            } catch {
               // Not a resolvable file URL — leave the tag off.
            }
         }
         if (scope === "append" && url === virtualUri) {
            model = modelName;
         }
         return model !== undefined ? { ...problem, model } : problem;
      });

   return {
      ...request,
      environmentName: host.environmentName,
      modelPath,
      packagePath,
      virtualUrl,
      virtualUri,
      fullSource,
      callerRegion,
      pkg,
      gateModel,
      hasExactGateModel,
      boundManifestEntries,
      runtime,
      documentCandidate,
      runEarlyGate,
      tagProblems,
   };
}

/**
 * The append-scope virtual file: the saved model with the caller's text
 * after it, and where that text starts.
 */
async function appendToSavedModel(
   modelPath: string,
   source: string | undefined,
   virtualUri: string,
): Promise<{ fullSource: string; callerRegion: CallerRegion }> {
   let modelContent = "";
   try {
      modelContent = await fs.promises.readFile(modelPath, "utf8");
   } catch {
      // Empty content; a missing model is refused by `loadAppendBase`.
   }
   const fullSource = modelContent
      ? `${modelContent}\n${source}`
      : (source ?? "");
   // Checked again in context: a saved model ending in an open block note
   // re-lexes the caller's prose. Both checks are load-bearing.
   if (source !== undefined && modelContent) {
      try {
         assertNoCallerAuthorizeAnnotation(source, `${modelContent}\n`);
      } catch (err) {
         recordAuthorizeGuardRejection("compile_source");
         throw err;
      }
   }
   // Where the caller's own text starts in the compiled file, so its joins
   // are gated as the query path gates them. "file" and "package" have
   // none: the whole text is the author's file.
   const callerRegion: CallerRegion = {
      kind: "span",
      url: virtualUri,
      // 0-based: the appended text starts on the line after the model's.
      fromLine: modelContent ? modelContent.split("\n").length : 0,
      text: source ?? "",
   };
   return { fullSource, callerRegion };
}

/**
 * The model whose `given:` block and authorize annotations gate the
 * submitted text, and whether it is the target file's own namespace.
 */
async function resolveGateModel(
   pkg: Package,
   { packageName, modelName, source }: CompileRequest,
   modelPath: string,
   packagePath: string,
): Promise<{ gateModel: Model | undefined; hasExactGateModel: boolean }> {
   // /compile can act as a schema oracle (columns in compile errors) and,
   // with includeSql, leak SQL, so the locks the text names are decided
   // BEFORE compiling against the package's cached Model, as on the query
   // path. A new model path has no cached Model; the compiled backstop then
   // evaluates gates carried by the runnable's own ModelDef.
   const { model, exact } = pkg.getCompileAuthorizationModel(modelName);
   // A file on disk but not in the cached package (added since the last
   // reload) is compiled once as an ephemeral gate model, so its givens and
   // authorize annotations come from the right namespace.
   if (!exact && source !== undefined) {
      const diskTarget = await fs.promises
         .stat(modelPath)
         .catch(() => undefined);
      if (diskTarget?.isFile()) {
         return {
            gateModel: await Model.create(
               packageName,
               packagePath,
               modelName,
               pkg.getMalloyConfig(),
               {
                  buildManifest: pkg.getBuildManifestEntries(),
               },
            ),
            hasExactGateModel: true,
         };
      }
   }
   return { gateModel: model, hasExactGateModel: exact };
}

/**
 * The authorize gate on a compiled runnable: the gate model's own namespace
 * when it is the target file's, otherwise the gates the runnable's ModelDef
 * carries. A denial on a target the boundary hides answers as not queryable.
 */
function gateCompiledRunnable(
   ctx: CompileContext,
   gateModel: Model,
   runnable: QueryMaterializer,
   callerRegion?: CallerRegion,
): Promise<void> {
   const { hasExactGateModel, source, givens } = ctx;
   return denyHiddenAsNotQueryable(
      () => {
         if (!hasExactGateModel) {
            throw new NotQueryableError("Query target is not queryable.");
         }
         return gateModel.assertCompiledTargetQueryable(runnable, source);
      },
      () =>
         hasExactGateModel
            ? gateModel.assertAuthorizedForRunnable(
                 runnable,
                 givens ?? {},
                 callerRegion,
              )
            : // No region: this gate model is another file's
              // namespace, so its source names cannot place a join.
              gateModel.assertAuthorizedFromCompiledRunnable(
                 runnable,
                 givens ?? {},
              ),
   );
}

/**
 * Scope "file": the source is compiled AS the target model file. The file
 * is the author's, so its data roots are not gated (see compileAppend).
 */
function compileFile(ctx: CompileContext): Promise<CompileSourceResult> {
   return compileVirtualFile(ctx);
}

/**
 * Scope "append": the source is a fragment compiled in the target model's
 * namespace, contained to the surface the author published, and read as a
 * document when it carries an artifact tag.
 */
async function compileAppend(
   ctx: CompileContext,
): Promise<CompileSourceResult> {
   const { source, documentCandidate } = ctx;
   // Containment: an append FRAGMENT has no legitimate need for its own data
   // roots, and Malloy resolves schemas at compile time, so an unrestricted
   // one reaches the connection, filesystem and network without running a
   // query. "file" and "package" are NOT gated: there the source IS the model
   // file, and `import`/`connection.table(...)` are how a model declares what
   // it reads.
   const { appendBase, baseModel } = await loadAppendBase(ctx);
   // The fragment ALONE, against the compiled base model: passing the
   // concatenation would yield `Cannot redefine` for every source and abort
   // before the fragment is classified — a bypass. A continuation fragment
   // is a syntax error alone, which the gate refuses rather than approving.
   // A document's whole-text gate runs after the per-cell access phase.
   if (!documentCandidate) {
      await refuseConstructs(ctx, baseModel, source ?? "", false);
   }

   if (documentCandidate && source !== undefined) {
      const result = await compileAppendDocument(
         ctx,
         appendBase,
         baseModel,
         source,
      );
      if (result) return result;
      // Not a readable document after all, so the ordinary compile runs and the whole text is judged as one.
      await refuseConstructs(ctx, baseModel, source, false);
      await ctx.runEarlyGate();
   }

   return compileVirtualFile(ctx);
}

/**
 * The model the append-scope fragment is judged against, loaded once for
 * the gate and for a document.
 */
async function loadAppendBase(
   ctx: CompileContext,
): Promise<{ appendBase: ModelMaterializer; baseModel: MalloyModel }> {
   const { runtime, modelPath, modelName, environmentName } = ctx;
   // The model as saved, WITHOUT the caller's text, so the caller cannot
   // widen the namespace it is judged against.
   //
   // `name!type(...)` and the `sql_*` family are classified only inside a
   // resolved FieldSpace; with no base model `run: base_source -> {…}` never
   // resolves and the construct is never classified. So a base model that
   // will not load fails the request rather than lowering the gate.
   try {
      const appendBase = runtime.loadModel(pathToFileURL(modelPath));
      const baseModel = await appendBase.getModel();
      return { appendBase, baseModel };
   } catch (error) {
      // The detail stays server-side: returning the absolute `modelPath`
      // would answer "does this file exist and is it readable" for any path
      // a caller names. `warn`, not `error`: a caller typo reaches here, and
      // an unauthenticated 400 must not emit ERROR at a caller-chosen rate.
      logger.warn("Compile gate could not load the base model", {
         modelPath,
         error,
      });
      getCompileRefusalsCounter().add(1, {
         environment: environmentName,
         reason: "base_model_load_failed",
      });
      if (error instanceof MalloyError) {
         // One 400 for every MalloyError, as at `file`/`package` scope. Do
         // not split out a 503 on `failed-to-fetch-table-schema`: a missing
         // table produces that code too, and a down warehouse can surface as
         // `invalid-sql-source`. Malloy has no code meaning "unreachable".
         throw new CompileRefusedError(
            `Cannot validate the submitted source: the model ` +
               `"${modelName}" does not compile, so there is nothing ` +
               `to check the submitted source against. Problems: ` +
               error.problems.map((p) => p.message).join("; "),
         );
      }
      // Missing file, permission, anything else: the caller named a
      // model this server cannot load, which is theirs to correct.
      throw new CompileRefusedError(
         `Cannot validate the submitted source: the model ` +
            `"${modelName}" could not be loaded to check it against.`,
      );
   }
}

// Only a refusal is counted; anything else the gate rethrows is an infrastructure failure.
function countRefusal(environmentName: string, error: unknown): void {
   // An unparseable tile is a compile problem for the document, not a refusal.
   if (
      error instanceof CompileRefusedError &&
      !(error instanceof UnparseableTextError)
   ) {
      getCompileRefusalsCounter().add(1, {
         environment: environmentName,
         reason:
            error instanceof RenderTagRefusedError
               ? "render_tag"
               : "restricted_construct",
      });
   }
}

async function refuseConstructs(
   ctx: CompileContext,
   baseModel: MalloyModel,
   text: string,
   renderTags: boolean,
): Promise<void> {
   try {
      await assertNoRestrictedConstructs(ctx.runtime, baseModel, text, {
         renderTags,
      });
   } catch (error) {
      countRefusal(ctx.environmentName, error);
      throw error;
   }
}

function refuseRenderTags(ctx: CompileContext, text: string): void {
   try {
      assertNoRenderTags(text);
   } catch (error) {
      countRefusal(ctx.environmentName, error);
      throw error;
   }
}

/**
 * Compile append-scope text as a document, gating each cell and tile.
 * Undefined when the text is not a readable document after all.
 */
async function compileAppendDocument(
   ctx: CompileContext,
   base: ModelMaterializer,
   baseModel: MalloyModel,
   source: string,
): Promise<CompileSourceResult | undefined> {
   const { gateModel: gate, hasExactGateModel: exact, givens } = ctx;
   // Syntactic and cheap, so it runs ahead of the cell reader; the compile-based gate runs once access is settled.
   refuseRenderTags(ctx, source);
   const result = await compileDocument({
      base,
      source,
      modelName: ctx.modelName,
      gates: {
         text: (text) =>
            gate && exact
               ? denyHiddenAsNotQueryable(
                    () => {
                       gate.assertQueryBoundaryEarly(
                          undefined,
                          undefined,
                          text,
                       );
                    },
                    () => gate.assertAuthorizedForText(text, givens ?? {}),
                 )
               : Promise.resolve(),
         compiled: (runnable) =>
            gate
               ? denyHiddenAsNotQueryable(
                    () => gate.assertCompiledTargetQueryable(runnable),
                    () =>
                       exact
                          ? gate.assertAuthorizedForRunnable(
                               runnable,
                               givens ?? {},
                            )
                          : gate.assertAuthorizedFromCompiledRunnable(
                               runnable,
                               givens ?? {},
                            ),
                 )
               : Promise.resolve(),
         constructs: (text) => refuseConstructs(ctx, baseModel, text, true),
         document: (text) => refuseConstructs(ctx, baseModel, text, false),
         nameVisible: (query, definitions) => {
            gate?.assertTextNameVisible(query, definitions);
         },
         boundaryCompiled: async (
            runnable,
            compiledSource,
            query,
            definitions,
         ) => {
            gate?.assertQueryBoundaryCompiled(
               compiledSource,
               query,
               definitions,
            );
            await gate?.assertDocumentJoinsQueryable(
               `${definitions}\n${query}`,
               runnable,
            );
         },
         boundary: async (query, definitions) => {
            gate?.assertQueryBoundaryEarly(
               undefined,
               undefined,
               query,
               definitions,
            );
            await gate?.assertDocumentJoinsQueryable(
               `${definitions}\n${query}`,
            );
         },
      },
   });
   if (!result) return undefined;
   return {
      problems: result.problems.map((problem) => ({
         ...problem,
      })) as TaggedLogMessage[],
      ...(result.document && { document: result.document }),
   };
}

/**
 * Compile the virtual file ("append" and "file" scope): the compiled
 * authorize backstop, the optional SQL, and the diagnostics.
 */
async function compileVirtualFile(
   ctx: CompileContext,
): Promise<CompileSourceResult> {
   const {
      runtime,
      virtualUrl,
      virtualUri,
      gateModel,
      includeSql,
      givens,
      modelName,
      fullSource,
      scope,
      tagProblems,
   } = ctx;
   try {
      const modelMaterializer = runtime.loadModel(virtualUrl);
      const model = await modelMaterializer.getModel();

      // Resolve the final query's materializer once (if there is one).
      let queryMaterializer: QueryMaterializer | null = null;
      try {
         queryMaterializer = modelMaterializer.loadFinalQuery();
      } catch {
         // No runnable query: nothing to gate or extract, and with no
         // runnable there is no result and no SQL to expose.
      }

      // Compiled authorize backstop, REGARDLESS of includeSql: compiling a
      // gated source is a schema oracle even without SQL. It gates what the
      // COMPILED final query reads, closing named-query and derivation
      // indirection the early text gate cannot see. (No boundary backstop:
      // /compile is exempt by design.) NOT guarded by hasAuthorize(), which
      // reads only top-level sources' own annotations and would skip a
      // target gated solely by what it derives from. (A `source: x is gated`
      // alias carries the gate: only its OWN `#(authorize)` replaces it, and
      // caller text may not declare one.)
      if (queryMaterializer && gateModel) {
         await gateCompiledRunnable(
            ctx,
            gateModel,
            queryMaterializer,
            ctx.callerRegion,
         );
      }

      let sql: string | undefined;
      if (includeSql && queryMaterializer) {
         // A given value the compiled text's own filter types cannot read is
         // a 400, as on the query route; checked against the submitted text's
         // givens, since this is the only place /compile binds values.
         assertFilterGivensParse(
            Array.from(model.givens.values()).map((g) =>
               malloyGivenToApi(g as MalloyGiven),
            ),
            givens,
         );
         try {
            sql = await queryMaterializer.getSQL({ givens });
         } catch (error) {
            // A bad caller given surfaces as a Malloy `runtime-given-*`
            // error: a 400, not a silently omitted `sql`. The prefix is
            // pinned to Malloy's codes (as in model.ts); if renamed upstream
            // a bad given would fall through to the omit branch.
            const givenCode = (error as { code?: string })?.code;
            if (
               typeof givenCode === "string" &&
               givenCode.startsWith("runtime-given-")
            ) {
               throw new BadRequestError(
                  error instanceof Error ? error.message : String(error),
               );
            }
            if (error instanceof MalloyError) {
               throw error;
            }
            // Otherwise there may be no runnable query: omit `sql`.
         }
      }

      const readerProblem = notebookReaderProblem(
         modelName,
         fullSource,
         model._modelDef,
         virtualUri,
      );
      // Its positions are in the concatenated file at "append", so the lint is for a whole file only.
      const lintProblems =
         scope === "append"
            ? []
            : notebookLintProblems(modelName, fullSource, virtualUri);
      return {
         problems: tagProblems([
            ...model.problems,
            ...(readerProblem ? [readerProblem] : []),
            ...lintProblems,
         ]),
         sql,
      };
   } catch (thrown) {
      // The translator's plain Error is a compile problem, not a server fault.
      const error = translatorMalloyError(thrown) ?? thrown;
      if (error instanceof MalloyError) {
         return {
            problems: tagProblems([
               ...error.problems,
               ...(scope === "append"
                  ? []
                  : notebookLintProblems(modelName, fullSource, virtualUri)),
            ]),
         };
      }
      throw error;
   }
}

/**
 * Scope "package": a dry-run of every file in the package through the
 * reload worker, plus the findings a reload adds on the main thread. Nothing
 * is swapped into the served package.
 */
async function compilePackageDryRun(
   ctx: CompileContext,
): Promise<CompileSourceResult> {
   const {
      source,
      modelName,
      packageName,
      packagePath,
      pkg,
      boundManifestEntries,
   } = ctx;
   await gateReplacementOnMainThread(ctx);
   const outcome = await loadPackageInWorker(ctx);

   // Package scope intentionally reports diagnostics from every model
   // the reload compiler sees, including files hidden from discovery.
   // It returns no rows or SQL; authorize still gates caller text.
   const { collect, problems } = problemCollector(ctx.tagProblems);
   // The findings a reload adds on the main thread (render tags; report,
   // given and drill lints). Read first: the per-file text lint below drops
   // a finding the report lint already reported.
   const { renderTagWarnings, reportWarnings } =
      await Package.lintWorkerOutcome(
         ctx.environmentName,
         packageName,
         packagePath,
         pkg.getMalloyConfig(),
         outcome,
         boundManifestEntries,
         source === undefined ? undefined : { modelPath: modelName, source },
      );
   for (const compiled of outcome.models) {
      if (compiled.problems) {
         collect(compiled.problems as LogMessage[], compiled.modelPath);
      }
      const readerProblem =
         compiled.modelDef && compiled.modelSourceText !== undefined
            ? notebookReaderProblem(
                 compiled.modelPath,
                 compiled.modelSourceText,
                 compiled.modelDef as ModelDef,
                 pathToFileURL(
                    path.join(packagePath, compiled.modelPath),
                 ).toString(),
              )
            : undefined;
      if (readerProblem) collect([readerProblem], compiled.modelPath);
      if (compiled.compilationError) {
         const compilerProblems = compiled.compilationError.malloyProblems;
         if (compilerProblems) {
            collect(compilerProblems as LogMessage[], compiled.modelPath);
         } else {
            collect(
               [
                  {
                     severity: "error",
                     message: compiled.compilationError.message,
                  } as LogMessage,
               ],
               compiled.modelPath,
            );
         }
      }
      // Any report-eligible file; one that did not compile carries no text
      // back, so it is read as saved (or as replaced).
      const lintText = !(
         compiled.modelPath.endsWith(".malloy") &&
         isReportEligiblePath(compiled.modelPath)
      )
         ? undefined
         : (compiled.modelSourceText ??
           (compiled.modelPath === modelName && source !== undefined
              ? source
              : await fs.promises
                   .readFile(path.join(packagePath, compiled.modelPath), "utf8")
                   .catch(() => undefined)));
      if (lintText !== undefined) {
         collect(
            notebookLintProblems(
               compiled.modelPath,
               lintText,
               pathToFileURL(
                  path.join(packagePath, compiled.modelPath),
               ).toString(),
            ).filter(
               (problem) =>
                  !reportedByDashboardLint(
                     problem,
                     compiled.modelPath,
                     reportWarnings,
                  ),
            ),
            compiled.modelPath,
         );
      }
   }
   for (const warning of renderTagWarnings) {
      collect([warningAsProblem(warning, "render-tag")], warning.model);
   }
   for (const warning of reportWarnings) {
      collect(
         [warningAsProblem(warning, warning.code ?? UNCODED_REPORT_FINDING)],
         warning.model,
      );
   }
   if (source !== undefined && outcome.replacementMatchedExisting === false) {
      collect(
         [
            {
               severity: "warn",
               message:
                  `No existing package file exactly matched ` +
                  `"${modelName}"; the source was validated as a new ` +
                  `file and did not replace another model.`,
            } as LogMessage,
         ],
         modelName,
      );
   }
   return { problems };
}

/**
 * Gate the caller's replacement once on the main thread. The full
 * package compile runs in the load worker, but authorization
 * probes use the package's live connection/config and must remain
 * on this side of the worker boundary.
 */
async function gateReplacementOnMainThread(ctx: CompileContext): Promise<void> {
   const { source, gateModel, runtime, virtualUrl } = ctx;
   if (source === undefined || !gateModel) return;
   try {
      const materializer = runtime.loadModel(virtualUrl);
      await materializer.getModel();
      let finalQuery: QueryMaterializer | null = null;
      try {
         finalQuery = materializer.loadFinalQuery();
      } catch {
         // No runnable query in the replacement text.
      }
      if (finalQuery) {
         await gateCompiledRunnable(ctx, gateModel, finalQuery);
      }
   } catch (error) {
      // Compiler diagnostics come back from the worker; authorization
      // denials propagate.
      if (!(error instanceof MalloyError) && !translatorMalloyError(error)) {
         throw error;
      }
   }
}

/**
 * Use the exact worker path a reload uses: dotfiles are ignored,
 * both .malloy and .malloynb files are compiled, CPU work is kept
 * off the event loop, and the worker's timeout bounds the request.
 * This does not swap the returned models into the served package.
 */
async function loadPackageInWorker(ctx: CompileContext) {
   const { source, modelName } = ctx;
   try {
      return await getPackageLoadPool().loadPackage({
         packagePath: ctx.packagePath,
         packageName: ctx.packageName,
         environmentName: ctx.environmentName,
         malloyConfig: ctx.pkg.getMalloyConfig(),
         defaultConnectionName: "duckdb",
         buildManifest: ctx.boundManifestEntries,
         collectProblems: true,
         connectionsEpoch: ctx.pkg.getConnectionsEpoch(),
         replacement:
            source === undefined ? undefined : { modelPath: modelName, source },
      });
   } catch (error) {
      // Same split as Package.loadViaWorker: compile errors and an
      // unusable publisher.json keep their 4xx mapping, and only an
      // infrastructure failure reads as a worker outage.
      if (
         error instanceof MalloyError ||
         error instanceof ModelCompilationError ||
         error instanceof PackageManifestError
      ) {
         throw error;
      }
      throw new ServiceUnavailableError(
         `Package compile worker unavailable: ${
            error instanceof Error ? error.message : String(error)
         }`,
      );
   }
}

/**
 * One problems array for the package dry-run, tagged and de-duplicated by
 * model, position, severity and message.
 */
function problemCollector(
   tagProblems: (problems: LogMessage[]) => TaggedLogMessage[],
): {
   problems: TaggedLogMessage[];
   collect: (batch: LogMessage[], fallbackModel?: string) => void;
} {
   const seen = new Set<string>();
   const problems: TaggedLogMessage[] = [];
   const collect = (batch: LogMessage[], fallbackModel?: string): void => {
      for (const problem of tagProblems(batch)) {
         const problemUrl = (problem as { at?: { url?: string } }).at?.url;
         const tagged =
            problem.model === undefined &&
            problemUrl === undefined &&
            fallbackModel !== undefined
               ? { ...problem, model: fallbackModel }
               : problem;
         const start = (
            tagged as {
               at?: {
                  range?: {
                     start?: { line?: number; character?: number };
                  };
               };
            }
         ).at?.range?.start;
         const key = `${tagged.model ?? problemUrl ?? ""}|${
            start?.line ?? -1
         }|${start?.character ?? -1}|${tagged.severity}|${tagged.message}`;
         if (seen.has(key)) continue;
         seen.add(key);
         problems.push(tagged);
      }
   };
   return { problems, collect };
}

// Keeps its own severity, so a broken dashboard makes the compile an error.
// No position, so the subject leads the message; otherwise two views with
// the same finding would de-duplicate into one.
function warningAsProblem(
   warning: Awaited<
      ReturnType<typeof Package.lintWorkerOutcome>
   >["renderTagWarnings"][number],
   code: string,
): LogMessage {
   return {
      severity: warning.severity ?? "warn",
      message: warning.subject
         ? `${warning.subject}: ${warning.message}`
         : warning.message,
      code,
   } as LogMessage;
}
