// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   GivenValue,
   LogMessage,
   Model as MalloyModel,
   ModelDef,
} from "@malloydata/malloy";
import { MalloyError, Runtime } from "@malloydata/malloy";
import { type Counter } from "@opentelemetry/api";
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
import { publisherMeter } from "../telemetry";
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

let compileRefusalsCounter: Counter | null = null;
/**
 * Append-scope compile refusals, by reason.
 *
 * The reasons answer 4xx or 5xx on the same endpoint, so without the label a
 * dependency outage and a caller sending forbidden text are one indistinguishable
 * spike -- and the one that needs paging looks like the one that does not.
 * `restricted_construct` is the caller's text; `base_model_load_failed` is the
 * named model failing to load, which includes the schema-fetch case that answers
 * 503. `render_tag` is a document carrying a URL-producing render tag or markup
 * in a label, also the caller's text but a different fix than a data root.
 */
function getCompileRefusalsCounter(): Counter {
   if (compileRefusalsCounter) return compileRefusalsCounter;
   compileRefusalsCounter = publisherMeter().createCounter(
      "publisher_compile_refusals_total",
      {
         description:
            "Compiles refused at append scope, labelled by reason and environment",
      },
   );
   return compileRefusalsCounter;
}

/** Visible for tests; resets the lazy counter so a fresh MeterProvider sees future writes. */
export function resetCompileTelemetryForTesting(): void {
   compileRefusalsCounter = null;
}

/**
 * Run a /compile authorize gate, converting an access denial on a
 * boundary-hidden target into the boundary's generic 404.
 *
 * /compile is exempt from the query boundary so a curated package stays
 * authorable, but the exemption must not turn /compile into an existence
 * oracle. Without this, an unauthorized caller probing a source that is both
 * boundary-hidden and `#(authorize)`-gated gets a 403 naming it — proof the
 * source exists — where the query surface answers a flat 404, letting the
 * hidden namespace be enumerated one guess at a time. Re-running the boundary
 * on the denial path only (it throws for a hidden target and otherwise returns
 * without effect) restores "hidden is indistinguishable from nonexistent" while
 * leaving compile itself ungated: a target the boundary does not hide keeps its
 * informative 403.
 */
/**
 * What the submitted source means to /compile, and how far the check reaches.
 *
 * - "append" (the default, and the historical behavior): the source is
 *   appended to the target model and compiled in its namespace. Right for
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

export async function compileSource(
   host: CompileSourceHost,
   packageName: string,
   modelName: string,
   source: string | undefined,
   includeSql: boolean = false,
   givens?: Record<string, GivenValue>,
   scope: CompileScope = "append",
): Promise<{
   problems: TaggedLogMessage[];
   sql?: string;
   document?: CompiledDocument;
}> {
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
   // The submitted source lands in the package's namespace (appended to the
   // target model, or replacing a file wholesale), so an authorize
   // annotation in it would sit alongside — or displace — the author's.
   // Same rejection as the query path on every scope, and `includeSql`
   // makes this door the more valuable one to an attacker.
   if (source !== undefined) {
      try {
         assertNoCallerAuthorizeAnnotation(source);
      } catch (err) {
         recordAuthorizeGuardRejection("compile_source");
         throw err;
      }
   }
   // /compile interprets modelPath as a .malloy model (namespace context at
   // "append", the file being written at "file"/"package"-with-source). A
   // notebook (.malloynb) is markdown + cells, not a model, so compiling
   // against it only yields a confusing parse error — reject it up front
   // with an actionable message. (Notebooks remain public for
   // discovery/query; this is specific to the compile context.)
   if (
      modelName.endsWith(NOTEBOOK_FILE_SUFFIX) &&
      (scope !== "package" || source !== undefined)
   ) {
      throw new BadRequestError(
         `Cannot compile against a notebook ("${modelName}"). ` +
            `/compile takes a .malloy model path.`,
      );
   }
   // Hold the per-package mutex for the duration of every disk read —
   // both the explicit `fs.readFile(modelPath)` below and the implicit
   // import resolution that `runtime.loadModel` does through the URL
   // reader. This is mutually exclusive with `installPackage`'s Phase 2
   // rename swap and with `deletePackage`'s rename-to-retired, so a
   // compile can never observe a half-rewritten tree. The slow Phase 1
   // download happens outside this lock, so a multi-second clone does
   // not block compiles.
   return host.withPackageLock(packageName, async () => {
      // Sanitized join: input segments are allowlisted above; the
      // resolve-and-contain check here is the secondary guard CodeQL's
      // path-injection sanitizer recognises.
      const modelPath = safeJoinUnderRoot(
         host.environmentPath,
         packageName,
         modelName,
      );
      const packagePath = safeJoinUnderRoot(host.environmentPath, packageName);
      // Where the compiled text lives, by scope. "append": a virtual file
      // in the model's directory (so relative imports resolve) holding the
      // model's content with the source appended — the historical behavior,
      // whose diagnostics are positioned in the CONCATENATED file. "file"
      // (and "package" with a source): the virtual file IS modelPath, so
      // the submitted text replaces the on-disk copy, diagnostics land at
      // true file coordinates, and — at "package" — every importer compiles
      // against the new text. Use `pathToFileURL` rather than hand-prefixing
      // `file://`: on Windows the latter produces a malformed URL
      // (`file://D:\Temp\…`) that round-trips differently than the URL the
      // Malloy runtime synthesizes from the same path, breaking the
      // intercepting reader's string comparison below and falling through
      // to disk for a virtual file that doesn't exist.
      const modelDir = path.dirname(modelPath);
      const virtualUrl =
         scope === "append"
            ? pathToFileURL(path.join(modelDir, "__compile_check.malloy"))
            : pathToFileURL(modelPath);
      const virtualUri = virtualUrl.toString();

      let fullSource = source ?? "";
      // Where the caller's own text starts in the compiled file, so its joins
      // are gated as the query path gates them. "file" and "package" have
      // none: the whole text is the author's file.
      let callerRegion: CallerRegion | undefined;
      if (scope === "append") {
         // Read the full model file so the submitted source inherits the
         // model's complete namespace — imports, source definitions,
         // queries, etc.
         let modelContent = "";
         try {
            modelContent = await fs.promises.readFile(modelPath, "utf8");
         } catch {
            // Empty content here, and compilation reports the problem. Note
            // this fallback no longer decides the missing-model case on its
            // own at `append` scope: the restricted-construct gate below
            // loads the same model to check the caller's text against, and
            // refuses when it cannot, so a model that does not exist is
            // rejected there before this leniency can apply.
         }
         fullSource = modelContent
            ? `${modelContent}\n${source}`
            : (source ?? "");
         // Checked again where the compiler reads it: a saved model ending in an open block note re-lexes the caller's prose.
         // Both checks are load-bearing: this in-context form accepts text the stand-alone check above refuses.
         if (source !== undefined && modelContent) {
            try {
               assertNoCallerAuthorizeAnnotation(source, `${modelContent}\n`);
            } catch (err) {
               recordAuthorizeGuardRejection("compile_source");
               throw err;
            }
         }
         callerRegion = {
            kind: "span",
            url: virtualUri,
            // 0-based: the appended text starts on the line after the model's.
            fromLine: modelContent ? modelContent.split("\n").length : 0,
            text: source ?? "",
         };
      }

      // Create a URL Reader that serves the source string for the virtual
      // file, but falls back to the disk for everything else (imports). At
      // scope "package" with no source there is nothing to substitute and
      // every file reads from disk as saved.
      const substitute = scope !== "package" || source !== undefined;
      const interceptingReader = {
         readURL: async (url: URL) => {
            if (substitute && url.toString() === virtualUri) {
               return fullSource;
            }
            return URL_READER.readURL(url);
         },
      };

      // Use the locked variant — we already hold the per-package mutex.
      const pkg = await host.loadPackageLocked(packageName);

      // Authorize gate: /compile is compile-only, but it can still act
      // as a schema oracle (a denied caller learns a gated source's columns
      // from compile errors) and, with includeSql, leak its SQL. Decide the
      // locks the submitted text names BEFORE compiling — mirrors the query
      // path's early gate (see `assertAuthorizedForText` for what each scope
      // reads); the compiled backstop below settles a target this cannot
      // name. The gate runs against the package's cached Model (its
      // `given:` block + authorize annotations), independent of the virtual
      // compile below. A new model path has no cached Model, so its early
      // surface-syntax gate cannot run; the compiled backstop below instead
      // evaluates gates carried by the runnable's own ModelDef.
      let { model: gateModel, exact: hasExactGateModel } =
         pkg.getCompileAuthorizationModel(modelName);
      // A file can exist on disk without being in the cached package model
      // (for example, it was added after the last reload). Compile that
      // author's file once as an ephemeral gate model so file-level givens
      // and authorize annotations come from the correct namespace. A truly
      // new path still falls back to the compiled-runnable gate below.
      if (!hasExactGateModel && source !== undefined) {
         const diskTarget = await fs.promises
            .stat(modelPath)
            .catch(() => undefined);
         if (diskTarget?.isFile()) {
            gateModel = await Model.create(
               packageName,
               packagePath,
               modelName,
               pkg.getMalloyConfig(),
               {
                  buildManifest: pkg.getBuildManifestEntries(),
               },
            );
            hasExactGateModel = true;
         }
      }
      // A document is gated cell by cell and tile by tile below, so one
      // restricted cell does not refuse the cells the caller may read.
      // Text over a document's annotation budget is not parsed to find
      // out: when it names `artifact` at all it is taken for a document, and
      // the budget refuses it below.
      const documentCandidate =
         scope === "append" &&
         source !== undefined &&
         (exceedsDocumentBudget(source)
            ? source.includes("artifact")
            : carriesArtifactTag(source));
      const runEarlyGate = async (): Promise<void> => {
         if (gateModel && hasExactGateModel && source !== undefined) {
            // Only the authorize gate (the *who* axis) applies to /compile.
            // The query boundary (`explores`/`queryableSources`, the *what*
            // axis) deliberately does NOT: compile is the authoring loop
            // (validate -> save -> reload), and gating it would make a curated
            // package un-authorable: every per-file compile would 404 once
            // `queryableSources: "declared"` is set. The boundary is
            // discovery curation, not access control; the accepted trade is
            // that /compile can reveal a
            // non-exported source's schema (and, with includeSql, SQL) —
            // sources whose confidentiality matters are gated by
            // `#(authorize)`, which still applies here in full.
            await denyHiddenAsNotQueryable(
               () => {
                  gateModel.assertQueryBoundaryEarly(
                     undefined,
                     undefined,
                     source,
                  );
               },
               () =>
                  gateModel.assertAuthorizedForText(source, givens ?? {}, {
                     // File and package scope compile the whole file (or, at
                     // package scope with a source, the whole replacement) —
                     // a locked name that is not the statement Malloy runs
                     // must not refuse it, and its joins are author joins.
                     wholeFile: scope !== "append",
                  }),
            );
         }
      };
      if (!documentCandidate) await runEarlyGate();

      // Initialize Runtime with the package's active MalloyConfig so compile
      // checks see the same package-scoped duckdb as execution. This runtime
      // borrows the package config; the package/environment lifecycle owns release.
      // Thread the package's bound build manifest (when present) so the
      // /compile preview routes persist sources to their materialized tables
      // exactly like execution does — otherwise includeSql=true would always
      // show base-table SQL and diverge from what executeQuery actually runs.
      const boundManifestEntries = pkg.getBuildManifestEntries();
      const runtime = new Runtime({
         urlReader: interceptingReader,
         config: pkg.getMalloyConfig(),
         buildManifest: boundManifestEntries
            ? { entries: boundManifestEntries, strict: false }
            : undefined,
      });

      // Tag each diagnostic with the package-relative model it points at,
      // read off `at.url`. Load-bearing at scope "package" (one array,
      // many files) and clarifying everywhere else: an "append"-scope
      // diagnostic can point at pre-existing model content, and the tag is
      // what says so. The append-mode virtual file reports as the model it
      // extends.
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

      if (scope === "package") {
         // Gate the caller's replacement once on the main thread. The full
         // package compile below runs in the load worker, but authorization
         // probes use the package's live connection/config and must remain
         // on this side of the worker boundary.
         if (source !== undefined && gateModel) {
            try {
               const materializer = runtime.loadModel(virtualUrl);
               await materializer.getModel();
               let finalQuery: ReturnType<
                  typeof materializer.loadFinalQuery
               > | null = null;
               try {
                  finalQuery = materializer.loadFinalQuery();
               } catch {
                  // No runnable query in the replacement text.
               }
               if (finalQuery) {
                  await denyHiddenAsNotQueryable(
                     () => {
                        if (!hasExactGateModel) {
                           throw new NotQueryableError(
                              "Query target is not queryable.",
                           );
                        }
                        return gateModel.assertCompiledTargetQueryable(
                           finalQuery,
                           source,
                        );
                     },
                     () =>
                        hasExactGateModel
                           ? gateModel.assertAuthorizedForRunnable(
                                finalQuery,
                                givens ?? {},
                             )
                           : gateModel.assertAuthorizedFromCompiledRunnable(
                                finalQuery,
                                givens ?? {},
                             ),
                  );
               }
            } catch (error) {
               // Compiler diagnostics are returned by the worker below,
               // the translator's plain Error among them (the worker
               // classifies it). Authorization denials are policy outcomes
               // and propagate.
               if (
                  !(error instanceof MalloyError) &&
                  !translatorMalloyError(error)
               ) {
                  throw error;
               }
            }
         }

         // Use the exact worker path a reload uses: dotfiles are ignored,
         // both .malloy and .malloynb files are compiled, CPU work is kept
         // off the event loop, and the worker's timeout bounds the request.
         // This does not swap the returned models into the served package.
         let outcome;
         try {
            outcome = await getPackageLoadPool().loadPackage({
               packagePath,
               packageName,
               environmentName: host.environmentName,
               malloyConfig: pkg.getMalloyConfig(),
               defaultConnectionName: "duckdb",
               buildManifest: boundManifestEntries,
               collectProblems: true,
               replacement:
                  source === undefined
                     ? undefined
                     : { modelPath: modelName, source },
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

         // Package scope intentionally reports diagnostics from every model
         // the reload compiler sees, including files hidden from discovery.
         // It returns no rows or SQL; authorize still gates caller text.
         const seen = new Set<string>();
         const problems: TaggedLogMessage[] = [];
         const collect = (
            batch: LogMessage[],
            fallbackModel?: string,
         ): void => {
            for (const problem of tagProblems(batch)) {
               const problemUrl = (problem as { at?: { url?: string } }).at
                  ?.url;
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
               }|${start?.character ?? -1}|${tagged.severity}|${
                  tagged.message
               }`;
               if (seen.has(key)) continue;
               seen.add(key);
               problems.push(tagged);
            }
         };
         // The findings a reload would add on the main thread after this
         // same worker compile: render tags and the report, given and
         // drill lints. Read before the per-file text lint below, which
         // drops its copy of a finding only when the report lint reported
         // it too.
         const { renderTagWarnings, reportWarnings } =
            await Package.lintWorkerOutcome(
               host.environmentName,
               packageName,
               packagePath,
               pkg.getMalloyConfig(),
               outcome,
               boundManifestEntries,
               source === undefined
                  ? undefined
                  : { modelPath: modelName, source },
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
               const compilerProblems =
                  compiled.compilationError.malloyProblems;
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
            // A file that did not compile carries no text back, so it is read as saved (or as replaced).
            // Any file that can be a report; the lint says nothing about a
            // file with no artifact tag, wherever it lives.
            const lintText = !(
               compiled.modelPath.endsWith(".malloy") &&
               isReportEligiblePath(compiled.modelPath)
            )
               ? undefined
               : (compiled.modelSourceText ??
                 (compiled.modelPath === modelName && source !== undefined
                    ? source
                    : await fs.promises
                         .readFile(
                            path.join(packagePath, compiled.modelPath),
                            "utf8",
                         )
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
         // Each keeps its own severity, so a broken dashboard makes the
         // compile an error, as it should. They carry no position, so the
         // subject (the view, field or given) leads the message: without
         // it, two views with the same finding would collapse into one.
         const asProblem = (
            warning: (typeof renderTagWarnings)[number],
            code: string,
         ): LogMessage =>
            ({
               severity: warning.severity ?? "warn",
               message: warning.subject
                  ? `${warning.subject}: ${warning.message}`
                  : warning.message,
               code,
            }) as LogMessage;
         for (const warning of renderTagWarnings) {
            collect([asProblem(warning, "render-tag")], warning.model);
         }
         for (const warning of reportWarnings) {
            collect(
               [asProblem(warning, warning.code ?? UNCODED_REPORT_FINDING)],
               warning.model,
            );
         }
         if (
            source !== undefined &&
            outcome.replacementMatchedExisting === false
         ) {
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

      // The model the append-scope fragment is judged against, loaded once for the gate and for a document.
      let appendBase: ReturnType<Runtime["loadModel"]> | undefined;

      // Counted here rather than inside the gate so every reason shares one instrument and one label set; only a refusal is counted, since anything else the gate rethrows is an infrastructure failure.
      const countRefusal = (error: unknown): void => {
         // An unparseable tile is a compile problem for the document, not a refusal.
         if (
            error instanceof CompileRefusedError &&
            !(error instanceof UnparseableTextError)
         ) {
            getCompileRefusalsCounter().add(1, {
               environment: host.environmentName,
               reason:
                  error instanceof RenderTagRefusedError
                     ? "render_tag"
                     : "restricted_construct",
            });
         }
      };
      const refuseConstructs = async (
         baseModel: MalloyModel,
         text: string,
         renderTags: boolean,
      ): Promise<void> => {
         try {
            await assertNoRestrictedConstructs(runtime, baseModel, text, {
               renderTags,
            });
         } catch (error) {
            countRefusal(error);
            throw error;
         }
      };
      const refuseRenderTags = (text: string): void => {
         try {
            assertNoRenderTags(text);
         } catch (error) {
            countRefusal(error);
            throw error;
         }
      };

      // Containment for caller-submitted fragments. Scope "append" is the
      // one scope whose text is a FRAGMENT checked against a curated model
      // rather than a file the author owns, so it has no legitimate need to
      // define its own data roots -- and Malloy resolves a source's schema
      // at compile time, so an unrestricted one reaches the connection, the
      // filesystem and the network without running a query. Scopes "file"
      // and "package" are deliberately NOT gated: there the source IS the
      // model file, and `import` plus `connection.table(...)` /
      // `connection.sql(...)` are how any model declares what it reads.
      // Gating them would make an ordinary package un-authorable.
      if (scope === "append") {
         // The model as saved, WITHOUT the caller's appended text: the
         // fragment is checked against the surface the author published, so
         // the caller cannot widen the namespace it is judged against.
         //
         // Five of the seven restricted constructs are refused on sight,
         // but two are not: `name!type(...)` and the `sql_*` family are
         // classified inside `computeExpression(fs)`, which needs a resolved
         // FieldSpace. With no base model a fragment like
         // `run: base_source -> { ... }` never resolves `base_source`, so
         // the expression is never evaluated, the construct is never
         // classified, and the gate passes text the real compile then runs
         // for real. So a base model that will not load fails the request
         // rather than lowering the gate: the caller's own text is not what
         // failed, and the same argument `assertNoRestrictedConstructs`
         // makes about its own catch applies here -- an infrastructure
         // error carries no evidence either way.
         let baseModel: MalloyModel;
         try {
            appendBase = runtime.loadModel(pathToFileURL(modelPath));
            baseModel = await appendBase.getModel();
         } catch (error) {
            // Three different failures arrive here and they are not one
            // answer. Refusing uniformly would tell a caller their text was
            // bad when the warehouse was down, and a 4xx says "do not
            // retry" -- the opposite of what an outage wants. The detail
            // stays server-side either way: `modelPath` is an absolute path
            // inside the container, so returning it would answer "does this
            // file exist, and is it readable" for any path a caller names,
            // which is the shape of oracle this gate exists to close.
            // `warn`, not `error`: a caller typo in `modelPath` reaches here,
            // and an unauthenticated 400 must not emit ERROR at whatever rate
            // a caller likes.
            logger.warn("Compile gate could not load the base model", {
               modelPath,
               error,
            });
            getCompileRefusalsCounter().add(1, {
               environment: host.environmentName,
               reason: "base_model_load_failed",
            });
            if (error instanceof MalloyError) {
               // Every MalloyError here is the same answer: the model this
               // fragment would be judged against did not compile, so there
               // is nothing to judge it against.
               //
               // There was a 503 branch keyed on `failed-to-fetch-table-schema`,
               // on the reading that a schema fetch which failed means the
               // dependency is unreachable. That code does not carry that
               // meaning: a table that simply DOES NOT EXIST produces it too,
               // so a permanent authoring error was telling the client to
               // retry. The reverse also held -- a `conn.sql(...)`-rooted
               // model whose warehouse was genuinely down fails with
               // `invalid-sql-source` and took the 400 anyway. Splitting on
               // it was therefore wrong in both directions, and Malloy
               // publishes no code here that means "unreachable". Until one
               // exists, these are one 400 carrying the model's own problems,
               // which is also what `file` and `package` scope already do
               // with the same failure.
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
         // The fragment ALONE, against the compiled base model. The
         // concatenation the real compile runs cannot be passed here:
         // `extendModel` judges text as an extension of a model that
         // already holds those declarations, so feeding it the model's
         // own text yields `Cannot redefine` for every source in the file
         // and aborts before the appended fragment is ever classified --
         // which is a bypass rather than a stricter check.
         //
         // What closes the continuation hole instead is the gate refusing
         // when it could not parse what it was given (see
         // assertNoRestrictedConstructs). A continuation fragment is a
         // syntax error on its own, and that is now a refusal rather than
         // silence read as approval.
         // A document's whole-text gate runs on the text that compiles, after the per-cell access phase.
         if (!documentCandidate) {
            await refuseConstructs(baseModel, source ?? "", false);
         }
      }

      if (documentCandidate && source !== undefined) {
         const gate = gateModel;
         const exact = hasExactGateModel;
         const base = appendBase;
         if (!base) throw new Error("append base model was not loaded");
         const baseModel = await base.getModel();
         // Syntactic and cheap, so it runs ahead of the cell reader; the compile-based gate runs once access is settled.
         refuseRenderTags(source);
         const result = await compileDocument({
            base,
            source,
            modelName,
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
                          () =>
                             gate.assertAuthorizedForText(text, givens ?? {}),
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
               constructs: (text) => refuseConstructs(baseModel, text, true),
               document: (text) => refuseConstructs(baseModel, text, false),
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
         if (result) {
            return {
               problems: result.problems.map((problem) => ({
                  ...problem,
               })) as TaggedLogMessage[],
               ...(result.document && { document: result.document }),
            };
         }
         // Not a readable document after all, so the ordinary compile runs and the whole text is judged as one.
         await refuseConstructs(baseModel, source, false);
         await runEarlyGate();
      }

      // Attempt to compile
      try {
         const modelMaterializer = runtime.loadModel(virtualUrl);
         const model = await modelMaterializer.getModel();

         // Resolve the final query's materializer once (if there is one).
         let queryMaterializer: ReturnType<
            typeof modelMaterializer.loadFinalQuery
         > | null = null;
         try {
            queryMaterializer = modelMaterializer.loadFinalQuery();
         } catch {
            // No runnable query (e.g. only source definitions) — nothing to
            // gate or extract. The early text gate ran, but it only REFUSES
            // a gate it cannot classify or graft: every expressible one is
            // deferred to the compiled backstop below, which needs a
            // runnable. Nothing is exposed by skipping it here, since
            // without a runnable there is no result and no SQL.
         }

         // Compiled-source backstops — run REGARDLESS of includeSql. They
         // gate the source the COMPILED final query actually reads, closing
         // the named-query and derivation indirection the early
         // surface-syntax gate cannot see. Compiling a gated source even
         // without SQL is a schema oracle
         // (field-not-found errors leak its columns), so this must not be
         // conditional on SQL extraction. (A `source: x is gated` alias
         // carries the gate: only a declaration of its OWN `#(authorize)`
         // replaces it, and caller text may not declare one.)

         // No boundary backstop here: /compile is exempt from the query
         // boundary by design (see the gate comment above). Only the
         // authorize backstop runs.

         // Authorize backstop (the *who* axis, 403). NOT guarded by
         // hasAuthorize(): that reads only top-level modelDef.contents
         // sources' OWN annotations, so a run target gated solely by what it
         // derives from is invisible to it and this backstop would silently
         // never run for such a model — the same bypass
         // assertAuthorizedForRunnable itself closes on the query path (see
         // model.ts assertAuthorizedForAllSources). The own-source probe and
         // derivation walk it runs are cheap no-ops for an ungated model.
         if (queryMaterializer && gateModel) {
            const materializer = queryMaterializer;
            await denyHiddenAsNotQueryable(
               () => {
                  if (!hasExactGateModel) {
                     throw new NotQueryableError(
                        "Query target is not queryable.",
                     );
                  }
                  return gateModel.assertCompiledTargetQueryable(
                     materializer,
                     source,
                  );
               },
               () =>
                  hasExactGateModel
                     ? gateModel.assertAuthorizedForRunnable(
                          materializer,
                          givens ?? {},
                          callerRegion,
                       )
                     : // No region: this gate model is another file's
                       // namespace, so its source names cannot place a join.
                       gateModel.assertAuthorizedFromCompiledRunnable(
                          materializer,
                          givens ?? {},
                       ),
            );
         }

         // If includeSql is requested and compilation succeeded, attempt to extract SQL
         let sql: string | undefined;
         if (includeSql && queryMaterializer) {
            // A given value the compiled text's own filter types cannot read
            // is a bad request, as on the query route. Checked here, after
            // the gate and against the submitted text's givens rather than
            // the cached model's, because this is the only place /compile
            // binds given values.
            assertFilterGivensParse(
               Array.from(model.givens.values()).map((g) =>
                  malloyGivenToApi(g as MalloyGiven),
               ),
               givens,
            );
            try {
               sql = await queryMaterializer.getSQL({ givens });
            } catch (error) {
               // A bad caller given (unknown name, wrong-typed value, finalized
               // override, ...) surfaces as a Malloy `runtime-given-*` error.
               // Map it to a 400 rather than silently omitting `sql` (which is
               // indistinguishable from "no runnable query"). Duck-type on
               // `.code`; let a MalloyError fall to the outer catch → problems.
               // The `runtime-given-` prefix is pinned to Malloy's error codes
               // (given_binding.ts / runtime.ts, same as model.ts) — if they're
               // renamed upstream a bad given would silently revert to the omit
               // branch below, so keep the two in sync.
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
               // Otherwise the source may just not contain a runnable query
               // (e.g. only source definitions) — omit the sql field.
            }
         }

         // If successful, return any non-fatal warnings
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
         // If parsing/compilation fails, return the errors. The
         // translator's plain Error is one of them, not a server fault.
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
         // If it's a system error (e.g. file not found), throw it up
         throw error;
      }
   });
}
