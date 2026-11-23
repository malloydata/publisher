// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(materialize)` build companion (see
 * build_file_generator.ts), as rollup_compile.ts compiles the rollup
 * companion: the text is served from an overlay at a URL beside the author's
 * model, so its `import` resolves while it never shows up in
 * `getModelPaths()`, the API or the package's own compile, and cannot go stale
 * against the author's edits.
 */

import { tierOf } from "./refusal_key";
import { errorCodeOf } from "../../../error_code";
import type { RefusedReason } from "../domain/types";
import {
   hiddenAccess,
   modelDefIfAny,
   modelDefOf,
} from "../../governance/ir/compiled_model";
import {
   FixedConnectionMap,
   type Runtime,
   type Connection,
   type Model as MalloyModel,
   type ModelMaterializer,
} from "@malloydata/malloy";
import * as path from "path";
import { identifier, stringLiteral } from "../../malloy_text";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import type { Omission } from "../domain/omitted_sources";
import { capabilitiesOf, STORAGE_DIALECT } from "../domain/dialect";
import {
   keyCheckCompilerKey,
   type KeyCheckCompiler,
} from "../domain/key_check";
import {
   buildCopyDeclarationAt,
   buildCopyName,
   buildCopyText,
   chainCopyName,
   crossFileCopyTexts,
} from "./materialize_build_copy";
import {
   chainLevels,
   generatedNamesOf,
   generateBuildFiles,
   givenReadTerms,
   levelFileName,
   MATERIALIZE_COMPANION_SUFFIX,
   withoutParent,
} from "./build_file_generator";
import {
   extensionOverStoredModel,
   storedChainText,
   type StoredColumnType,
} from "./materialize_stored_chain";
import {
   chainsOver,
   chainThroughFile,
   chainWithinFile,
   crossFileEntries,
   planModelMaterialize,
   type MaterializeRefusal,
} from "./materialize_plan";
import { splitSourceID, type MaterializePlan } from "../domain/table_plans";
import { UPSTREAM_TABLE_UNUSABLE_REASON } from "./downstream_upstreams";
import type { TermUse } from "../domain/read_time_terms";
import { SourceMap } from "../text/source_map";
import { wholeImportsOf } from "../text/declaration_text";
import {
   compileInMemory,
   companionFailure,
   companionOmissions,
   type CompanionFailure,
   declaredInModel,
   GeneratedFiles,
   type SynthesizedRuntimeFactory,
} from "../text/generated_files";
import { leftOutWindowTerms } from "./rolling_window";
import { storedColumnsOfOutput } from "../stored_column_types";
import {
   probeSourceDependence,
   type FieldDependence,
   type ProbeCompiler,
} from "../../probe_compiler";

interface GeneratedMaterialize {
   /** The sources the generator planned. Never empty. */
   plans: MaterializePlan[];
   /** The compiled companion: its build targets are the generated sources. */
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   /** The generated text, for logging and tests. */
   text: string;
   /**
    * The build copies of the author's model, by the sourceID of the source each is for
    *: the author's text with that source's read-time terms taken out.
    */
   buildCopies?: ReadonlyMap<string, string>;
   /**
    * The URL of every file generated beside the author's (the companion, its
    * level files and the build copies). A source declared in one that no plan
    * names (a parent a build copy re-roots) shares a generated source's
    * table and is no build of its own.
    */
   generatedURLs: ReadonlySet<string>;
   /**
    * The text of every file generated beside the author's, by URL: what a
    * compile that imports a generated build source needs (rollup_compile.ts,
    * a rollup over a stored `#(materialize)` base), and the companions of the
    * files an extension's parent is declared in (`buildsOver.crossFile`),
    * which its build copies import.
    */
   files: ReadonlyMap<string, string>;
   /**
    * The author's sourceID of each generated build source this companion's
    * builds may name, by the generated source's own sourceID: its own plans'
    * and those of the companions it compiled over.
    */
   renames: ReadonlyMap<string, string>;
   /**
    * Each uniqueness check's compiler over a run's compiler manifest, by
    * {@link keyCheckCompilerKey}: compiled through the companion, which only
    * this process holds.
    */
   checkCompilers: ReadonlyMap<string, KeyCheckCompiler>;
}

/** Where the generated companion for `modelPath` lives, as an absolute URL. */
function materializeCompanionURL(packagePath: string, modelPath: string): URL {
   // pathToFileURL, not `file://${path}`: on Windows the path is `D:\…`, and
   // the overlay key must match the URL Malloy normalizes to.
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${MATERIALIZE_COMPANION_SUFFIX}`,
   );
}

/**
 * Plan, generate and compile the build companion for one author model.
 * `generated` is undefined when the model declares no plannable
 * `#(materialize)`, or when the companion does not compile: a failure is a
 * publisher bug (the text is generated), so it is logged at `error` level and
 * costs that model its generated builds and nothing else. `refused` holds the
 * declarations the generator would not plan, for the caller to report; the
 * eligibility gates judge the generated sources like any persist source once
 * they are in the plan.
 */
export async function tryCompileMaterializeCompanion(args: {
   packagePath: string;
   modelPath: string;
   contents: Record<string, unknown>;
   /**
    * The author model's `ModelDef.givens`, which its read-time terms read.
    * Required: without it every given-reading `where:` is refused.
    */
   givens: Record<string, unknown>;
   getRuntime: SynthesizedRuntimeFactory;
   /**
    * Compiles probes against the author's model: which of each planned
    * source's computed dimensions read a given, the current time or the query
    * time zone. Absent: the IR walks alone decide.
    */
   probe?: ProbeCompiler;
   /**
    * The model files whose companions are being compiled up the stack, for
    * a parent's companion compiled for an extension in another file: one
    * already on it is not compiled again (an import cycle).
    */
   crossFileStack?: ReadonlySet<string>;
   /**
    * Each model file's companion as a cross-file parent's file, compiled
    * once per package plan and shared by every file that imports it, and by
    * the companions compiled for it in turn (see {@link crossFileParents}).
    */
   crossFileCompanions?: CrossFileCompanions;
   /**
    * The author model's source registry, with `contents` what a later
    * model's cross-file walk reads of this one when it finds this model's
    * companion in {@link crossFileCompanions}.
    */
   sourceRegistry?: unknown;
}): Promise<{
   generated?: GeneratedMaterialize;
   refused: MaterializeRefusal[];
   /** The sources a failure left out of the plan (see omitted_sources). */
   omitted?: Omission[];
   /** Why the companion failed, for the caller to report. */
   failure?: CompanionFailure;
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const refused: MaterializeRefusal[] = [];
   // The author's sourceIDs each generated source is presented as, once
   // planned; until then a failure cannot name them.
   let plannedIDs: string[] | undefined;
   try {
      const planned = await planWithProbes(args, modelFile);
      refused.push(...planned.refused);
      plannedIDs = planned.plans.map((p) => p.sourceID);
      const foreign = await crossFileParents(
         planned.plans,
         args,
         modelFile,
      ).catch((error: unknown): CrossFileParents => {
         // Never a cost to the model's builds: its extensions build over
         // the raw chain, as they did before.
         logger.warn(
            "A model's extension tables could not be chained on parents in other files; they build over the raw chain",
            {
               modelPath: args.modelPath,
               code: errorCodeOf(error),
            },
         );
         return { plans: planned.plans, parents: new Map() };
      });
      const compile = companionCompiler(args, modelFile, refused, foreign);
      let round = await compile(foreign.plans);
      // Three checks of a round that compiled, each run once, in this order,
      // each compiling again through `compile` (whose own loop settles any
      // failure that brings). They stay out of compileContained's loop: they
      // judge a compile that succeeded, two of them by compiling again, so
      // running them after every round would add compiles, and a later
      // check's recompile would reopen an earlier one's decision.
      round = await unchainStoredChainsNotCompiling(
         round,
         compile,
         args,
         refused,
         foreign,
      );
      round = await withoutLeakedBuilds(round, compile, modelFile, refused);
      round = await withCrossFileColumnsKept(
         round,
         compile,
         args,
         refused,
         foreign,
      );
      // Shared with every later model that imports this one: its companion
      // is not compiled again as a cross-file parent's.
      if (
         round.generated &&
         args.crossFileCompanions &&
         !args.crossFileCompanions.has(modelFile)
      ) {
         const generated = withStoredColumns(round.generated);
         args.crossFileCompanions.set(
            modelFile,
            GeneratedFiles.over(args.getRuntime)
               .read(pathToFileURL(modelFile))
               .then((text) => ({
                  generated,
                  text,
                  modelFile,
                  modelDef: {
                     contents: args.contents,
                     sourceRegistry: args.sourceRegistry,
                  } as unknown as ReturnType<typeof modelDefOf>,
               }))
               .catch(() => undefined),
         );
      }
      return round.generated
         ? { generated: withStoredColumns(round.generated), refused }
         : { refused };
   } catch (error) {
      return {
         refused,
         failure: companionFailure("materialize", error),
         omitted: companionOmissions(
            "materialize",
            plannedIDs
               ? {
                    sourceIDs: plannedIDs.filter(
                       (id) => !refused.some((r) => r.sourceID === id),
                    ),
                 }
               : { file: pathToFileURL(modelFile).href, names: undefined },
         ),
      };
   }
}

type CompanionArgs = Parameters<typeof tryCompileMaterializeCompanion>[0];

/** One compile of the companion: the plans it holds, their build copies, and the model. */
interface CompanionRound {
   plans: MaterializePlan[];
   copies: Map<string, BuildCopy>;
   generated?: GeneratedMaterialize;
}

/** Compiles the companion over a set of plans, each source's failure its own. */
type CompanionCompile = (from: MaterializePlan[]) => Promise<CompanionRound>;

/**
 * Plan the model's sources, only those it declares itself. Caller-dependent
 * dimensions: plan once from the IR, probe the computed dimensions that plan
 * would store, and plan again with the answers. A probe only ever leaves a
 * dimension out or refuses a source, so every dimension the second plan stores
 * was probed.
 */
async function planWithProbes(
   args: CompanionArgs,
   modelFile: string,
): Promise<ReturnType<typeof planModelMaterialize>> {
   // Not a local file (a host, another scheme): planned.
   const declaredHere = declaredInModel(modelFile, true);
   const probed = args.probe
      ? await probeMaterializeDimensions(
           args.probe,
           planModelMaterialize(args.contents, declaredHere, args.givens).plans,
           args.contents,
        )
      : undefined;
   return planModelMaterialize(
      args.contents,
      declaredHere,
      args.givens,
      probed?.fields,
      probed?.zoneFreeRows,
   );
}

/**
 * The companion's compiler: each round gives the plans their build copies,
 * then compiles them, one source's generated text that does not compile
 * costing that source alone (see compileContained). Each source is refused
 * into `refused` at most once across rounds.
 */
function companionCompiler(
   args: CompanionArgs,
   modelFile: string,
   refused: MaterializeRefusal[],
   foreign: CrossFileParents = NO_CROSS_FILE_PARENTS,
): CompanionCompile {
   // The text the author's model was compiled from, through the reader
   // the companion compiles with: a worker's file map or an overlay, not
   // necessarily the file on disk, and the compiled locations the terms are found by are
   // positions in that text. Read once.
   const files = GeneratedFiles.over(args.getRuntime);
   let modelText: Promise<string> | undefined;
   const readModelText = () =>
      (modelText ??= files.read(pathToFileURL(modelFile)));
   // A build copy compiled on its own, beside the author's model, so its
   // imports resolve as the author's do; never run. The hidden-field check
   // reads which hidden fields the copy lets the build select off it.
   const compileCopy = async (fileName: string, text: string) => {
      const url = pathToFileURL(path.join(path.dirname(modelFile), fileName));
      const { model } = await files.compile(url, new Map([[url.href, text]]));
      return {
         contents: modelDefOf(model).contents as Record<string, unknown>,
         url: url.href,
      };
   };
   const refusedIDs = new Set<string>();
   const prepare = async (from: MaterializePlan[]) => {
      const out = await withBuildCopies(
         from,
         modelFile,
         readModelText,
         compileCopy,
         args.contents,
         foreign.parents,
      );
      for (const r of out.refused) {
         if (r.sourceID !== undefined && refusedIDs.has(r.sourceID)) continue;
         if (r.sourceID !== undefined) refusedIDs.add(r.sourceID);
         refused.push(r);
      }
      return out;
   };
   return (from) =>
      compileContained({ ...args, foreign }, from, prepare, (refusal) => {
         if (refusedIDs.has(refusal.sourceID as string)) return;
         refusedIDs.add(refusal.sourceID as string);
         refused.push(refusal);
      });
}

/**
 * A `storage=` extension table builds in its destination from its chain
 * alone, over its parent's stored table. One whose chain does not compile
 * there is refused (`upstream_table_unusable`), never built over the raw
 * chain from the warehouse: the companion is compiled again without it.
 */
async function unchainStoredChainsNotCompiling(
   round: CompanionRound,
   compile: CompanionCompile,
   args: CompanionArgs,
   refused: MaterializeRefusal[],
   foreign: CrossFileParents = NO_CROSS_FILE_PARENTS,
): Promise<CompanionRound> {
   const unstored = round.generated
      ? await storedChainsNotCompiling(round.generated, foreign.parents)
      : new Map<string, string>();
   if (unstored.size === 0) return round;
   for (const [sourceID, why] of unstored) {
      const plan = planOf(round.plans, sourceID);
      if (!plan) continue;
      logger.warn(
         "A storage= extension table does not compile over its parent's stored table alone; it is refused",
         { modelPath: args.modelPath, sourceName: plan.sourceName },
      );
      refuseUnchained(
         refused,
         plan,
         `whose stored table its build does not compile over in its ` +
            `destination (${why})`,
      );
   }
   return compile(round.plans.filter((p) => !unstored.has(p.sourceID)));
}

/**
 * Refuse `plan`, an extension table that cannot be built over its parent's
 * table, as `upstream_table_unusable` naming the parent and `why`: it is
 * never built over the raw chain from the warehouse instead, and answers
 * live. Once per source.
 */
function refuseUnchained(
   refused: MaterializeRefusal[],
   plan: MaterializePlan,
   why: string,
): void {
   if (refused.some((r) => r.sourceID === plan.sourceID)) return;
   refused.push(unchainedRefusal(plan, why));
}

/** The refusal {@link refuseUnchained} records. */
function unchainedRefusal(
   plan: MaterializePlan,
   why: string,
): MaterializeRefusal {
   return {
      sourceName: plan.sourceName,
      sourceID: plan.sourceID,
      reason: unchainedReason(plan, why),
      outcome: {
         refused: {
            reason: UPSTREAM_TABLE_UNUSABLE_REASON,
            tier: tierOf(plan.persist.storage),
         },
      },
   };
}

/** Why an extension table is refused rather than built over the raw chain. */
function unchainedReason(plan: MaterializePlan, why: string): string {
   const parent = plan.buildsOver?.sourceName;
   return (
      `it extends ${parent ? `'${parent}'` : "a #(materialize) parent"}, ${why}; ` +
      `an extension table is built over its parent's table or not at all, ` +
      `never by recomputing its parent from the warehouse`
   );
}

/**
 * A build copy re-declares its whole file, so a `#@ persist` source the
 * copied source reads (its base, a join) would enter the plan a second time
 * under the copy's URL: a build node that no planned source names. A chained
 * source is refused (`upstream_table_unusable`); a source whose given term needs
 * the copy is refused into `refused`, and the companion compiled again
 * without it.
 */
async function withoutLeakedBuilds(
   round: CompanionRound,
   compile: CompanionCompile,
   modelFile: string,
   refused: MaterializeRefusal[],
): Promise<CompanionRound> {
   let { plans, copies, generated } = round;
   let leaked = generated
      ? await copyDeclaredBuilds(generated, modelFile, copies)
      : new Map<string, string[]>();
   const chainedLeaks = plans.filter(
      (p) => p.buildsOver && leaked.has(p.sourceID),
   );
   if (chainedLeaks.length > 0) {
      for (const plan of chainedLeaks) {
         refuseUnchained(
            refused,
            plan,
            `whose build copy declares a persisted source the plan does not ` +
               `name (${(leaked.get(plan.sourceID) ?? []).join(", ")})`,
         );
      }
      const ids = new Set(chainedLeaks.map((p) => p.sourceID));
      ({ plans, copies, generated } = await compile(
         plans.filter((p) => !ids.has(p.sourceID)),
      ));
      leaked = generated
         ? await copyDeclaredBuilds(generated, modelFile, copies)
         : new Map<string, string[]>();
   }
   if (leaked.size === 0) return { plans, copies, generated };
   for (const plan of plans) {
      const names = leaked.get(plan.sourceID);
      if (!names) continue;
      if (
         givenReadTerms(plan).length === 0 &&
         leftOutWindowTerms(plan.readTimeTerms).length === 0
      ) {
         // A copy made only to store hidden columns: build from the
         // author's file instead, storing the public ones.
         copies.delete(plan.sourceID);
         const columns = plan.columns.filter((c) => !c.access);
         const kept = new Set(columns.map((c) => c.column));
         if (
            columns.length > 0 &&
            (plan.readTimeTerms ?? []).every((t) =>
               t.columns.every((c) => kept.has(c)),
            )
         ) {
            leaked.delete(plan.sourceID);
            plans = plans.map((p) => (p === plan ? { ...plan, columns } : p));
            continue;
         }
      }
      refused.push({
         sourceName: plan.sourceName,
         sourceID: plan.sourceID,
         outcome: {
            refused: {
               reason:
                  givenReadTerms(plan).length > 0
                     ? "reads_given"
                     : leftOutWindowTerms(plan.readTimeTerms).length > 0
                       ? "current_time"
                       : "no_stored_columns",
               tier: tierOf(plan.persist.storage),
            },
         },
         reason:
            givenReadTerms(plan).length > 0 ||
            leftOutWindowTerms(plan.readTimeTerms).length > 0
               ? "a `where:` reading a given or keeping rows older than a " +
                 "moving cut-off cannot yet be left out of the " +
                 "build of a source that reads a #@ persist source " +
                 `(${names.join(", ")}): its build would declare that source ` +
                 "a second time"
               : "the source stores only hidden fields, which its build " +
                 "cannot read while it reads a #@ persist source " +
                 `(${names.join(", ")})`,
      });
   }
   return compile(plans.filter((p) => !leaked.has(p.sourceID)));
}

/**
 * Each plan with its build source's compiled output columns
 * ({@link MaterializePlan.storedColumns}): what a database build writes,
 * which its serve binding declares. A nested column is kept only on DuckDB,
 * whose tables hold nests natively; elsewhere a nest is stored as JSON, which
 * the served copy cannot declare, so the column is left out and the source
 * serves live.
 */
function withStoredColumns(
   generated: GeneratedMaterialize,
): GeneratedMaterialize {
   const contents = modelDefIfAny(
      generated.model as unknown as {
         _modelDef?: { contents?: Record<string, unknown> };
      },
   )?.contents;
   return {
      ...generated,
      plans: generated.plans.map((plan) => {
         const def = contents?.[plan.buildSourceName] as
            | { fields?: unknown[]; dialect?: unknown }
            | undefined;
         if (!def?.fields) return plan;
         return {
            ...plan,
            storedColumns: storedColumnsOfOutput(def.fields, {
               nested: capabilitiesOf(def.dialect).nestedColumns,
            }),
         };
      }),
   };
}

/**
 * The probe compiler's answers for each plan's own computed dimensions, by
 * `sourceID` then dimension, and the sources whose rows it found the same in
 * every query time zone. Compiled against the author's model by the
 * source's name there; never run.
 */
async function probeMaterializeDimensions(
   compiler: ProbeCompiler,
   plans: readonly MaterializePlan[],
   contents: Record<string, unknown>,
): Promise<{
   fields: Map<string, Map<string, FieldDependence>>;
   zoneFreeRows: Set<string>;
}> {
   const fields = new Map<string, Map<string, FieldDependence>>();
   const zoneFreeRows = new Set<string>();
   for (const plan of plans) {
      // Every plan is probed, one with no computed dimension too: its rows
      // can depend on the zone through its own `where:` or base relation.
      const computed = plan.columns
         .filter((c) => c.computed && c.join === undefined)
         .map((c) => c.dimension);
      // By sourceID (the keying rule); the probe names the source as this
      // model does, which for a source planned here is its own name.
      const def = Object.values(contents).find(
         (o) =>
            !!o &&
            typeof o === "object" &&
            (o as { sourceID?: unknown }).sourceID === plan.sourceID,
      ) as { fields?: { name?: unknown; as?: unknown }[] } | undefined;
      const reserved = new Set(
         (def?.fields ?? []).map((f) => String(f.as ?? f.name)),
      );
      const answer = await probeSourceDependence(
         compiler,
         plan.sourceName,
         computed,
         reserved,
      );
      if (computed.length > 0) fields.set(plan.sourceID, answer.fields);
      if (answer.rowsZoneFree) zoneFreeRows.add(plan.sourceID);
   }
   return { fields, zoneFreeRows };
}

/** A planned source's build copy: its file name beside the model, and text. */
interface BuildCopy {
   fileName: string;
   text: string;
   /**
    * For an extension whose parent is in another file: the copy of that
    * file re-rooted on the parent's build source, and a copy of each file
    * the chain passes through on the way, by URL, each importing the next
    * one's copy in the file's place ({@link crossFileCopyTexts}).
    */
   chainCopies?: { url: string; text: string }[];
   /**
    * For a chained copy: the declaration off the chain a place in the
    * copy lies in, if any, by sourceID and its range in the author's file
    * ({@link buildCopyDeclarationAt}).
    */
   declarationAt?: (at: {
      line: number;
      character: number;
   }) => { sourceID: string; declaration: TermUse } | undefined;
}

/** Generate and compile one companion, or undefined when nothing is planned. */
async function compileCompanion(
   args: {
      packagePath: string;
      modelPath: string;
      getRuntime: SynthesizedRuntimeFactory;
      foreign?: CrossFileParents;
   },
   plans: MaterializePlan[],
   copies: ReadonlyMap<string, BuildCopy>,
): Promise<GeneratedMaterialize | undefined> {
   const used = new Map(
      plans.flatMap((p) => {
         const copy = copies.get(p.sourceID);
         return copy ? [[p.sourceID, copy] as const] : [];
      }),
   );
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const files = generateBuildFiles(
      plans,
      path.basename(args.modelPath),
      (declaringURL) => {
         let declaringFile: string;
         try {
            declaringFile = fileURLToPath(declaringURL);
         } catch {
            return undefined;
         }
         if (path.resolve(declaringFile) === modelFile) return undefined;
         return path
            .relative(path.dirname(modelFile), declaringFile)
            .split(path.sep)
            .join("/");
      },
      new Map([...used].map(([name, c]) => [name, c.fileName])),
   );
   if (!files) return undefined;
   // Each cross-file parent's build source, imported so the companion's
   // build targets list it, as an in-file parent's level file is: the
   // extension's build then orders after it and reads its table.
   const own = new Set(plans.map((p) => p.buildSourceName));
   const parentImports = new Map<string, string>();
   for (const plan of plans) {
      const over = plan.buildsOver;
      const name = args.foreign?.parents.get(plan.sourceID)?.plan
         .buildSourceName;
      if (!over?.crossFile || !name || own.has(name)) continue;
      let from: string;
      try {
         from = path
            .relative(
               path.dirname(modelFile),
               fileURLToPath(over.crossFile.buildSourceURL),
            )
            .split(path.sep)
            .join("/");
      } catch {
         continue;
      }
      parentImports.set(name, from);
   }
   const text = parentImports.size
      ? files.entry.replace(
           /^(##![^\n]*\n)/,
           `$1${[...parentImports]
              .map(
                 ([name, from]) =>
                    `import { ${identifier(name)} } from ${stringLiteral(from)}\n`,
              )
              .join("")}`,
        )
      : files.entry;
   const url = materializeCompanionURL(args.packagePath, args.modelPath);
   const overlay = new Map([[url.href, text]]);
   const dir = path.dirname(modelFile);
   // The companions of the files a cross-file parent is declared in, which
   // a build copy's re-rooted parent imports its build source from, at
   // their own URLs: the same build sources there as in that file's own
   // companion, so the parent's build source is the same build.
   const renames = new Map<string, string>();
   for (const plan of plans) {
      const parent = args.foreign?.parents.get(plan.sourceID);
      if (!plan.buildsOver?.crossFile || !parent) continue;
      for (const [u, t] of parent.overlay) overlay.set(u, t);
      for (const [from, to] of parent.renames) renames.set(from, to);
   }
   for (const copy of used.values()) {
      overlay.set(pathToFileURL(path.join(dir, copy.fileName)).href, copy.text);
      for (const c of copy.chainCopies ?? []) overlay.set(c.url, c.text);
   }
   for (const [name, level] of files.levels) {
      overlay.set(pathToFileURL(path.join(dir, name)).href, level);
   }
   const { runtime, materializer } = await GeneratedFiles.over(
      args.getRuntime,
   ).load(url, overlay);
   const copyOwners = new Map(
      [...used].flatMap(([sourceID, c]) => [
         [pathToFileURL(path.join(dir, c.fileName)).href, sourceID] as const,
         ...(c.chainCopies ?? []).map((f) => [f.url, sourceID] as const),
      ]),
   );
   const model = await materializer.getModel().catch((error: unknown) => {
      throw new CompanionCompileError(error, overlay, copyOwners);
   });
   const checkCompilers = new Map<string, KeyCheckCompiler>();
   // The inlined join_one checks' SQL, compiled now (never run here) so a build
   // runs it against the joined source's connection before it writes anything.
   // Each check can also be compiled again over a run's compiler manifest, so a
   // run whose parent build reads the joined source's persisted table checks
   // that table (inline_uniqueness.ts).
   for (const plan of plans) {
      for (const check of plan.uniquenessChecks ?? []) {
         check.sql = await materializer
            .loadQueryByName(check.queryName)
            .getSQL()
            .catch((error: unknown) => {
               throw new CompanionCompileError(
                  error,
                  overlay,
                  copyOwners,
                  plan.sourceID,
               );
            });
         checkCompilers.set(
            keyCheckCompilerKey(plan.sourceID, check.queryName),
            (buildManifest, connectionDigests) =>
               materializer
                  .loadQueryByName(check.queryName)
                  .getSQL({ buildManifest, connectionDigests }),
         );
      }
   }
   const contents = modelDefOf(model).contents as Record<string, unknown>;
   for (const plan of plans) {
      const id = (contents[plan.buildSourceName] as { sourceID?: unknown })
         ?.sourceID;
      if (typeof id === "string") renames.set(id, plan.sourceID);
   }
   return {
      plans,
      model,
      materializer,
      runtime,
      text,
      generatedURLs: new Set(overlay.keys()),
      // The companion as another file's compile imports it: without the
      // imports of its own cross-file parents' build sources, which only
      // make them this companion's build targets. Imported again there, each
      // would be reached twice per level of a chain, and a compile of a deep
      // chain would read each file once per path to it.
      files:
         text === files.entry
            ? overlay
            : new Map([...overlay, [url.href, files.entry]]),
      renames,
      checkCompilers,
      ...(used.size > 0
         ? {
              buildCopies: new Map(
                 [...used].map(([name, c]) => [name, c.text] as const),
              ),
           }
         : {}),
   };
}

/**
 * The `storage=` extension tables among `generated`'s plans whose build
 * does not compile in a destination from {@link extensionOverStoredModel}
 * alone, by sourceID, with why. Checked here, at plan time, against a
 * stand-in for the destination that has DuckDB's dialect and no table, and
 * the parent's stored columns as its build source declares them, so a chain
 * that reaches anything but the parent's table (a join to a warehouse source,
 * a table named in place) never chains, and its build is never refused for a
 * shape the plan could have seen.
 */
async function storedChainsNotCompiling(
   generated: GeneratedMaterialize,
   foreign: ReadonlyMap<string, CrossFileParent> = new Map(),
): Promise<Map<string, string>> {
   const out = new Map<string, string>();
   for (const plan of generated.plans) {
      const over = plan.buildsOver;
      const chain = over?.overStored;
      const destination = plan.persist.storage;
      if (!over || !chain || destination === undefined) continue;
      // A parent in another file is declared by that file's companion.
      const across = over.crossFile ? foreign.get(plan.sourceID) : undefined;
      const parent = over.crossFile
         ? across?.plan
         : planOf(generated.plans, over.sourceID);
      try {
         if (!parent) throw new Error("its parent is no longer planned");
         const columns = parentStoredColumns(
            across ? across.model : generated.model,
            parent.buildSourceName,
         );
         const handle = "stand_in";
         const url = "file:///materialize-stored-chain/m.malloy";
         const { runtime, model } = await compileInMemory(
            new Map([
               [
                  url,
                  extensionOverStoredModel({
                     plan,
                     chain,
                     connectionName: destination,
                     virtualHandle: handle,
                     columns,
                  }),
               ],
            ]),
            url,
            new FixedConnectionMap(
               new Map([[destination, destinationStandIn(destination)]]),
               destination,
            ),
         );
         const targets = await runtime.getBuildTargets(model, {
            virtualMap: new Map([[destination, new Map([[handle, "t"]])]]),
         });
         const built = targets.connections.some((c) =>
            c.targets.some((t) =>
               t.sources.some((s) => s.name === plan.buildSourceName),
            ),
         );
         if (!built) throw new Error("its build source is not planned there");
      } catch (error) {
         out.set(
            plan.sourceID,
            error instanceof Error ? error.message : String(error),
         );
      }
   }
   return out;
}

/**
 * The parent's stored columns as its generated build source declares them,
 * each with the Malloy type a stored table of it reads back as. Throws for a
 * column of a type a stored table does not carry faithfully.
 */
function parentStoredColumns(
   model: MalloyModel,
   buildSourceName: string,
): StoredColumnType[] {
   return model
      .getExploreByName(buildSourceName)
      .allFields.filter((f) => f.isAtomicField())
      .map((f) => {
         const type = STORED_TYPES[String(f.type)];
         if (!type) {
            throw new Error(`column '${f.name}' is of type ${String(f.type)}`);
         }
         return { name: f.name, type };
      });
}

/** The Malloy type a stored column of each atomic type reads back as. */
const STORED_TYPES: Readonly<Record<string, string>> = {
   string: "string",
   number: "number",
   boolean: "boolean",
   date: "date",
   timestamp: "timestamp",
   timestamptz: "timestamp",
   json: "json",
};

/**
 * A destination for a plan-time compile: DuckDB's dialect (every destination
 * is DuckDB or DuckLake), and no table or SQL to read, so anything a chain
 * names but its parent's stored table fails to compile.
 */
export function destinationStandIn(name: string): Connection {
   const none = "not available to an extension table's build";
   const standIn = {
      name,
      dialectName: STORAGE_DIALECT,
      getDigest: () => `materialize-stored-chain:${name}`,
      fetchSchemaForTables: async (tables: Record<string, string>) => ({
         schemas: {},
         errors: Object.fromEntries(Object.keys(tables).map((k) => [k, none])),
      }),
      fetchSchemaForSQLStruct: async () => ({ error: none }),
   };
   return standIn as unknown as Connection;
}

/** The plan of `sourceID` among `plans`. */
function planOf(
   plans: readonly MaterializePlan[],
   sourceID: string,
): MaterializePlan | undefined {
   return plans.find((p) => p.sourceID === sourceID);
}

/**
 * For each planned source whose build plans a source only its build copy
 * declares, the names of those sources. A `sourceID` is `name@<declaring
 * URL>`, so a build copy's own are told apart by the copy's URL. A copy's
 * source on the table of a generated source (a parent an extension's copy
 * re-roots on its build source, and the extension over it) shares that
 * table: no build of its own, so not one the copy declared.
 */
async function copyDeclaredBuilds(
   generated: GeneratedMaterialize,
   modelFile: string,
   copies: ReadonlyMap<string, BuildCopy>,
): Promise<Map<string, string[]>> {
   const out = new Map<string, string[]>();
   if (!generated.buildCopies?.size) return out;
   const dir = path.dirname(modelFile);
   const ownerByURL = new Map(
      [...generated.buildCopies.keys()].flatMap((name) => {
         const copy = copies.get(name);
         return [
            [pathToFileURL(path.join(dir, copy?.fileName ?? "")).href, name],
            ...(copy?.chainCopies ?? []).map((f) => [f.url, name]),
         ] as [string, string][];
      }),
   );
   const urlOf = (id: string) => splitSourceID(id)?.url;
   const targets = await generated.runtime.getBuildTargets(generated.model);
   const groups = [
      ...targets.connections.flatMap((c) =>
         c.targets.map((t) => t.sources.map((s) => s.sourceID)),
      ),
      ...targets.unbuildable.map((u) => [u.source.sourceID]),
   ];
   for (const ids of groups) {
      const sharesGenerated = ids.some((id) => {
         const url = urlOf(id);
         return (
            url !== undefined &&
            generated.generatedURLs.has(url) &&
            !ownerByURL.has(url)
         );
      });
      if (sharesGenerated) continue;
      for (const id of ids) {
         const url = urlOf(id);
         const owner = url === undefined ? undefined : ownerByURL.get(url);
         if (owner === undefined) continue;
         const name = splitSourceID(id)!.name;
         const names = out.get(owner) ?? [];
         if (!names.includes(name)) names.push(name);
         out.set(owner, names.sort());
      }
   }
   return out;
}

/**
 * The field of the build copy's source a hidden column is read through: an
 * inlined join's column through its join (the join is what is hidden, and
 * what the lift must make readable), any other through its own dimension.
 */
function liftedName(c: { dimension: string; join?: string }): string {
   return c.join ?? c.dimension;
}

/**
 * Give each plan that needs one a build copy of the author's model, whose
 * text is read back here: a plan with given-reading terms or
 * current-time terms holding an upper bound (Rolling windows), which the copy
 * takes out; an extension table that builds over its parent's build
 * source, whose copy re-roots the parent on it; and a plan with hidden
 * columns, whose own access modifiers the copy lifts. One copy does all
 * three a plan needs.
 *
 * Given terms: a source with given-reading `where:` terms must be declared in
 * that model and read givens declared in local files; otherwise, or when the
 * file cannot be read or a term not placed in it, the plan is refused (fail
 * closed: built from the author's file, the term would be baked into the table
 * with the given's default). Each given declared in another file is imported
 * from that file. A current-time term the build leaves out must be written in
 * the model file itself, its own declaration or a base declared there;
 * otherwise the plan is refused the same way (built from the author's file, the
 * table would hold only the rows older than the build's cut-off).
 *
 * Extension tables: one whose copy cannot be written (its parent no longer
 * planned, a declaration that does not re-root) builds over the raw chain
 * instead, which a source with given-reading terms or hidden fields still gets
 * a copy for.
 *
 * Hidden columns: which ones the build can select is settled by compiling the
 * copy over the raw chain (`compileCopy`) and reading its source: a field
 * public there is stored, any other is dropped from the plan and stays
 * computed at read (live), such as a field private on a source this one
 * extends. The same set is stored whether or not the plan chains, so a
 * chained build (whose parent's re-root reads hidden parent columns as plain
 * stored ones) never stores a field the raw chain could not select. A copy
 * that cannot be made or compiled drops them all. A plan left with no column,
 * or with a read-time term over a dropped column, is refused.
 *
 * Plans are visited parents first, so a copy imports its parent's build
 * source from the parent's level file as it finally stands, and re-roots the
 * parent over the columns its table finally stores; the returned plans keep
 * `plans`' order.
 */
async function withBuildCopies(
   input: MaterializePlan[],
   modelFile: string,
   readModelText: () => Promise<string>,
   compileCopy: CopyCompiler,
   /** The author model's contents, to trace a chained copy's problems. */
   contents: Record<string, unknown> = {},
   /** Each cross-file extension's parent, by the extension's sourceID. */
   foreign: ReadonlyMap<string, CrossFileParent> = new Map(),
): Promise<{
   plans: MaterializePlan[];
   copies: Map<string, BuildCopy>;
   refused: MaterializeRefusal[];
}> {
   // An extension whose parent is no longer planned (refused after planning)
   // is refused in turn when its copy is made, never built over the raw chain.
   const plans = input;
   const copies = new Map<string, BuildCopy>();
   const refused: MaterializeRefusal[] = [];
   if (
      !plans.some(
         (p) =>
            givenReadTerms(p).length > 0 ||
            leftOutWindowTerms(p.readTimeTerms).length > 0 ||
            p.buildsOver ||
            hiddenColumnsOf(p).length > 0,
      )
   ) {
      return { plans, copies, refused };
   }
   let text: string | undefined;
   let unreadable: string | undefined;
   try {
      text = await readModelText();
   } catch (error) {
      unreadable = error instanceof Error ? error.message : String(error);
   }
   const ctx: CopyContext = {
      modelFile,
      modelFileName: path.basename(modelFile),
      text,
      unreadable,
      compileCopy,
      contents,
      copies,
      foreign,
   };
   // Parents first: a stable sort by depth in the planned chains.
   const depth = chainLevels(plans);
   const visitOrder = plans
      .map((plan, index) => ({ plan, index }))
      .sort(
         (a, b) =>
            (depth.get(a.plan.sourceID) ?? 0) -
               (depth.get(b.plan.sourceID) ?? 0) || a.index - b.index,
      );
   const kept = new Map<string, MaterializePlan>();
   const levels = new Map<string, number>();
   for (const { plan: original } of visitOrder) {
      const copied = await copiedPlan(original, ctx, kept, levels);
      if ("reason" in copied) {
         refused.push({
            sourceName: copied.plan.sourceName,
            sourceID: copied.plan.sourceID,
            reason: copied.reason,
            outcome: {
               refused: {
                  reason: copied.refusal,
                  tier: tierOf(copied.plan.persist.storage),
               },
            },
         });
         continue;
      }
      kept.set(copied.plan.sourceID, copied.plan);
      levels.set(copied.plan.sourceID, copied.level);
   }
   return {
      plans: plans.flatMap((p) => {
         const k = kept.get(p.sourceID);
         return k ? [k] : [];
      }),
      copies,
      refused,
   };
}

/** Compiles a build copy on its own, beside the author's model; never run. */
type CopyCompiler = (
   fileName: string,
   text: string,
) => Promise<{ contents: Record<string, unknown>; url: string }>;

/** What making one plan's build copy reads, and the copies made so far. */
interface CopyContext {
   modelFile: string;
   modelFileName: string;
   /** The author's model text, unless it could not be read (`unreadable`). */
   text?: string;
   unreadable?: string;
   compileCopy: CopyCompiler;
   contents: Record<string, unknown>;
   copies: Map<string, BuildCopy>;
   /** Each cross-file extension's parent, by the extension's sourceID. */
   foreign: ReadonlyMap<string, CrossFileParent>;
}

function hiddenColumnsOf(plan: MaterializePlan): MaterializePlan["columns"] {
   return plan.columns.filter((c) => c.access !== undefined);
}

function localPath(url: string): string | undefined {
   try {
      return path.resolve(fileURLToPath(url));
   } catch {
      return undefined;
   }
}

/**
 * One plan as its build copy leaves it, with its level in the chain, and its
 * copy added to `ctx.copies` when it needs one; or why it is refused.
 * `kept` and `levels` hold the plans visited before it, parents first.
 */
async function copiedPlan(
   original: MaterializePlan,
   ctx: CopyContext,
   kept: ReadonlyMap<string, MaterializePlan>,
   levels: ReadonlyMap<string, number>,
): Promise<
   | { plan: MaterializePlan; level: number }
   | { plan: MaterializePlan; reason: string; refusal: RefusedReason }
> {
   const { modelFile, modelFileName, text } = ctx;
   const declaredIn = original.declaration
      ? localPath(original.declaration.url)
      : undefined;
   const lifted = await liftedColumnsOf(original, declaredIn, ctx);
   const columns = original.columns.filter(
      (c) => c.access === undefined || lifted?.has(liftedName(c)),
   );
   let plan: MaterializePlan =
      columns.length === original.columns.length
         ? original
         : { ...original, columns };
   const liftAccess = lifted !== undefined;
   const given = givenReadTerms(plan).length > 0;
   const givens = (plan.givenDeclarations ?? []).map((g) => ({
      ...g,
      file: localPath(g.url),
   }));
   const windowsOut = leftOutWindowTerms(plan.readTimeTerms);
   const reason = copyRefusal(plan, ctx, {
      declaredIn,
      given,
      givens,
      windowsOut,
   });
   if (reason) return { plan, ...reason };
   const leftOut = given || windowsOut.length > 0;
   const foreign = plan.buildsOver?.crossFile
      ? ctx.foreign.get(plan.sourceID)
      : undefined;
   const parent = plan.buildsOver?.crossFile
      ? foreign?.plan
      : plan.buildsOver
        ? kept.get(plan.buildsOver.sourceID)
        : undefined;
   const unchained = (why: string) => ({
      plan,
      reason: unchainedReason(plan, why),
      refusal: UPSTREAM_TABLE_UNUSABLE_REASON,
   });
   if (plan.buildsOver && !parent) {
      return unchained(
         "which is not planned, so it has no table to build over",
      );
   }
   if (plan.buildsOver && text === undefined) {
      return unchained(
         `and its model text could not be read to write its build over that ` +
            `table${ctx.unreadable ? ` (${ctx.unreadable})` : ""}`,
      );
   }
   if (
      plan.buildsOver &&
      localPath(plan.buildsOver.declaration.url) !==
         (foreign ? foreign.modelFile : modelFile)
   ) {
      return unchained(
         "declared in a file its build copy cannot re-root it in",
      );
   }
   // A `storage=` pair also carries what its build compiles in the
   // destination over the parent's stored table, lifted as its build copy
   // is; without it, the raw chain.
   if (plan.buildsOver && parent && plan.persist.storage !== undefined) {
      const overStored =
         foreign && !foreign.carriesAlike
            ? undefined
            : storedChainText(text as string, plan, parent, {
                 liftAccess,
                 ...(foreign
                    ? {
                         parentText: foreign.text,
                         between: new Map(
                            foreign.via.map((v) => [
                               pathToFileURL(v.modelFile).href,
                               v.text,
                            ]),
                         ),
                      }
                    : {}),
              });
      if (!overStored) {
         return unchained(
            "whose stored table its build cannot be written over in its destination",
         );
      }
      plan = { ...plan, buildsOver: { ...plan.buildsOver, overStored } };
   }
   if (plan.buildsOver?.crossFile && foreign) {
      const made = crossFileCopy(plan, foreign, ctx, liftAccess);
      if (made) {
         ctx.copies.set(plan.sourceID, made);
         return {
            plan: given ? withGivenImports(plan, givens, modelFile) : plan,
            level: 0,
         };
      }
      return unchained(
         "declared in another file, whose chain its build copy cannot carry",
      );
   }
   if (!leftOut && !plan.buildsOver && !liftAccess) return { plan, level: 0 };
   const parentLevel = parent ? (levels.get(parent.sourceID) ?? 0) : 0;
   const chainOf = (p: MaterializePlan) =>
      p.buildsOver && parent
         ? {
              parent,
              importFrom: levelFileName(modelFileName, parentLevel),
           }
         : undefined;
   // One copy per source: its terms taken out, its parent re-rooted, and
   // its access modifiers lifted, as it needs.
   const copy = buildCopyText(text as string, plan, chainOf(plan), {
      liftAccess,
   });
   if ("reason" in copy && plan.buildsOver) {
      return unchained(
         `whose table its build copy cannot re-root: ${copy.reason}`,
      );
   }
   if ("reason" in copy) {
      return { plan, reason: copy.reason, refusal: "generated_build_failed" };
   }
   const chain = chainOf(plan);
   const located = plan;
   ctx.copies.set(plan.sourceID, {
      fileName: buildCopyName(modelFileName, ctx.copies.size),
      text: copy.copy,
      ...(chain
         ? {
              declarationAt: (at: { line: number; character: number }) =>
                 buildCopyDeclarationAt(
                    text as string,
                    located,
                    chain,
                    { liftAccess },
                    ctx.contents,
                    at,
                 ),
           }
         : {}),
   });
   return {
      plan: given ? withGivenImports(plan, givens, modelFile) : plan,
      level: plan.buildsOver ? parentLevel + 1 : 0,
   };
}

/**
 * Hidden fields: the columns the build copy lets the build select, read off the
 * copy compiled over the raw chain, when it lets it select at least one of the
 * plan's hidden columns; undefined otherwise (none is lifted).
 */
async function liftedColumnsOf(
   original: MaterializePlan,
   declaredIn: string | undefined,
   ctx: CopyContext,
): Promise<Set<string> | undefined> {
   const hidden = hiddenColumnsOf(original);
   if (
      hidden.length === 0 ||
      declaredIn !== ctx.modelFile ||
      ctx.text === undefined
   ) {
      return undefined;
   }
   const probe = buildCopyText(ctx.text, withoutParent(original), undefined, {
      liftAccess: true,
   });
   if (!("copy" in probe)) return undefined;
   const compiled = await ctx
      .compileCopy(
         buildCopyName(ctx.modelFileName, ctx.copies.size),
         probe.copy,
      )
      .catch(() => undefined);
   // The copy's own declaration of the source: its sourceID names
   // the copy's file in place of the author's (the keying rule).
   const declared = splitSourceID(original.sourceID);
   const copyID =
      compiled && declared ? `${declared.name}@${compiled.url}` : undefined;
   const fields = (
      Object.values(compiled?.contents ?? {}).find(
         (d) => (d as { sourceID?: unknown } | null)?.sourceID === copyID,
      ) as
         | {
              fields?: {
                 name?: unknown;
                 as?: unknown;
                 accessModifier?: unknown;
              }[];
           }
         | undefined
   )?.fields;
   const selectable = new Set(
      (fields ?? [])
         .filter((f) => hiddenAccess(f) === undefined)
         .map((f) => String(f.as ?? f.name)),
   );
   return hidden.some((c) => selectable.has(liftedName(c)))
      ? selectable
      : undefined;
}

/**
 * Why a plan cannot be given the build copy it needs, or undefined: no column
 * left, a read-time term over a column the build cannot select, a given term
 * outside local files, a term the build cannot leave out of another file, or
 * a model file that could not be read.
 */
function copyRefusal(
   plan: MaterializePlan,
   ctx: Pick<CopyContext, "modelFile" | "text" | "unreadable">,
   facts: {
      declaredIn: string | undefined;
      given: boolean;
      givens: { file: string | undefined }[];
      windowsOut: ReturnType<typeof leftOutWindowTerms>;
   },
): { reason: string; refusal: RefusedReason } | undefined {
   const { modelFile, text, unreadable } = ctx;
   const { declaredIn, given, givens, windowsOut } = facts;
   if (plan.columns.length === 0) {
      return {
         reason: "the source has no dimension the build can store",
         refusal: "no_stored_columns",
      };
   }
   const stored = new Set(plan.columns.map((c) => c.column));
   const unstored = (plan.readTimeTerms ?? []).find(
      (t) => !t.columns.every((c) => stored.has(c)),
   );
   if (unstored) {
      return {
         reason:
            `\`where: ${unstored.code}\` reads a field the build cannot ` +
            "select (one private on a source this one extends, or hidden " +
            "where the build copy cannot lift it), so it could not be " +
            "re-applied over the table at read",
         refusal:
            unstored.kind === "given"
               ? "given_where_over_unstored_field"
               : "current_time",
      };
   }
   if (given && (declaredIn !== modelFile || givens.some((g) => !g.file))) {
      return {
         reason:
            "a `where:` reading a given can only be left out of the build " +
            "for a source declared in a local model file, reading givens " +
            "declared in local files",
         refusal: "reads_given",
      };
   }
   if (given && text === undefined) {
      return {
         reason:
            "the model file could not be read to leave its given-reading " +
            `\`where:\` out of the build: ${unreadable}`,
         refusal: "reads_given",
      };
   }
   // An upper-bound current-time term (option A): taken out of the copy
   // wherever this file writes it, the source's own or a base's.
   const elsewhere = windowsOut.find((t) =>
      t.at.some((use) => localPath(use.url) !== modelFile),
   );
   if (elsewhere) {
      return {
         reason:
            `\`where: ${elsewhere.code}\` keeps rows older than a moving ` +
            "cut-off and is written in another file than this source's " +
            "model, so the build cannot leave it out. Write it in this " +
            "file, or materialize the source in the file that writes it",
         refusal: "current_time",
      };
   }
   if (windowsOut.length > 0 && text === undefined) {
      return {
         reason:
            "the model file could not be read to leave its `where:` keeping " +
            `rows older than a moving cut-off out of the build: ${unreadable}`,
         refusal: "current_time",
      };
   }
   return undefined;
}

/**
 * The plan with each given its terms read imported from where it is declared:
 * a given declared in another file than the model's, by its path from it.
 */
function withGivenImports(
   plan: MaterializePlan,
   givens: (NonNullable<MaterializePlan["givenDeclarations"]>[number] & {
      file: string | undefined;
   })[],
   modelFile: string,
): MaterializePlan {
   return {
      ...plan,
      givenDeclarations: givens.map(({ file, ...g }) =>
         file === modelFile
            ? g
            : {
                 ...g,
                 from: path
                    .relative(path.dirname(modelFile), file as string)
                    .split(path.sep)
                    .join("/"),
              },
      ),
   };
}

/**
 * A generated companion that did not compile, with what is needed to tell
 * whose generated text the failure is in: every generated file's text by URL,
 * the source each build copy is for by its URL, and, when known outright, the
 * failing source.
 */
class CompanionCompileError extends Error {
   constructor(
      readonly error: unknown,
      readonly overlay: ReadonlyMap<string, string>,
      readonly copyOwners: ReadonlyMap<string, string>,
      readonly sourceID?: string,
   ) {
      super(error instanceof Error ? error.message : String(error));
   }
}

/** A compile problem's message and where it is, as Malloy reports it. */
interface CompileProblem {
   message?: unknown;
   at?: { url?: unknown; range?: { start?: { line?: unknown } } };
}

/**
 * The planned source whose generated text a companion's compile failure is
 * in, by sourceID, or undefined when it cannot be told. A problem in a build
 * copy is the copy's source's (a copy is one source's text, and a problem in
 * one leaves the files importing it with problems of their own, so a copy's
 * comes first); one in a generated file is the plan named on its line, or on
 * the nearest line above it naming exactly one plan (a `select:` list sits
 * under its build source's line).
 */
function failingSourceOf(
   failure: CompanionCompileError,
   plans: readonly MaterializePlan[],
): string | undefined {
   if (failure.sourceID !== undefined) return failure.sourceID;
   const problems = ((failure.error as { problems?: unknown } | null)
      ?.problems ?? []) as CompileProblem[];
   const located = problems.filter(
      (p): p is CompileProblem & { at: { url: string } } =>
         typeof p?.at?.url === "string",
   );
   const inCopy = located.find((p) => failure.copyOwners.has(p.at.url));
   if (inCopy) return failure.copyOwners.get(inCopy.at.url);
   const names = plans.map((plan) => ({
      sourceID: plan.sourceID,
      patterns: generatedNamesOf(plan).map(
         (name) =>
            new RegExp(
               `(^|[^A-Za-z0-9_])${name.replace(/[^A-Za-z0-9_]/g, "\\$&")}($|[^A-Za-z0-9_])`,
            ),
      ),
   }));
   for (const problem of located) {
      const text = failure.overlay.get(problem.at.url);
      const line = problem.at.range?.start?.line;
      if (text === undefined || typeof line !== "number") continue;
      const lines = text.split("\n");
      for (let at = Math.min(line, lines.length - 1); at >= 0; at--) {
         const named = names.filter((n) =>
            n.patterns.some((re) => re.test(lines[at])),
         );
         if (named.length === 1) return named[0].sourceID;
         if (named.length > 1) return undefined;
      }
      return undefined;
   }
   return undefined;
}

/**
 * The declarations off the chain that the problems in `plan`'s chained build
 * copy lie in, each with its range in the author's file and each named once,
 * leaving out any the copy already omits. Empty when every problem is on the
 * chain (the plan's own declaration, its parent's or one between) or in no
 * declaration. All of them at once, so a file with many siblings that do not
 * compile over the re-root settles in one round rather than one per sibling.
 */
function copyOmissions(
   failure: CompanionCompileError,
   copy: BuildCopy | undefined,
   plan: MaterializePlan,
): { sourceID: string; declaration: TermUse }[] {
   if (!copy?.declarationAt) return [];
   const url = [...failure.copyOwners].find(
      ([, id]) => id === plan.sourceID,
   )?.[0];
   if (url === undefined) return [];
   const problems = ((failure.error as { problems?: unknown } | null)
      ?.problems ?? []) as CompileProblem[];
   const key = (d: TermUse) =>
      `${d.url}:${d.range.start.line}:${d.range.start.character}`;
   const seen = new Set((plan.buildsOver?.omit ?? []).map(key));
   const out: { sourceID: string; declaration: TermUse }[] = [];
   for (const problem of problems) {
      if (problem?.at?.url !== url) continue;
      const start = problem.at.range?.start as
         | { line?: unknown; character?: unknown }
         | undefined;
      if (typeof start?.line !== "number") continue;
      const found = copy.declarationAt({
         line: start.line,
         character: typeof start.character === "number" ? start.character : 0,
      });
      if (found === undefined || seen.has(key(found.declaration))) continue;
      seen.add(key(found.declaration));
      out.push(found);
   }
   return out;
}

/** The first problem's message, or the error's own, for a refusal reason. */
function firstProblem(failure: CompanionCompileError): string {
   const [problem] = ((failure.error as { problems?: unknown } | null)
      ?.problems ?? []) as CompileProblem[];
   const message =
      typeof problem?.message === "string" ? problem.message : failure.message;
   return message.split("\n")[0];
}

/**
 * How many rounds one chained source may leave declarations out of its build
 * copy before it builds over the raw chain instead. Each round leaves out
 * every declaration its copy's problems lie in, so more rounds are needed
 * only where leaving one out fails another that reads it.
 */
const MAX_OMISSION_ROUNDS = 4;

/**
 * Prepare `from` (build copies) and compile its companion, so that
 * a failure in one source's generated text costs that source alone. The
 * source a failure is in ({@link failingSourceOf}) is an extension table
 * built over its parent's build source: it alone builds over the raw
 * chain instead, with the same rows, and every other extension keeps reading
 * its parent's table. Any other source is refused (`refuse`, reported as
 * `materialize-not-built`) and answers live, and the rest are compiled again.
 * A failure no source can be told for sends every extension to the raw chain
 * once; one that still cannot be told throws, costing the model its builds
 * (logged by the caller as a Publisher bug).
 */
async function compileContained(
   args: Parameters<typeof compileCompanion>[0],
   from: MaterializePlan[],
   prepare: (
      plans: MaterializePlan[],
   ) => Promise<{ plans: MaterializePlan[]; copies: Map<string, BuildCopy> }>,
   refuse: (refusal: MaterializeRefusal) => void,
): Promise<{
   plans: MaterializePlan[];
   copies: Map<string, BuildCopy>;
   generated?: GeneratedMaterialize;
}> {
   let input = from;
   // How many rounds each chained source has left declarations out of its
   // copy. Past MAX_OMISSION_ROUNDS it builds over the raw chain instead.
   const omissionRounds = new Map<string, number>();
   // Each round leaves declarations out of one copy (at most
   // MAX_OMISSION_ROUNDS times per source), unchains or drops one source, or
   // unchains every one once, so this many rounds always settle.
   const rounds = (MAX_OMISSION_ROUNDS + 2) * from.length + 2;
   for (let round = 0; round < rounds; round++) {
      const { plans, copies } = await prepare(input);
      try {
         return {
            plans,
            copies,
            generated: await compileCompanion(args, plans, copies),
         };
      } catch (error) {
         if (!(error instanceof CompanionCompileError)) throw error;
         input = nextRound(error, plans, copies, {
            modelPath: args.modelPath,
            omissionRounds,
            refuse,
         });
      }
   }
   throw new Error(
      "the generated #(materialize) companion did not settle on sources that compile",
   );
}

/**
 * The plans the next round of {@link compileContained} compiles, after
 * `error` in this round's: the failing chained copy leaves the declarations
 * its problems lie in out, or builds over the raw chain; any other failing
 * source is refused alone; a failure no source can be told for sends every
 * extension to the raw chain. One that still cannot be told throws.
 */
function nextRound(
   error: CompanionCompileError,
   plans: MaterializePlan[],
   copies: Map<string, BuildCopy>,
   ctx: {
      modelPath: string;
      omissionRounds: Map<string, number>;
      refuse: (refusal: MaterializeRefusal) => void;
   },
): MaterializePlan[] {
   const { modelPath, omissionRounds } = ctx;
   const failing = failingSourceOf(error, plans);
   const plan = failing === undefined ? undefined : planOf(plans, failing);
   // In a chained copy, a problem in another source's declaration (one that
   // reads the parent and does not compile over its re-root) is that
   // source's: the copy leaves it out, empty, and keeps the chain.
   const omitted =
      plan?.buildsOver &&
      (omissionRounds.get(plan.sourceID) ?? 0) < MAX_OMISSION_ROUNDS
         ? copyOmissions(error, copies.get(plan.sourceID), plan)
         : [];
   if (plan?.buildsOver && omitted.length > 0) {
      omissionRounds.set(
         plan.sourceID,
         (omissionRounds.get(plan.sourceID) ?? 0) + 1,
      );
      logger.warn(
         "A chained #(materialize) build copy leaves out declarations that do not compile over the parent's build source",
         {
            modelPath,
            sourceName: plan.sourceName,
            omitted: omitted.map((o) => splitSourceID(o.sourceID)?.name),
         },
      );
      const omit = omitted.map((o) => o.declaration);
      return plans.map((p) =>
         p === plan && p.buildsOver
            ? {
                 ...p,
                 buildsOver: {
                    ...p.buildsOver,
                    omit: [...(p.buildsOver.omit ?? []), ...omit],
                 },
              }
            : p,
      );
   }
   if (plan?.buildsOver) {
      logger.warn(
         "An extension table of a #(materialize) model did not compile over its parent's build source; it is refused",
         { modelPath, sourceName: plan.sourceName, code: errorCodeOf(error) },
      );
      ctx.refuse(
         unchainedRefusal(
            plan,
            `whose table its build does not compile over: ${firstProblem(error)}`,
         ),
      );
      return plans.filter((p) => p !== plan);
   }
   if (plan) {
      logger.error(
         "A #(materialize) source's generated build did not compile; it is not built, and the model's other sources are",
         { modelPath, sourceName: plan.sourceName, code: errorCodeOf(error) },
      );
      ctx.refuse({
         sourceName: plan.sourceName,
         sourceID: plan.sourceID,
         outcome: {
            refused: {
               reason: "generated_build_failed",
               tier: tierOf(plan.persist.storage),
            },
         },
         reason:
            "its generated build did not compile, so it is not built " +
            `(a Publisher bug; please report it): ${firstProblem(error)}`,
      });
      return plans.filter((p) => p !== plan);
   }
   if (plans.some((p) => p.buildsOver)) {
      // No source to blame: every extension over its parent's table is
      // refused, never built over the raw chain.
      logger.warn(
         "Extension tables of a #(materialize) model did not compile over their parents' build sources; they are refused",
         { modelPath, code: errorCodeOf(error) },
      );
      for (const p of plans.filter((q) => q.buildsOver)) {
         ctx.refuse(
            unchainedRefusal(
               p,
               `and the model's builds over their parents' tables do not ` +
                  `compile: ${firstProblem(error)}`,
            ),
         );
      }
      return plans.filter((p) => !p.buildsOver);
   }
   throw error.error;
}

/**
 * The parent of an extension table declared in another file (across
 * files), as that file's own companion plans it.
 */
interface CrossFileParent {
   /** The parent's plan, as its own file's companion finally has it. */
   plan: MaterializePlan;
   /** The parent's model file, as an absolute path. */
   modelFile: string;
   /** That file's text as the compile reads it. */
   text: string;
   /** Every file that companion compiled over, by URL. */
   overlay: ReadonlyMap<string, string>;
   /** That companion's {@link GeneratedMaterialize.renames}. */
   renames: ReadonlyMap<string, string>;
   /** That companion's model, which declares the parent's build source. */
   model: MalloyModel;
   /**
    * The files the chain passes through between the extension's and the
    * parent's (`buildsOver.crossFile.via`), the extension's side first: each
    * one's path, text and compiled contents.
    */
   via: {
      modelFile: string;
      text: string;
      contents?: Record<string, unknown>;
   }[];
   /**
    * Whether the chain's declarations read every name alike when carried out
    * of their files into one text (a `storage=` build's, {@link
    * storedChainText}): each file imports the next whole, and no file on the
    * chain names another's chain source otherwise ({@link carriesAlike}).
    */
   carriesAlike: boolean;
}

/**
 * A model file's companion, compiled as a cross-file parent's file, or a
 * file a chain passes through; `generated` is absent when it plans nothing.
 */
interface CrossFileCompanion {
   generated?: GeneratedMaterialize;
   text: string;
   modelFile: string;
   modelDef: ReturnType<typeof modelDefOf>;
}

/**
 * {@link CrossFileCompanion}s by model file, for one package plan. Made by
 * the plan's caller ({@link newCrossFileCompanions}) and passed to every
 * model's companion compile.
 */
export type CrossFileCompanions = Map<
   string,
   Promise<CrossFileCompanion | undefined>
>;

/** An empty {@link CrossFileCompanions}, for one package plan. */
export function newCrossFileCompanions(): CrossFileCompanions {
   return new Map();
}

/** The plans, each cross-file extension with its parent set, and the parents. */
interface CrossFileParents {
   plans: MaterializePlan[];
   /** By the extension's sourceID. */
   parents: ReadonlyMap<string, CrossFileParent>;
}

const NO_CROSS_FILE_PARENTS: CrossFileParents = {
   plans: [],
   parents: new Map(),
};

/**
 * `plans` with `buildsOver` set on each extension table whose nearest
 * materialized ancestor is declared in another local file of the package
 * ({@link crossFileAncestors}), when that file's own companion plans the
 * ancestor and the pair may chain ({@link chainsOver}); with each such
 * parent. The parent's companion is compiled here, as its own model's plan
 * compiles it (the same text at the same URLs, so its build source is the
 * same build), and never twice up one stack of files.
 *
 * Any failure leaves the extension building over the raw chain, from the
 * warehouse, which gives the same rows.
 */
async function crossFileParents(
   plans: MaterializePlan[],
   args: CompanionArgs,
   modelFile: string,
): Promise<CrossFileParents> {
   const candidates = crossFileEntries(plans, args.contents);
   if (candidates.length === 0) return { plans, parents: new Map() };
   const stack = new Set([...(args.crossFileStack ?? []), modelFile]);
   const files = GeneratedFiles.over(args.getRuntime);
   const companions = args.crossFileCompanions ?? new Map();
   const companionOf = (file: string) => {
      let found = companions.get(file);
      if (!found) {
         found = (async () => {
            const relative = path.relative(args.packagePath, file);
            if (
               stack.has(file) ||
               relative === "" ||
               relative.startsWith("..") ||
               path.isAbsolute(relative)
            ) {
               return undefined;
            }
            const url = pathToFileURL(file);
            const { model, materializer } = await files.compile(url);
            const def = modelDefOf(model);
            const modelPath = relative.split(path.sep).join("/");
            const { generated, failure } = await tryCompileMaterializeCompanion(
               {
                  packagePath: args.packagePath,
                  modelPath,
                  contents: (def.contents ?? {}) as Record<string, unknown>,
                  givens: (def.givens ?? {}) as Record<string, unknown>,
                  getRuntime: args.getRuntime,
                  ...(args.probe ? { probe: materializer } : {}),
                  crossFileStack: stack,
                  crossFileCompanions: companions,
               },
            );
            if (failure) logCompanionFailure(failure, modelPath);
            // A file that plans nothing is kept too: a chain may pass
            // through it to a parent in a file beyond.
            return {
               ...(generated ? { generated } : {}),
               text: await files.read(url),
               modelFile: file,
               modelDef: def,
            };
         })().catch((error: unknown) => {
            logger.warn(
               "An extension table's parent in another file could not be planned here; it builds over the raw chain",
               {
                  modelPath: args.modelPath,
                  parentFile: path.relative(args.packagePath, file),
                  code: errorCodeOf(error),
               },
            );
            return undefined;
         });
         companions.set(file, found);
      }
      return found;
   };
   const parents = new Map<string, CrossFileParent>();
   const chained = new Map<string, MaterializePlan>();
   let ownText: Promise<string | undefined> | undefined;
   const readOwnText = () =>
      (ownText ??= files.read(pathToFileURL(modelFile)).catch(() => undefined));
   for (const { plan, entry, path: below } of candidates) {
      // The chain enters `file` at `entryID`. A file that plans no parent on
      // it and declares no `#(materialize)` there is passed through: the
      // walk goes on into the file it is built on, each companion compiled
      // once for the package plan (`companions`), so a deep chain costs one
      // compile per file.
      let file = localPath(entry.url);
      let entryID = entry.sourceID;
      let pathBelow = below;
      const via: CrossFileParent["via"] = [];
      for (let hops = 0; file !== undefined && hops < 64; hops++) {
         const companion = await companionOf(file);
         if (!companion) break;
         const found = chainOver(plan, companion, entryID, pathBelow, via);
         if (found === "stop") break;
         if (found) {
            chained.set(plan.sourceID, found.plan);
            parents.set(plan.sourceID, {
               ...found.parent,
               carriesAlike: carriesAlike(
                  { modelFile, text: await readOwnText() },
                  args.contents,
                  plan.sourceID,
                  via,
                  found.parent,
                  companion.modelDef,
               ),
            });
            break;
         }
         const through = chainThroughFile(
            companion.modelDef as {
               contents?: unknown;
               sourceRegistry?: unknown;
            },
            entryID,
         );
         // A file between that declares `#(materialize)` on the chain, planned
         // or refused, is no pass-through: the extension builds from the
         // warehouse.
         if (through.declares || !through.next) break;
         const next = localPath(through.next.url);
         if (next === undefined || next === file) break;
         via.push({
            modelFile: file,
            text: companion.text,
            contents: (
               companion.modelDef as { contents?: Record<string, unknown> }
            ).contents,
         });
         pathBelow = [...through.declarations, ...pathBelow];
         entryID = through.next.sourceID;
         file = next;
      }
   }
   return {
      plans: plans.map((p) => chained.get(p.sourceID) ?? p),
      parents,
   };
}

/**
 * The parent the chain entering `companion`'s file at `entryID` builds over
 * there: the nearest source on the chain that file's own companion plans
 * ({@link chainWithinFile}; that file's compile, not this one's, tells its
 * own `#(materialize)` from one it shares with its base), with the
 * extension's plan given its `buildsOver`. `"stop"` when that source may not
 * be chained on ({@link chainsOver}) or its build source cannot be found;
 * undefined when the file plans no source on the chain.
 */
function chainOver(
   plan: MaterializePlan,
   companion: CrossFileCompanion,
   entryID: string,
   below: TermUse[],
   via: CrossFileParent["via"],
):
   | { plan: MaterializePlan; parent: Omit<CrossFileParent, "carriesAlike"> }
   | "stop"
   | undefined {
   const generated = companion.generated;
   if (!generated) return undefined;
   for (const candidate of chainWithinFile(
      companion.modelDef as { contents?: unknown; sourceRegistry?: unknown },
      entryID,
   )) {
      const parent = generated.plans.find(
         (p: MaterializePlan) => p.sourceID === candidate.sourceID,
      );
      if (!parent) continue;
      if (!chainsOver(plan, parent)) return "stop";
      const buildSourceID = (
         modelDefOf(generated.model).contents?.[parent.buildSourceName] as
            | { sourceID?: unknown }
            | undefined
      )?.sourceID;
      const buildSourceURL =
         typeof buildSourceID === "string"
            ? splitSourceID(buildSourceID)?.url
            : undefined;
      if (buildSourceURL === undefined) return "stop";
      return {
         plan: {
            ...plan,
            buildsOver: {
               sourceID: parent.sourceID,
               sourceName: parent.sourceName,
               declaration: candidate.declaration,
               path: [...candidate.path, ...below],
               crossFile: {
                  modelFile: companion.modelFile,
                  buildSourceURL,
                  ...(via.length > 0
                     ? { via: via.map((v) => v.modelFile) }
                     : {}),
               },
            },
         },
         parent: {
            plan: parent,
            modelFile: companion.modelFile,
            text: companion.text,
            overlay: generated.files,
            renames: generated.renames,
            model: generated.model,
            via: [...via],
         },
      };
   }
   return undefined;
}

/**
 * Whether the declarations on a cross-file chain read every name alike when
 * carried out of their files into one text, as a `storage=` build compiles
 * them ({@link storedChainText}), the rule a served copy's kept blocks keep
 * across files: each file on the chain imports the next one whole (a
 * selective import may leave a name out for the importing file to declare
 * itself), and no file on the chain binds the name of a chain source declared
 * in another file to any other source. Otherwise a carried declaration could
 * read, in the one text, a source its own file never meant.
 */
function carriesAlike(
   own: { modelFile: string; text: string | undefined },
   ownContents: Record<string, unknown>,
   extensionID: string,
   via: CrossFileParent["via"],
   parent: Omit<CrossFileParent, "carriesAlike">,
   parentDef: unknown,
): boolean {
   const parentContents = (parentDef as { contents?: Record<string, unknown> })
      ?.contents;
   const chainFiles = [
      { modelFile: own.modelFile, text: own.text, contents: ownContents },
      ...via,
      {
         modelFile: parent.modelFile,
         text: parent.text,
         contents: parentContents,
      },
   ];
   for (let k = 0; k + 1 < chainFiles.length; k++) {
      const from = chainFiles[k];
      if (from.text === undefined) return false;
      const whole = wholeImportsOf(
         from.text,
         pathToFileURL(from.modelFile).href,
      );
      const next = pathToFileURL(chainFiles[k + 1].modelFile).href;
      if (!whole?.includes(next)) return false;
   }
   // The chain's sources, by file: the extension's own file's from the
   // extension down to where it leaves the file, and the parent's and each
   // file between's from the plan's path.
   const chainIDs = new Set<string>([parent.plan.sourceID]);
   const map = SourceMap.of({ contents: ownContents });
   let node = map.node(extensionID);
   for (let hops = 0; node && hops < 64; hops++) {
      chainIDs.add(node.sourceID);
      const base = node.base;
      if (!base || base.hop === "query" || base.sourceID === undefined) break;
      chainIDs.add(base.sourceID);
      node = map.node(base.sourceID);
   }
   for (const file of chainFiles) {
      const href = pathToFileURL(file.modelFile).href;
      for (const id of chainIDs) {
         const split = splitSourceID(id);
         if (!split || split.url === href) continue;
         const bound = (
            file.contents?.[split.name] as { sourceID?: unknown } | undefined
         )?.sourceID;
         if (bound !== undefined && bound !== id) return false;
      }
   }
   return true;
}

/**
 * A cross-file extension's build copy ({@link crossFileCopyTexts}): its own
 * file with its own edits and its `import`s of the parent's file pointed at
 * the parent's file re-rooted on the parent's build source; undefined when
 * it cannot be written, and the extension builds over the raw chain.
 */
function crossFileCopy(
   plan: MaterializePlan,
   parent: CrossFileParent,
   ctx: CopyContext,
   liftAccess: boolean,
): BuildCopy | undefined {
   const over = plan.buildsOver;
   if (!over?.crossFile || ctx.text === undefined) return undefined;
   const posix = (from: string, to: string) => {
      const relative = path.relative(from, to).split(path.sep).join("/");
      return relative.startsWith(".") ? relative : `./${relative}`;
   };
   let buildSourceFile: string;
   try {
      buildSourceFile = fileURLToPath(over.crossFile.buildSourceURL);
   } catch {
      return undefined;
   }
   // Each file on the chain past the extension's, and its copy beside it:
   // the files between, then the parent's.
   const copyOf = new Map<string, string>(
      [...parent.via.map((v) => v.modelFile), parent.modelFile].map((file) => [
         pathToFileURL(file).href,
         path.join(
            path.dirname(file),
            chainCopyName(
               path.basename(file),
               ctx.modelFileName,
               ctx.copies.size,
            ),
         ),
      ]),
   );
   // An import, written in `fromFile`, of a file on the chain points at
   // that file's copy instead.
   const importOf = (fromFile: string) => (written: string) => {
      let href: string;
      try {
         href = new URL(written, pathToFileURL(fromFile)).href;
      } catch {
         return undefined;
      }
      const copy = copyOf.get(href);
      return copy === undefined
         ? undefined
         : posix(path.dirname(fromFile), copy);
   };
   const parentDir = path.dirname(parent.modelFile);
   const made = crossFileCopyTexts({
      text: ctx.text,
      parentText: parent.text,
      plan,
      parent: parent.plan,
      buildSourceFrom: posix(parentDir, buildSourceFile),
      importOf: importOf(ctx.modelFile),
      between: parent.via.map((v) => ({
         text: v.text,
         importOf: importOf(v.modelFile),
      })),
      liftAccess,
   });
   if ("reason" in made) {
      logger.warn(
         "An extension table's build copy could not read its parent in another file re-rooted; it builds over the raw chain",
         { sourceName: plan.sourceName, reason: made.reason },
      );
      return undefined;
   }
   const urlOf = (file: string) =>
      pathToFileURL(copyOf.get(pathToFileURL(file).href)!).href;
   return {
      fileName: buildCopyName(ctx.modelFileName, ctx.copies.size),
      text: made.copy,
      chainCopies: [
         ...parent.via.map((v, k) => ({
            url: urlOf(v.modelFile),
            text: made.betweenCopies[k],
         })),
         { url: urlOf(parent.modelFile), text: made.parentCopy },
      ],
   };
}

/**
 * The round with every cross-file extension whose build over its parent's
 * build source does not keep the columns, and each column's type, its build
 * over the raw chain has, refused (`upstream_table_unusable`): compared field by
 * field with the author's compiled source, or, for a column of an inlined
 * join, with the companion compiled once more without those parents. A re-rooted
 * parent changes where the rows are read from, never which columns the
 * extension stores; the compiler, not the rewrite, is what says so.
 */
async function withCrossFileColumnsKept(
   round: CompanionRound,
   compile: CompanionCompile,
   args: CompanionArgs,
   refused: MaterializeRefusal[],
   foreign: CrossFileParents,
): Promise<CompanionRound> {
   const chained = round.plans.filter((p) => p.buildsOver?.crossFile);
   if (!round.generated || chained.length === 0) return round;
   // The raw chain's shape: each stored column as the author's compiled
   // source types its field, which is what the build over the raw chain
   // selects. Compiled again over the raw chain only where that cannot be
   // read (a column of an inlined join).
   const authored = (p: MaterializePlan) => {
      if (p.columns.some((c) => c.join !== undefined)) return undefined;
      const def = Object.values(args.contents).find(
         (d) => (d as { sourceID?: unknown } | null)?.sourceID === p.sourceID,
      ) as
         | { fields?: { name?: unknown; as?: unknown; type?: unknown }[] }
         | undefined;
      const types = new Map(
         (def?.fields ?? []).map((f) => [String(f.as ?? f.name), f.type]),
      );
      const out: [string, string][] = [];
      for (const c of p.columns) {
         const type = types.get(c.dimension);
         if (typeof type !== "string") return undefined;
         out.push([c.column, type]);
      }
      return JSON.stringify(out);
   };
   let raw: CompanionRound | undefined;
   const rawShape = async (p: MaterializePlan) => {
      const fromAuthor = authored(p);
      if (fromAuthor !== undefined) return fromAuthor;
      raw ??= await companionCompiler(
         args,
         path.resolve(args.packagePath, args.modelPath),
         [],
      )(
         foreign.plans.map((q) =>
            q.buildsOver?.crossFile ? withoutParent(q) : q,
         ),
      );
      return shape(raw.generated, p.buildSourceName);
   };
   const shape = (
      generated: GeneratedMaterialize | undefined,
      name: string,
   ) => {
      const def = (
         generated
            ? modelDefIfAny(
                 generated.model as unknown as {
                    _modelDef?: { contents?: Record<string, unknown> };
                 },
              )?.contents?.[name]
            : undefined
      ) as
         | { fields?: { name?: unknown; as?: unknown; type?: unknown }[] }
         | undefined;
      return def?.fields
         ? JSON.stringify(
              def.fields.map((f) => [String(f.as ?? f.name), String(f.type)]),
           )
         : undefined;
   };
   const differ: MaterializePlan[] = [];
   for (const p of chained) {
      const kept = shape(round.generated, p.buildSourceName);
      if (kept === undefined || kept !== (await rawShape(p))) differ.push(p);
   }
   if (differ.length === 0) return round;
   for (const plan of differ) {
      logger.warn(
         "An extension table over its parent in another file does not keep its columns over the parent's build source; it is refused",
         { modelPath: args.modelPath, sourceName: plan.sourceName },
      );
      refuseUnchained(
         refused,
         plan,
         `declared in another file, whose table its build would not store ` +
            `the same columns, with the same types, over`,
      );
   }
   const ids = new Set(differ.map((p) => p.sourceID));
   return compile(round.plans.filter((p) => !ids.has(p.sourceID)));
}

/** How a companion failure is logged, by its kind. */
const COMPANION_FAILURE_TEXT: Record<CompanionFailure["kind"], string> = {
   materialize:
      "Failed to plan or compile the generated #(materialize) companion",
   persisted_view:
      "Failed to plan or compile the generated persisted-view companion",
   rollup: "Failed to plan or compile the generated #(rollup) companion",
   rollup_blanking:
      "A #(rollup) companion was not built: a `#@` line of the author's files could not be left out of it",
   v1_build:
      "The v1 build copy of a model could not be read or did not compile; its #@ persist sources are left out of the build plan",
};

/**
 * Log a generated companion's failure once, at `error`: a publisher bug,
 * since the text is generated.
 */
export function logCompanionFailure(
   failure: CompanionFailure,
   modelPath: string,
): void {
   logger.error(COMPANION_FAILURE_TEXT[failure.kind], {
      modelPath,
      code: errorCodeOf(failure.error),
   });
}
