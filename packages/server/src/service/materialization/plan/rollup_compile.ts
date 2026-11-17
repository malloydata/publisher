// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated `#(preaggregate)` build companion (see rollup_plan.ts),
 * as materialize_compile.ts compiles the `#(materialize)` one: the text is
 * served from an overlay at a URL beside the declaring model, so its `import`
 * resolves while it never shows up in `getModelPaths()`, the API or the
 * package's own compile.
 *
 * **Planned over the author's rows** (pinned in preaggregate_variants.spec.ts).
 * The companion reads the author's files with every source-level `#@` line
 * blanked (positions kept), so Malloy's planner sees no persisted source under
 * a rollup: the plan's addresses, edges and SQL are the author's rows'. That
 * is what lets a rollup over a `#@ persist storage=` base build at all (the
 * chained storage build lifts a downstream's definition from its file, and a
 * rollup has none). The content address is unchanged by the blanking: Malloy
 * keys a target on its fully inlined SQL.
 *
 * **Built over a colocated base's stored table when a run holds it.** A
 * colocated rollup over a colocated `#@ persist` base has a second compile,
 * made only when a run asks ({@link GeneratedRollups.overStoredBases}), with
 * the base's own `#@ persist` kept: the run builds the rollup from that
 * source, at the same address, only when its build manifest holds the base's
 * table (instructed_build.ts), so the build reads the table, not the
 * warehouse. The rows are the base's rows either way.
 *
 * A colocated rollup over a colocated `#(materialize)` base is offered the
 * same, in the same second compile: Malloy sees no `#@ persist` on the base,
 * so the companion imports the base from a copy of its file that re-roots it
 * on its generated build source (as an extension's build copy re-roots its
 * parent, P10), beside the `#(materialize)` companion's own files. The
 * build source is persisted, so the build reads the base's table through the
 * run's build manifest. The re-root moves the rollup's SQL, so the run checks
 * the build source, not the rollup, against the plan's addresses.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as path from "path";
import { getMaterializeEnabled } from "../../../config";
import { pathToFileURL } from "url";
import { logger } from "../../../logger";
import { queryGivenUsageIds } from "../../governance/ir/prepared_query";
import {
   companionOmissions,
   declaredInModel,
   GeneratedFiles,
   type SynthesizedRuntimeFactory,
} from "../text/generated_files";
import {
   measureRefusalKey,
   planModelRollups,
   parentKeyCheckQuery,
   readsAsJoinMany,
   rollupKeyCheckQuery,
   type JoinedKeyNeed,
   type ParentKeyNeed,
   type RollupRefusal,
} from "./rollup_plan";
import {
   generateRollupBuildFile,
   packageRelativeSourceID,
   ROLLUP_COMPANION_SUFFIX,
   readsBuildCopy,
   rollupBuildCopyName,
   rollupBuildCopyText,
} from "../text/read_text";
import { overlayKey } from "../../overlay_key";
import { PREAGGREGATE_NOT_BUILT_CODE } from "../../annotation_reader";
import type { PersistSource } from "@malloydata/malloy";
import {
   ownLevelNotes,
   type AnnotationsDef,
   type NotePosition,
} from "../../annotations";
import { routeOfNote } from "../../annotation_reader";
import {
   notesDeclarePersist,
   placementOf,
   readNotesIfAny,
} from "./declaration";
import { noteSpanKey } from "../text/persist_blanking";
import { plannedSources } from "./addressing";
import { SourceMap } from "../text/source_map";
import type { Omission } from "../domain/omitted_sources";
import {
   splitSourceID,
   type MaterializePlan,
   type OverStoredBase,
   type ParentKeyCheck,
   type RollupVariantPlan,
} from "../domain/table_plans";
import type { TermUse } from "../domain/read_time_terms";
import { declarationOf } from "./materialize_read_time";
import { storedSourceCopyText } from "./materialize_build_copy";
import {
   chainLevels,
   levelFileName,
   MATERIALIZE_COMPANION_SUFFIX,
} from "./build_file_generator";
import { rollupStoredBaseCopyName } from "../text/read_text";

/**
 * A model's `#(materialize)` companion, as rollup_compile reads it: its plans
 * and its generated files, which a rollup over one of its sources imports to
 * build from that source's stored table.
 */
export interface MaterializedBases {
   plans: readonly MaterializePlan[];
   files: ReadonlyMap<string, string>;
}

/** A `#(materialize)` base a rollup may build from the stored table of. */
interface StoredMaterializeBase {
   plan: MaterializePlan;
   /** The base's declaration in the author's model: what the copy re-roots. */
   declaration: TermUse;
   /** The generated file that declares its build source, beside the model. */
   importFrom: string;
}

interface GeneratedRollups {
   /** The rollups the companion declares. Never empty. */
   plans: RollupVariantPlan[];
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   text: string;
   /**
    * The rollups that may be built over their base's stored table, by sourceID,
    * each as the companion compiled over the author's files with its base's
    * own `#@ persist` kept ({@link overStoredBaseNotes}), so Malloy's planner
    * reads the base's table through a run's build manifest. Compiled once, on
    * the first call: a plan never builds, so only a run pays for it. Empty
    * when no rollup qualifies or the compile fails (each rollup then builds
    * from the warehouse, as it is planned).
    */
   overStoredBases(): Promise<ReadonlyMap<string, OverStoredBase>>;
   /** The sourceIDs {@link overStoredBases} may hold: the reuse candidates. */
   overStoredCandidates: ReadonlySet<string>;
   /**
    * The `storage=` rollups that may build over their base's stored table in
    * their destination ({@link storedBaseBuildsOf}), by sourceID: each with
    * its base and its own generated build file, which a run compiles there
    * over the base re-rooted on that table.
    */
   storedBaseBuilds: ReadonlyMap<string, StoredBaseBuild>;
}

/**
 * What a `storage=` rollup's build over its base's stored table compiles: the
 * rollup's own generated build file, served at `url` beside the base's model
 * (it imports the base from it), over the base re-rooted on its table there.
 */
interface StoredBaseBuild {
   baseSourceID: string;
   companion: { url: string; text: string };
}

/**
 * The `storage=` rollups that may build over their base's stored table: a
 * rollup over a base whose own notes declare `#@ persist` in the same
 * destination, read from the base itself (no build copy, so no given-reading
 * term is left out of the base's rows). Each with the one-rollup build file
 * that a run compiles in the destination over the base re-rooted on its
 * table, which reads nothing but that table (storage_build_strategy.ts).
 */
function storedBaseBuildsOf(
   map: SourceMap,
   plans: readonly RollupVariantPlan[],
   companionHref: string,
   modelFileName: string,
): Map<string, StoredBaseBuild> {
   const out = new Map<string, StoredBaseBuild>();
   for (const plan of plans) {
      if (plan.storage === undefined || readsBuildCopy(plan)) continue;
      const def = map.node(plan.baseSourceID)?.def as
         | { annotations?: AnnotationsDef }
         | undefined;
      const own = ownLevelNotes(def?.annotations).filter(
         (n) => routeOfNote(n.text) === "@",
      );
      if (!notesDeclarePersist(own)) continue;
      const tag = readNotesIfAny(own, "@");
      const placed = tag ? placementOf(tag, "persist") : undefined;
      if (
         !placed ||
         !("placement" in placed) ||
         placed.placement.tier !== "storage" ||
         placed.placement.destination !== plan.storage
      ) {
         continue;
      }
      const text = generateRollupBuildFile([plan], modelFileName);
      if (text === undefined) continue;
      out.set(`${plan.rollupSourceName}@${companionHref}`, {
         baseSourceID: plan.baseSourceID,
         companion: { url: companionHref, text },
      });
   }
   return out;
}

/**
 * The rollups that may read their base's stored table, and what their second
 * compile needs: a colocated rollup over a base whose own notes declare a
 * colocated `#@ persist` (its notes kept, `keep`), or a colocated
 * `#(materialize)` that `materialized` planned as a plain table (its copy,
 * `stored`, by the base's declared name). A rollup in a `storage=`
 * destination, or over a base built into one, reads the author's rows as
 * planned: a destination cannot be read from the base's warehouse.
 */
function overStoredBaseNotes(
   map: SourceMap,
   plans: readonly RollupVariantPlan[],
   companionHref: string,
   modelFile: string,
   materialized: MaterializedBases | undefined,
): {
   keep: Set<string>;
   rollups: Set<string>;
   stored: Map<string, StoredMaterializeBase>;
} {
   const keep = new Set<string>();
   // By the sourceID the companion declares each rollup under.
   const rollups = new Set<string>();
   const stored = new Map<string, StoredMaterializeBase>();
   for (const plan of plans) {
      // A base read from its build copy is never its stored table.
      if (plan.storage !== undefined || readsBuildCopy(plan)) continue;
      const node = map.node(plan.baseSourceID);
      const def = node?.def as { annotations?: AnnotationsDef } | undefined;
      const notes = ownLevelNotes(def?.annotations);
      const own = notes.filter((n) => routeOfNote(n.text) === "@");
      if (!notesDeclarePersist(own)) {
         const base = storedMaterializeBase(
            notes,
            def,
            plan.baseSourceID,
            modelFile,
            materialized,
         );
         if (!base) continue;
         stored.set(plan.baseDeclaredName, base);
         rollups.add(`${plan.rollupSourceName}@${companionHref}`);
         continue;
      }
      const tag = readNotesIfAny(own, "@");
      const placed = tag ? placementOf(tag, "persist") : undefined;
      if (!placed || !("placement" in placed)) continue;
      if (placed.placement.tier !== "colocated") continue;
      const spans = own.flatMap((n) => {
         const at = (n as { at?: NotePosition }).at;
         return at ? [noteSpanKey(at.url, at.range)] : [];
      });
      if (spans.length !== own.length) continue;
      for (const span of spans) keep.add(span);
      rollups.add(`${plan.rollupSourceName}@${companionHref}`);
   }
   return { keep, rollups, stored };
}

/**
 * The base `baseSourceID` as a `#(materialize)` base whose stored table a
 * rollup may build from, or undefined: its own notes declare a colocated
 * `#(materialize)` (read by declaration.ts), and the companion planned it as
 * a plain table in its own warehouse, declared in this model. A pair table
 * holds a row per pair, not the base's rows, and a table with read-time
 * terms holds rows its source filters again at read: neither is read.
 */
function storedMaterializeBase(
   notes: readonly { text: string }[],
   def: unknown,
   baseSourceID: string,
   modelFile: string,
   materialized: MaterializedBases | undefined,
): StoredMaterializeBase | undefined {
   if (!materialized) return undefined;
   const own = notes.filter((n) => routeOfNote(n.text) === "materialize");
   if (own.length === 0) return undefined;
   const tag = readNotesIfAny(own, "materialize");
   const placed = tag ? placementOf(tag) : undefined;
   if (!placed || !("placement" in placed)) return undefined;
   if (placed.placement.tier !== "colocated") return undefined;
   const plan = materialized.plans.find((p) => p.sourceID === baseSourceID);
   if (
      !plan ||
      plan.pair ||
      plan.pairBlocks ||
      plan.persist.storage !== undefined ||
      (plan.readTimeTerms ?? []).length > 0
   ) {
      return undefined;
   }
   const declaration = declarationOf(
      def as Parameters<typeof declarationOf>[0],
   );
   const modelHref = pathToFileURL(modelFile).href;
   if (!declaration || overlayKey(declaration.url) !== overlayKey(modelHref)) {
      return undefined;
   }
   // Where the build source is declared: the companion, or, with a chain
   // (P10), its level's file (build_file_generator.ts).
   const levels = chainLevels(materialized.plans);
   const modelName = path.basename(modelFile);
   const importFrom = [...levels.values()].some((level) => level > 0)
      ? levelFileName(modelName, levels.get(plan.sourceID) ?? 0)
      : `${modelName}${MATERIALIZE_COMPANION_SUFFIX}`;
   const declaring = pathToFileURL(
      path.join(path.dirname(modelFile), importFrom),
   ).href;
   if (!materialized.files.has(declaring)) return undefined;
   return { plan, declaration, importFrom };
}

/**
 * The copy each stored `#(materialize)` base is imported from by the second
 * compile, by the base's declared name: `text`, the model as the companion
 * reads it, with the base re-rooted on its build source
 * ({@link storedSourceCopyText}), at its URL beside the model. A base whose
 * copy cannot be made is left out, so its rollups build from the warehouse.
 */
function storedBaseCopies(
   bases: ReadonlyMap<string, StoredMaterializeBase>,
   text: string | undefined,
   modelFile: string,
): Map<string, { url: string; text: string }> {
   const out = new Map<string, { url: string; text: string }>();
   if (text === undefined) return out;
   for (const [name, base] of bases) {
      const copy = storedSourceCopyText(
         text,
         base.plan,
         base.declaration,
         base.importFrom,
      );
      if (copy === undefined) continue;
      out.set(name, {
         url: pathToFileURL(
            path.join(
               path.dirname(modelFile),
               rollupStoredBaseCopyName(path.basename(modelFile), name),
            ),
         ).href,
         text: copy,
      });
   }
   return out;
}

/**
 * The build copy of each base whose rollups read one ({@link readsBuildCopy}:
 * its declaring file, the author's model, with its given-reading terms left
 * out and, for a private grain dimension, its own access modifiers lifted),
 * keyed by the copy's URL beside the model. A base whose copy cannot be made
 * has its rollups refused, by the reason the copy was for.
 */
function rollupBuildCopies(
   plans: readonly RollupVariantPlan[],
   text: string | undefined,
   modelFile: string,
): {
   files: Map<string, string>;
   refused: (RollupRefusal & { plan: RollupVariantPlan })[];
} {
   const files = new Map<string, string>();
   const refused: (RollupRefusal & { plan: RollupVariantPlan })[] = [];
   const bases = new Map<string, RollupVariantPlan[]>();
   for (const p of plans.filter(readsBuildCopy)) {
      bases.set(p.baseSourceID, [...(bases.get(p.baseSourceID) ?? []), p]);
   }
   for (const of of bases.values()) {
      const first = of[0] as RollupVariantPlan;
      const made =
         text === undefined
            ? { reason: "its file could not be read" }
            : rollupBuildCopyText(
                 text,
                 first,
                 of.some((p) => (p.privateGrain ?? []).length > 0),
              );
      if ("copy" in made) {
         const name = rollupBuildCopyName(
            path.basename(modelFile),
            first.baseDeclaredName,
         );
         files.set(
            pathToFileURL(path.join(path.dirname(modelFile), name)).href,
            made.copy,
         );
         continue;
      }
      for (const plan of of) {
         refused.push({
            plan,
            tier: "unservable",
            code: PREAGGREGATE_NOT_BUILT_CODE,
            message: `#(preaggregate) rollup of source "${plan.baseSourceName}" at (${plan.grainDimensions.join(", ")}) is not built, so its measures are served live: ${made.reason}.`,
            sourceName: plan.baseSourceName,
            refused: measureRefusalKey(
               plan.baseSourceID,
               plan.baseDeclaredName,
               undefined,
               // A given-reading `where:` the build copy could not leave
               // out, as `#(materialize)` names it.
               plan.readTimeTerms?.length
                  ? "given_in_persisted_query"
                  : "hidden_field",
            ),
         });
      }
   }
   return { files, refused };
}

/** Where the rollup companion for `modelPath` lives, as an absolute URL. */
function rollupCompanionURL(packagePath: string, modelPath: string): URL {
   return pathToFileURL(
      `${path.join(packagePath, modelPath)}${ROLLUP_COMPANION_SUFFIX}`,
   );
}

/**
 * Plan, generate and compile the rollup companion for one author model: the
 * rollups of every source the model DECLARES. `generated` is undefined when it
 * declares none, or when the companion does not compile (a publisher bug: the
 * text is generated, so it is logged at `error` and costs that model its
 * rollups and nothing else). `refused` holds the declarations not planned.
 */
export async function tryCompileRollupCompanion(args: {
   packagePath: string;
   modelPath: string;
   modelDef: unknown;
   getRuntime: SynthesizedRuntimeFactory;
   /** The model's `#(materialize)` companion, when it compiled one. */
   materialized?: MaterializedBases;
}): Promise<{
   generated?: GeneratedRollups;
   refused: RollupRefusal[];
   /** The rollups a failure left out of the plan (see omitted_sources). */
   omitted?: Omission[];
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const companionHref = rollupCompanionURL(
      args.packagePath,
      args.modelPath,
   ).href;
   let refused: RollupRefusal[] = [];
   // Named once planned; until then a failure cannot name them.
   let plannedNames: string[] | undefined;
   try {
      const planned = planModelRollups(
         SourceMap.of(args.modelDef),
         {
            packageRelativeSourceID: (id) =>
               packageRelativeSourceID(args.packagePath, id),
            // v1 lines are always planned; `#(preaggregate)` only with its switch.
            readsPreaggregateDeclarations: getMaterializeEnabled(),
         },
         declaredInModel(modelFile, true),
      );
      refused = planned.refused;
      plannedNames = planned.plans.map((p) => p.rollupSourceName);
      if (planned.plans.length === 0) return { refused };
      const url = rollupCompanionURL(args.packagePath, args.modelPath);
      const files = GeneratedFiles.over(args.getRuntime);
      // The shared, checked blanking (persist_blanking.ts): each compiler
      // span must hold its note and the file must lex the same once blanked.
      const overlay = await files.blanked(args.modelDef);
      // A base's build copy is made from its file as the companion reads it:
      // blanked, where it has a `#@` line to blank.
      const modelURL = pathToFileURL(modelFile);
      const copies = overlay
         ? rollupBuildCopies(
              planned.plans,
              overlay.get(overlayKey(modelURL)) ??
                 (await files.readIfAny(modelURL)),
              modelFile,
           )
         : { files: new Map<string, string>(), refused: [] };
      refused = [...refused, ...copies.refused];
      planned.plans = planned.plans.filter(
         (p) => !copies.refused.some((r) => r.plan === p),
      );
      const text = generateRollupBuildFile(
         planned.plans,
         path.basename(args.modelPath),
      );
      if (!text) return { refused };
      if (!overlay) {
         // Building over the author's files as they are would chain a rollup
         // onto its base's table; build nothing instead.
         logger.error(
            "A #(preaggregate) companion was not built: a `#@` line of the author's files could not be left out of it",
            { modelPath: args.modelPath },
         );
         return {
            refused,
            omitted: companionOmissions("rollup_blanking", {
               file: companionHref,
               names: plannedNames,
            }),
         };
      }
      overlay.set(url.href, text);
      for (const [copy, copyText] of copies.files) overlay.set(copy, copyText);
      const { runtime, materializer, model } = await files.compile(
         url,
         overlay,
      );
      await compileRollupKeyChecks(planned.plans, materializer, args.modelPath);
      const overStored = overStoredBaseNotes(
         SourceMap.of(args.modelDef),
         planned.plans,
         url.href,
         modelFile,
         args.materialized,
      );
      let overStoredBases:
         | Promise<ReadonlyMap<string, OverStoredBase>>
         | undefined;
      return {
         generated: {
            plans: planned.plans,
            model,
            materializer,
            runtime,
            text,
            overStoredBases: () =>
               (overStoredBases ??= compileOverStoredBases({
                  files,
                  modelDef: args.modelDef,
                  modelPath: args.modelPath,
                  modelFile,
                  companionURL: url,
                  text,
                  plans: planned.plans,
                  copies: copies.files,
                  overStored,
                  materialized: args.materialized,
               })),
            overStoredCandidates: overStored.rollups,
            storedBaseBuilds: storedBaseBuildsOf(
               SourceMap.of(args.modelDef),
               planned.plans,
               url.href,
               path.basename(args.modelPath),
            ),
         },
         refused,
      };
   } catch (error) {
      logger.error(
         "Failed to plan or compile the generated #(preaggregate) companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return {
         refused,
         omitted: companionOmissions("rollup", {
            file: companionHref,
            names: plannedNames,
         }),
      };
   }
}

/**
 * The companion compiled a second time, over the bases' stored tables: each
 * `#@ persist` base with its own notes kept, and each `#(materialize)` base
 * imported from its re-rooted copy beside the `#(materialize)` companion's
 * files ({@link GeneratedRollups.overStoredBases}). A rollup whose base's copy
 * or build source is missing is left out, and builds from the warehouse.
 * Each copy is compile-checked on its own, and one that fails is left out
 * alone; when the copies still fail together, the companion is compiled
 * again with none of them, so the `#@ persist` rollups still read their
 * bases' tables; when that fails too, every rollup builds from the warehouse.
 */
async function compileOverStoredBases(args: {
   files: GeneratedFiles;
   modelDef: unknown;
   modelPath: string;
   modelFile: string;
   companionURL: URL;
   /** The planned companion's text. */
   text: string;
   plans: readonly RollupVariantPlan[];
   /** The bases' build copies ({@link rollupBuildCopies}), by URL. */
   copies: ReadonlyMap<string, string>;
   overStored: ReturnType<typeof overStoredBaseNotes>;
   materialized: MaterializedBases | undefined;
}): Promise<ReadonlyMap<string, OverStoredBase>> {
   const { files, modelFile, overStored } = args;
   const url = args.companionURL;
   const modelURL = pathToFileURL(modelFile);
   const out = new Map<string, OverStoredBase>();
   if (overStored.rollups.size === 0) return out;
   let kept: Map<string, string> | undefined;
   let stored: Map<string, { url: string; text: string }>;
   try {
      kept = await files.blanked(args.modelDef, overStored.keep);
      if (!kept) return out;
      stored = storedBaseCopies(
         overStored.stored,
         kept.get(overlayKey(modelURL)) ?? (await files.readIfAny(modelURL)),
         modelFile,
      );
   } catch (error) {
      warnWarehouse(args.modelPath, error);
      return out;
   }
   for (const [copy, copyText] of args.copies) kept.set(copy, copyText);
   // Each stored copy checked on its own: one that does not compile (a base
   // whose `except:` names a column its build source never had, say) is left
   // out, so only its own rollups build from the warehouse.
   const materializedFiles = new Map(kept);
   for (const [file, fileText] of args.materialized?.files ?? []) {
      materializedFiles.set(file, fileText);
   }
   for (const [name, copy] of [...stored]) {
      try {
         await files.compile(
            new URL(copy.url),
            new Map([...materializedFiles, [copy.url, copy.text]]),
         );
      } catch (error) {
         logger.warn(
            "A rollup builds from the warehouse: its #(materialize) base's copy over the stored table did not compile",
            {
               modelPath: args.modelPath,
               base: name,
               error: error instanceof Error ? error.message : String(error),
            },
         );
         stored.delete(name);
      }
   }
   try {
      await compileOverStored(stored);
   } catch (error) {
      if (stored.size === 0) {
         warnWarehouse(args.modelPath, error);
         return out;
      }
      // Together the copies still failed: the `#@ persist` rollups compile
      // without any `#(materialize)` copy, as they would with none declared.
      logger.warn(
         "The rollups over #(materialize) bases build from the warehouse: their companion over the bases' stored tables did not compile",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      out.clear();
      try {
         await compileOverStored(new Map());
      } catch (retryError) {
         warnWarehouse(args.modelPath, retryError);
      }
   }
   return out;

   /** Compile the companion over `copies`, filling `out`. Throws on a Malloy error. */
   async function compileOverStored(
      copies: ReadonlyMap<string, { url: string; text: string }>,
   ): Promise<void> {
      const overlay = new Map(
         copies.size === 0 ? (kept as Map<string, string>) : materializedFiles,
      );
      overlay.set(
         url.href,
         copies.size === 0
            ? args.text
            : (generateRollupBuildFile(
                 args.plans,
                 path.basename(args.modelPath),
                 new Set(copies.keys()),
              ) as string),
      );
      for (const { url: copy, text: copyText } of copies.values()) {
         overlay.set(copy, copyText);
      }
      const compiled = await files.compile(url, overlay);
      const sources = plannedSources(
         await compiled.runtime.getBuildTargets(compiled.model),
      );
      // Each stored base's build source, as this compile holds it.
      const buildSources = new Map<string, PersistSource>();
      for (const [name, base] of overStored.stored) {
         const declaring = pathToFileURL(
            path.join(path.dirname(modelFile), base.importFrom),
         ).href;
         const found = sources.find(
            (s) =>
               s.name === base.plan.buildSourceName &&
               overlayKey(splitSourceID(s.sourceID)?.url ?? "") ===
                  overlayKey(declaring),
         );
         if (found) buildSources.set(name, found);
      }
      const baseOf = new Map(
         args.plans.map((p) => [
            `${p.rollupSourceName}@${url.href}`,
            p.baseDeclaredName,
         ]),
      );
      for (const source of sources) {
         if (!overStored.rollups.has(source.sourceID)) continue;
         const base = baseOf.get(source.sourceID) as string;
         if (!overStored.stored.has(base)) {
            out.set(source.sourceID, { source });
            continue;
         }
         const baseBuildSource = copies.has(base)
            ? buildSources.get(base)
            : undefined;
         if (baseBuildSource) {
            out.set(source.sourceID, { source, baseBuildSource });
         }
      }
   }
}

/** Log that a model's rollups build from the warehouse, and why. */
function warnWarehouse(modelPath: string, error: unknown): void {
   logger.warn(
      "The rollups of a model build from the warehouse: their companion over the bases' stored tables did not compile",
      {
         modelPath,
         error: error instanceof Error ? error.message : String(error),
      },
   );
}

/**
 * The `compile` {@link compileParentKeyChecks} takes, over the author's model
 * `materializer`: the check's SQL, refused when the check reads a given (as
 * Malloy records it, `givenUsage`: a `where:` that reads one, say). Compiled
 * once at plan time, such a check reads the rows of the given's default (or
 * does not compile without one), not the rows each caller's query reads, so a
 * key it finds unique may repeat for another caller; refused, the
 * join-to-rollup branch reads that source's key as unknown and declines.
 */
export function parentKeyCheckCompiler(materializer: {
   loadQuery(text: string): {
      getPreparedQuery(): Promise<unknown>;
      getSQL(): Promise<string>;
   };
}): (text: string) => Promise<string> {
   return async (text) => {
      const query = materializer.loadQuery(text);
      const givens = queryGivenUsageIds(await query.getPreparedQuery());
      if (givens === undefined || givens.length > 0) {
         throw new Error(
            givens === undefined
               ? "the check's given usage could not be read"
               : "the check reads a given, so it would describe one caller's rows, not each caller's",
         );
      }
      return query.getSQL();
   };
}

/**
 * Attach to each colocated rollup in `plans` the primary key checks of the
 * query sources that read it through a join read as a join_many
 * (`readsAsJoinMany`), each compiled once by its `compile` (the author's
 * model that names the source). To each rollup, colocated or in a storage
 * destination, also attach the check of its base's own primary key when a
 * join reaches the base on another column (`joinedKeys`, see
 * {@link JoinedKeyNeed}): the join-to-rollup branch serves a count, sum or
 * average of the base through such a join only when that key is unique and
 * never NULL. A check that does not compile is left off, logged: the branch
 * then reads that source's key as unknown and declines.
 */
export async function compileParentKeyChecks(
   plans: Iterable<RollupVariantPlan>,
   needs: readonly {
      need: ParentKeyNeed;
      compile: (text: string) => Promise<string>;
   }[],
   joinedKeys: readonly {
      need: JoinedKeyNeed;
      compile: (text: string) => Promise<string>;
   }[] = [],
): Promise<void> {
   const compiled = new Map<string, Promise<string | undefined>>();
   const compileOnce = (
      sourceID: string,
      name: string,
      keyColumn: string,
      compile: (text: string) => Promise<string>,
   ): Promise<string | undefined> => {
      let sql = compiled.get(sourceID);
      if (sql === undefined) {
         sql = compile(parentKeyCheckQuery(name, keyColumn)).catch((error) => {
            logger.warn(
               "A source's primary key check did not compile; queries it answers through a #(preaggregate) rollup read through a join are answered live",
               {
                  source: name,
                  keyColumn,
                  error: error instanceof Error ? error.message : String(error),
               },
            );
            return undefined;
         });
         compiled.set(sourceID, sql);
      }
      return sql;
   };
   for (const plan of plans) {
      // A `#@ preaggregate` (v1) plan is never read through a join.
      if (plan.variant !== true) continue;
      const checks: ParentKeyCheck[] = [];
      const seen = new Set<string>();
      // A query source's key is checked over its rows where the rollup is
      // colocated; in a destination, over its stored table there
      // (`storedKeyChecksOf`).
      for (const { need, compile } of plan.storage === undefined ? needs : []) {
         if (seen.has(need.sourceID) || !readsAsJoinMany(plan, need)) continue;
         seen.add(need.sourceID);
         const text = await compileOnce(
            need.sourceID,
            need.name,
            need.keyColumn,
            compile,
         );
         if (text === undefined) continue;
         checks.push({
            sourceID: need.sourceID,
            keyColumn: need.keyColumn,
            connectionName: need.connectionName,
            sql: text,
         });
      }
      for (const { need, compile } of joinedKeys) {
         if (need.sourceID !== plan.baseSourceID || seen.has(need.sourceID)) {
            continue;
         }
         seen.add(need.sourceID);
         const text = await compileOnce(
            need.sourceID,
            need.name,
            need.keyColumn,
            compile,
         );
         if (text === undefined) continue;
         checks.push({
            sourceID: need.sourceID,
            keyColumn: need.keyColumn,
            connectionName: need.connectionName,
            sql: text,
         });
      }
      if (checks.length > 0) plan.parentKeyChecks = checks;
      else delete plan.parentKeyChecks;
   }
}

/**
 * Compile each rollup's key check (known limitation 29, see
 * `rollupKeyCheckQuery`) against the companion, onto its plan. A check that
 * does not compile is left off, logged: its rollup's join_one is then never
 * known unique, and the join-to-rollup branch declines it.
 */
async function compileRollupKeyChecks(
   plans: RollupVariantPlan[],
   materializer: ModelMaterializer,
   modelPath: string,
): Promise<void> {
   for (const plan of plans) {
      const text = rollupKeyCheckQuery(plan);
      const keyColumn = plan.grainDimensions[0];
      if (text === undefined || keyColumn === undefined) continue;
      try {
         const sql = await materializer.loadQuery(text).getSQL();
         plan.keyCheck = { keyColumn, sql };
      } catch (error) {
         logger.warn("A #(preaggregate) rollup's key check did not compile", {
            modelPath,
            rollup: plan.rollupSourceName,
            error: error instanceof Error ? error.message : String(error),
         });
      }
   }
}
