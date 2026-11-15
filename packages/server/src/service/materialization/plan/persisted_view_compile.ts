// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated persisted-view companion (see persisted_view.ts) and
 * judges each declaration against the author's compiled model, on the load
 * worker, compiling and never running:
 *
 * - **Caller-dependent (P14).** A view whose result reads the current time,
 *   through its own stages, its source's `where:` or a field it reads, differs
 *   per read: not built. (A view that reads a given is refused by the build
 *   plan, from Malloy's own `readsGivens` on its build target.)
 * - **Nested results on `storage=`.** A destination's `type:` declaration
 *   cannot declare a nested result, so such a view is stored only colocated.
 * - **Access filters (question 3).** A row-level gate on the view's source is
 *   re-applied to the stored rows at read (route, then govern), which is exact
 *   only when every field the gate reads is one the view groups by, under its
 *   own name, in a single grouping stage with no `limit:`, no ungrouped
 *   aggregate (`all()`, `exclude()`) and no `calculate:`, so filtering the
 *   stored groups keeps exactly the groups the gate keeps live. Anything else
 *   is a publish warning, never a compile error: the gate may be added later
 *   by another author.
 *
 * The companion reads the author's files with every source-level `#@` line
 * blanked (positions kept; `persist_blanking.ts`), so a view never chains
 * onto its source's own table.
 */

import type {
   Model as MalloyModel,
   ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { logger } from "../../../logger";
import { annotationTexts, type AnnotationsDef } from "../../annotations";
import { routeOfNote } from "../../annotation_reader";
import {
   buildRowLevelProbe,
   gateFilterText,
   liftProbeFilterCondition,
} from "../../authorize";
import { collectEntryPointGates } from "../../gate_classification";
import { readPreparedQuery } from "../../governance/ir/prepared_query";
import { identifier } from "../../malloy_text";
import { overlayKey } from "../../overlay_key";
import { persistBlankedFiles } from "../text/persist_blanking";
import { currentTimeFields, readsCurrentTime } from "./persist_dynamic_terms";
import {
   generatePersistedViewFile,
   persistedViewCompanionHref,
   planModelPersistedViews,
   type PersistedViewPlan,
   type PersistedViewRefusal,
} from "./persisted_view";
import type { SynthesizedRuntimeFactory } from "./preaggregation_compile";
import { SourceMap } from "../text/source_map";
import { omittedFromFile, type Omission } from "../domain/omitted_sources";

export interface GeneratedPersistedViews {
   /** The views the companion declares. Never empty. */
   plans: PersistedViewPlan[];
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   text: string;
}

interface StageLike {
   type?: unknown;
   limit?: unknown;
   queryFields?: unknown[];
   expandedFieldUsage?: { path?: unknown }[];
   expandedUngroupings?: unknown[];
   outputStruct?: { fields?: unknown[] };
}

/** The dotted paths a stage reads, from the compiler's expanded usage. */
function stageReads(stage: StageLike): string[] {
   return (stage.expandedFieldUsage ?? []).flatMap((u) =>
      Array.isArray(u.path) && u.path.length > 0 ? [u.path.join(".")] : [],
   );
}

/** Whether `path` (or a join it reads through) is in `dependent`. */
function dependsOn(path: string, dependent: ReadonlySet<string>): boolean {
   const parts = path.split(".");
   for (let i = 1; i <= parts.length; i++) {
      if (dependent.has(parts.slice(0, i).join("."))) return true;
   }
   return false;
}

/** Whether an output field is nested (a `nest:`, a record or an array). */
function isNested(field: unknown): boolean {
   const f = field as { type?: unknown; join?: unknown };
   return f.join !== undefined || f.type === "array" || f.type === "record";
}

/** The output names a reduce stage groups by, as plain one-segment references. */
function groupedNames(stage: StageLike): Set<string> {
   const out = new Set<string>();
   for (const q of stage.queryFields ?? []) {
      const f = q as { type?: unknown; path?: unknown };
      if (
         f.type === "fieldref" &&
         Array.isArray(f.path) &&
         f.path.length === 1 &&
         typeof f.path[0] === "string"
      ) {
         out.add(f.path[0]);
      }
   }
   return out;
}

/** Whether a stage computes anything across groups: a window or an ungrouped aggregate. */
function computesAcrossGroups(stage: StageLike): boolean {
   if ((stage.expandedUngroupings ?? []).length > 0) return true;
   return (stage.queryFields ?? []).some((q) => {
      const t = (q as { expressionType?: unknown }).expressionType;
      return (
         typeof t === "string" &&
         (t.includes("analytic") || t.includes("ungrouped"))
      );
   });
}

/**
 * Why one planned view cannot be built and served as declared, or undefined
 * when it can. Compiles `run: <source> -> <view>` and the gate probes on the
 * author's materializer; never runs them.
 */
export async function persistedViewRefusalReason(
   plan: PersistedViewPlan,
   modelDef: unknown,
   materializer: ModelMaterializer,
): Promise<string | undefined> {
   const node = SourceMap.of(modelDef).node(plan.sourceID);
   const localName = node?.name;
   if (!node || !localName) {
      return "its source is not in this model's namespace";
   }
   const def = node.def as {
      fields?: unknown;
      filterList?: unknown;
   };
   let stages: StageLike[];
   try {
      const prepared = await readPreparedQuery(
         materializer.loadQuery(
            `run: ${identifier(localName)} -> ${identifier(plan.viewName)}`,
         ),
      );
      stages = Array.isArray(prepared.pipeline)
         ? (prepared.pipeline as StageLike[])
         : [];
   } catch (error) {
      return `the view does not compile on its own (${error instanceof Error ? error.message.split("\n")[0] : String(error)})`;
   }
   if (stages.length === 0) return "the view has no stage to store";
   const timed = currentTimeFields(def.fields);
   if (
      readsCurrentTime(def.filterList) ||
      readsCurrentTime(stages) ||
      stages.some((s) => stageReads(s).some((p) => dependsOn(p, timed)))
   ) {
      return "its result reads the current time, so it differs per read";
   }
   const last = stages[stages.length - 1];
   if (
      plan.storage !== undefined &&
      (last.outputStruct?.fields ?? []).some(isNested)
   ) {
      return "its result nests, which a storage destination's table cannot declare yet; persist it without storage=";
   }
   const gates = collectEntryPointGates(node.def as never, modelDef as never);
   if (gates.length === 0) return undefined;
   if (gates.some((g) => !g.struct)) {
      return "the gates of its source cannot be read";
   }
   const gatedWhy =
      "its source carries a row-level gate (#(access_filter) or #(authorize))";
   if (stages.length !== 1 || last.type !== "reduce") {
      return `${gatedWhy}, which a multi-stage or non-grouping view's stored result cannot keep`;
   }
   if (last.limit !== undefined) {
      return `${gatedWhy}, which a view with limit: cannot keep: its stored rows are limited across every caller`;
   }
   if (computesAcrossGroups(last)) {
      return `${gatedWhy}, which a view computing across groups (all(), exclude(), calculate:) cannot keep`;
   }
   const grouped = groupedNames(last);
   for (const gate of gates) {
      const filterText = gateFilterText(gate.exprs);
      let paths: unknown[];
      try {
         const probe = await materializer
            .loadQuery(buildRowLevelProbe(localName, filterText))
            .getPreparedQuery();
         const condition = liftProbeFilterCondition<{
            code?: string;
            isSourceFilter?: boolean;
            refSummary?: { fieldUsage?: { path?: unknown }[] };
         }>(probe, `persisted view gate on "${localName}"`, filterText);
         paths = (condition.refSummary?.fieldUsage ?? []).map((u) => u.path);
      } catch {
         return `${gatedWhy} that could not be read against the view`;
      }
      for (const p of paths) {
         if (!Array.isArray(p) || p.length === 0) continue;
         if (p.length !== 1 || typeof p[0] !== "string" || !grouped.has(p[0])) {
            return `the access filter of its source reads \`${p.join(".")}\`, which the view does not group by, so its stored rows could not be filtered per caller`;
         }
      }
   }
   return undefined;
}

/**
 * Plan, judge, generate and compile the persisted-view companion for one
 * author model: the views of every source it DECLARES. `generated` is
 * undefined when it declares none or the companion does not compile (a
 * publisher bug, logged at `error`); `refused` holds the views not planned.
 */
export async function tryCompilePersistedViewCompanion(args: {
   packagePath: string;
   modelPath: string;
   modelDef: unknown;
   materializer: ModelMaterializer;
   getRuntime: SynthesizedRuntimeFactory;
}): Promise<{
   generated?: GeneratedPersistedViews;
   refused: PersistedViewRefusal[];
   /** The views a failure left out of the plan (see omitted_sources). */
   omitted?: Omission[];
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const companionHref = persistedViewCompanionHref(
      pathToFileURL(modelFile).href,
   );
   const refused: PersistedViewRefusal[] = [];
   // The build sources admitted so far; until planned a failure cannot name them.
   let plannedNames: string[] | undefined;
   try {
      const planned = planModelPersistedViews(args.modelDef, (url) => {
         try {
            return path.resolve(fileURLToPath(url)) === modelFile;
         } catch {
            return false;
         }
      });
      refused.push(...planned.refused);
      const plans: PersistedViewPlan[] = [];
      for (const plan of planned.plans) {
         const reason = await persistedViewRefusalReason(
            plan,
            args.modelDef,
            args.materializer,
         );
         if (reason === undefined) plans.push(plan);
         else refused.push({ ...plan, reason });
      }
      plannedNames = plans.map((p) => p.buildSourceName);
      const text = generatePersistedViewFile(
         plans,
         path.basename(args.modelPath),
      );
      if (!text) return { refused };
      const { runtime: reader } = await args.getRuntime(new Map());
      const overlay = await persistBlankedFiles(args.modelDef, async (url) => {
         try {
            const read = await reader.urlReader.readURL(new URL(url));
            return typeof read === "string" ? read : read.contents;
         } catch {
            return undefined;
         }
      });
      if (!overlay) {
         // A stored base must never stand in for the rows a view reads.
         for (const plan of plans) {
            refused.push({
               ...plan,
               reason:
                  "a source-level `#@` line in its files could not be set aside, so the view's build would read a stored table",
            });
         }
         return { refused };
      }
      const url = new URL(companionHref);
      overlay.set(overlayKey(url), text);
      const { runtime, importBaseURL } = await args.getRuntime(overlay);
      const materializer = runtime.loadModel(url, { importBaseURL });
      const model = await materializer.getModel();
      return {
         generated: { plans, model, materializer, runtime, text },
         refused,
      };
   } catch (error) {
      logger.error(
         "Failed to plan or compile the generated persisted-view companion",
         {
            modelPath: args.modelPath,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return {
         refused,
         omitted: omittedFromFile(
            companionHref,
            plannedNames,
            "persisted_view_companion_failed",
         ),
      };
   }
}

/**
 * The notes of a view field the variant carries onto its own view: every
 * route but `#@`, inherited levels included (an alias `view: v2 is v`, or an
 * `include:` re-annotation, keeps the render tags of the view it names on an
 * inherited level, and live renders with them), ancestors first so a later
 * note overrides an earlier one as it does live.
 */
export function viewNoteTexts(
   annotations: AnnotationsDef | undefined,
): string[] {
   return (annotationTexts(annotations) ?? [])
      .map((t) => t.trim())
      .filter((t) => routeOfNote(t) !== "@" && !t.includes("\n"));
}
