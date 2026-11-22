// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compiles the generated persisted-view companion (see persisted_view.ts) and
 * judges each declaration against the author's compiled model, on the load
 * worker, compiling and never running:
 *
 * - **Caller-dependent.** A view whose result reads the current time,
 *   through its own stages, its source's `where:` or a field it reads, differs
 *   per read: not built. (A view that reads a given is refused by the build
 *   plan, from Malloy's own `readsGivens` on its build target.)
 * - **Nested results on `storage=`.** A destination's `type:` declaration
 *   cannot declare a nested result, so such a view is stored only database.
 * - **Access filters.** A row-level gate on the view's source is
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
import { pathToFileURL } from "url";
import {
   ownLevelNotes,
   type AnnotationNote,
   type AnnotationsDef,
   type NotePosition,
} from "../../annotations";
import {
   notesDeclarePersist,
   placementOf,
   readNotesIfAny,
} from "./declaration";
import { noteSpanKey } from "../text/persist_blanking";
import { plannedSources } from "./addressing";
import { routeOfNote } from "../../annotation_reader";
import {
   buildRowLevelProbe,
   gateFilterText,
   liftProbeFilterCondition,
} from "../../authorize";
import {
   AUTHORIZE_ROUTE,
   canonicalAuthorizeRoute,
} from "../../authorize_routes";
import { collectEntryPointGates } from "../../gate_classification";
import { readPreparedQuery } from "../../governance/ir/prepared_query";
import { identifier } from "../../malloy_text";
import { pipelineReadsCurrentTime } from "./persist_dynamic_terms";
import {
   planModelPersistedQueries,
   planModelPersistedViews,
   type PersistedQueryRefusal,
   type PersistedViewRefusal,
} from "./persisted_view";
import {
   generatePersistedResultFile,
   persistedViewCompanionHref,
} from "../text/read_text";
import {
   splitSourceID,
   type OverStoredBase,
   type PersistedQueryPlan,
   type PersistedViewPlan,
} from "../domain/table_plans";
import {
   companionFailure,
   companionOmissions,
   type CompanionFailure,
   declaredInModel,
   GeneratedFiles,
   type SynthesizedRuntimeFactory,
} from "../text/generated_files";
import { SourceMap } from "../text/source_map";
import {
   storedBaseCopies,
   storedMaterializeUpstream,
   type MaterializedBases,
   type StoredMaterializeBase,
} from "./stored_base_copy";
import { overlayKey } from "../../overlay_key";
import {
   reachedSourceIDsOf,
   reachedUpstreams,
   storedElsewhereReason,
   UPSTREAM_TABLE_UNUSABLE_REASON,
} from "./downstream_upstreams";
import type { Omission } from "../domain/omitted_sources";
import type { RefusedReason } from "../domain/types";
import { gateRefusalReason } from "../domain/gate_refusal";

interface GeneratedPersistedViews {
   /** The views the companion declares. */
   plans: PersistedViewPlan[];
   /** The named queries the companion declares. Never both empty. */
   queries: PersistedQueryPlan[];
   model: MalloyModel;
   materializer: ModelMaterializer;
   runtime: Runtime;
   text: string;
   /**
    * The database views and queries that may be built over their upstreams'
    * stored tables, by build sourceID, each as the companion compiled over
    * the author's files with those upstreams' own `#@ persist` lines kept
    * ({@link overStoredUpstreamNotes}), so Malloy's planner reads each
    * upstream's table through a run's build manifest. Compiled at plan
    * time, each candidate alone, so the plan records the decision
    * (build_plan.ts proves each against its planned table). A candidate
    * missing here did not compile, and builds from its definition.
    */
   overStoredSources: ReadonlyMap<string, OverStoredBase>;
   /** The build sourceIDs offered the compile: the candidates. */
   overStoredCandidates: ReadonlySet<string>;
}

interface StageLike {
   type?: unknown;
   limit?: unknown;
   queryFields?: unknown[];
   expandedFieldUsage?: { path?: unknown }[];
   expandedUngroupings?: unknown[];
   outputStruct?: { fields?: unknown[] };
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
async function persistedViewRefusalReason(
   plan: PersistedViewPlan,
   modelDef: unknown,
   materializer: ModelMaterializer,
): Promise<{ reason: string; refusal: RefusedReason } | undefined> {
   const node = SourceMap.of(modelDef).node(plan.sourceID);
   const localName = node?.name;
   if (!node || !localName) {
      return {
         reason: "its source is not in this model's namespace",
         refusal: "run_target_unsupported",
      };
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
      return {
         reason: `the view does not compile on its own (${error instanceof Error ? error.message.split("\n")[0] : String(error)})`,
         refusal: "generated_build_failed",
      };
   }
   if (stages.length === 0)
      return {
         reason: "the view has no stage to store",
         refusal: "no_stored_columns",
      };
   // A result that reads the current time is stored as of its build and
   // rebuilt on every run, unless its source is gated (below).
   const readsNow = pipelineReadsCurrentTime(def, stages);
   const last = stages[stages.length - 1];
   // On either tier: the view leg never answers from a nested result (it
   // cannot rebuild a nest's rows exactly), so a table built for one would
   // never be read.
   if ((last.outputStruct?.fields ?? []).some(isNested)) {
      return {
         reason:
            "its result nests, which a persisted view cannot answer from its table: a nest's stored rows cannot be rebuilt exactly",
         refusal: "nested_result",
      };
   }
   const gates = collectEntryPointGates(node.def as never, modelDef as never);
   if (gates.length === 0) return undefined;
   if (readsNow) {
      return {
         reason:
            "its result reads the current time and its source carries a " +
            "row-level gate, whose decision a result stored as of its build " +
            "would freeze",
         refusal: "current_time",
      };
   }
   // Named by the gates' kind, as on every mechanism.
   const gateRefusal = gateRefusalReason(gates.map((g) => g.route));
   if (gates.some((g) => !g.struct)) {
      return {
         reason: "the gates of its source cannot be read",
         refusal: gateRefusal,
      };
   }
   // A row filter read through a `->` derivation filters the derivation's
   // base before it runs; the view's stored rows are the derivation's
   // output, which no filter at read can make equal to that.
   const throughDerivation = gates.filter(
      (g) =>
         g.baseHops > 0 && canonicalAuthorizeRoute(g.route) !== AUTHORIZE_ROUTE,
   );
   if (throughDerivation.length > 0) {
      return {
         reason:
            "its source inherits a row filter through `->`, which filters the derivation's base, not the rows a stored result holds",
         refusal: gateRefusalReason(throughDerivation.map((g) => g.route)),
      };
   }
   const gatedWhy =
      "its source carries a row-level gate (#(access_filter) or #(authorize))";
   if (stages.length !== 1 || last.type !== "reduce") {
      return {
         reason: `${gatedWhy}, which a multi-stage or non-grouping view's stored result cannot keep`,
         refusal: gateRefusal,
      };
   }
   if (last.limit !== undefined) {
      return {
         reason: `${gatedWhy}, which a view with limit: cannot keep: its stored rows are limited across every caller`,
         refusal: gateRefusal,
      };
   }
   if (computesAcrossGroups(last)) {
      return {
         reason: `${gatedWhy}, which a view computing across groups (all(), exclude(), calculate:) cannot keep`,
         refusal: gateRefusal,
      };
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
         return {
            reason: `${gatedWhy} that could not be read against the view`,
            refusal: gateRefusalReason([gate.route]),
         };
      }
      for (const p of paths) {
         if (!Array.isArray(p) || p.length === 0) continue;
         if (p.length !== 1 || typeof p[0] !== "string" || !grouped.has(p[0])) {
            return {
               reason: `the access filter of its source reads \`${p.join(".")}\`, which the view does not group by, so its stored rows could not be filtered per caller`,
               refusal: gateRefusalReason([gate.route]),
            };
         }
      }
   }
   return undefined;
}

/**
 * The routes of the gates on any source `struct` joins (at any depth), empty
 * when none carries one.
 */
function joinedGateRoutes(
   struct: unknown,
   modelDef: unknown,
   seen: Set<unknown> = new Set(),
): string[] {
   const routes: string[] = [];
   const fields = (struct as { fields?: unknown[] })?.fields ?? [];
   for (const f of fields) {
      const field = f as { join?: unknown; fields?: unknown };
      if (field.join === undefined || seen.has(f)) continue;
      seen.add(f);
      routes.push(
         ...collectEntryPointGates(f as never, modelDef as never).map(
            (g) => g.route,
         ),
         ...joinedGateRoutes(f, modelDef, seen),
      );
   }
   return routes;
}

/** The routes a row-level gate is written on. */
const GATE_NOTE_ROUTES = new Set(["access_filter", "authorize"]);

/**
 * The routes of the gate notes the source `sourceID` names carries that its
 * own statement did not write: Malloy copies a base's notes onto an
 * extension (`ops2 is ops extend { … }`), where the gate walk reads them as
 * the extension's own. A note is another statement's when it sits in another
 * file, or at the exact position of a note another source carries. A source
 * that cannot be found reads as one lock (fails closed).
 */
function inheritedGateNoteRoutes(
   sourceID: string,
   modelDef: unknown,
): string[] {
   const map = SourceMap.of(modelDef);
   const node = map.node(sourceID);
   const declaredIn = splitSourceID(sourceID)?.url;
   if (!node) return ["authorize"];
   const position = (n: AnnotationNote) =>
      n.at
         ? `${n.at.url}\u0000${n.at.range.start.line}\u0000${n.at.range.start.character}`
         : undefined;
   const gateNotes = ownLevelNotes(
      (node.def as { annotations?: AnnotationsDef }).annotations,
   ).filter((n) => GATE_NOTE_ROUTES.has(routeOfNote(n.text) ?? ""));
   if (gateNotes.length === 0) return [];
   const elsewhere = new Set<string>();
   for (const other of map.nodes()) {
      if (other.sourceID === sourceID) continue;
      for (const n of ownLevelNotes(
         (other.def as { annotations?: AnnotationsDef }).annotations,
      )) {
         const at = position(n);
         if (at !== undefined) elsewhere.add(at);
      }
   }
   return gateNotes
      .filter((n) => {
         const at = position(n);
         return (
            at === undefined || n.at?.url !== declaredIn || elsewhere.has(at)
         );
      })
      .map((n) => routeOfNote(n.text) ?? "authorize");
}

/**
 * Why one planned named query cannot be built and served as declared, or
 * undefined when it can. Compiles `run: <query>` and the gate probes on the
 * author's materializer; never runs them. Fails closed: each case below is a
 * refusal, and the query is answered live.
 *
 * - its result reads the current time (a read reads a given is refused by the
 *   build plan, from Malloy's own `readsGivens`: the query's own stages, its
 *   source's own given-reading `where:` and an inherited one alike);
 * - its result nests (regrouping a stored nest does not keep its rows yet:
 *   a `select:` nest's duplicates, an empty nest);
 * - it reaches a row-level gate: its source's own, one it inherits, or one on
 *   a source it joins. A persisted query is never read past a gate, so the
 *   gate policy needs no row for it.
 */
async function persistedQueryRefusalReason(
   plan: PersistedQueryPlan,
   modelDef: unknown,
   materializer: ModelMaterializer,
): Promise<{ reason: string; refusal: RefusedReason } | undefined> {
   let stages: StageLike[];
   let structRef: unknown;
   try {
      const prepared = await readPreparedQuery(
         materializer.loadQuery(`run: ${identifier(plan.queryName)}`),
      );
      stages = Array.isArray(prepared.pipeline)
         ? (prepared.pipeline as StageLike[])
         : [];
      structRef = prepared.structRef;
   } catch (error) {
      return {
         reason: `the query does not compile on its own (${error instanceof Error ? error.message.split("\n")[0] : String(error)})`,
         refusal: "generated_build_failed",
      };
   }
   if (stages.length === 0)
      return {
         reason: "the query has no stage to store",
         refusal: "no_stored_columns",
      };
   const node =
      plan.sourceID !== undefined
         ? SourceMap.of(modelDef).node(plan.sourceID)
         : undefined;
   if (plan.sourceID !== undefined && !node) {
      return {
         reason: "its source is not in this model's namespace",
         refusal: "run_target_unsupported",
      };
   }
   const def = (node?.def ?? structRef) as {
      fields?: unknown;
      filterList?: unknown;
   };
   if (!def || typeof def !== "object")
      return {
         reason: "its source cannot be read",
         refusal: "run_target_unsupported",
      };
   // A result that reads the current time is stored as of its build and
   // rebuilt on every run; a gate anywhere refuses the query below.
   const last = stages[stages.length - 1];
   if ((last.outputStruct?.fields ?? []).some(isNested)) {
      return {
         reason:
            "its result nests, which a persisted query cannot answer from its table yet",
         refusal: "nested_result",
      };
   }
   // A gate anywhere the query reaches refuses it: a stored result is never
   // read past a gate (the gate policy has no row for a persisted query).
   const gateRoutes = [
      ...collectEntryPointGates(
         (node?.def ?? def) as never,
         modelDef as never,
         new Set(),
         // The entry point: an inherited gate reads as self-contained.
         true,
      ).map((g) => g.route),
      ...joinedGateRoutes(node?.def ?? def, modelDef),
      ...(plan.sourceID !== undefined
         ? inheritedGateNoteRoutes(plan.sourceID, modelDef)
         : []),
   ];
   if (gateRoutes.length > 0) {
      // Named by the gates' kind, as on every mechanism.
      return {
         reason: GATED_QUERY_REASON,
         refusal: gateRefusalReason(gateRoutes),
      };
   }
   return undefined;
}

/**
 * The plan-time refusal of a database view or query (`storage` absent) an
 * upstream of which is stored in a `storage=` destination: its build in
 * the warehouse cannot read that table, and a downstream is never built by
 * recomputing the upstream from raw. Undefined otherwise.
 */
function storedUpstreamRefusal(
   storage: string | undefined,
   root: unknown,
   modelDef: unknown,
   map: SourceMap,
): { reason: string; refusal: RefusedReason } | undefined {
   if (storage !== undefined || root === undefined) return undefined;
   const reason = storedElsewhereReason(reachedUpstreams(root, modelDef, map));
   return reason === undefined
      ? undefined
      : { reason, refusal: UPSTREAM_TABLE_UNUSABLE_REASON };
}

/** A planned query's refusal record. */
function queryRefusalOf(
   plan: PersistedQueryPlan,
   refusal: { reason: string; refusal: RefusedReason },
): PersistedQueryRefusal {
   return {
      queryName: plan.queryName,
      ...refusal,
      declaringURL: plan.declaringURL,
      ...(plan.storage !== undefined ? { storage: plan.storage } : {}),
   };
}

/** Why a persisted query over a gated source is not built. */
const GATED_QUERY_REASON =
   "it reaches a row-level gate (#(access_filter) or #(authorize)), and a persisted query is never built over a gated source; it is answered live under the gate";

/**
 * Plan, judge, generate and compile the persisted-result companion for one
 * author model: the views of every source it DECLARES, and the named queries
 * it declares (`persisted_query.ts`). `generated` is undefined when it
 * declares none or the companion does not compile (a publisher bug, logged
 * at `error`); `refused` and `refusedQueries` hold what was not planned.
 */
export async function tryCompilePersistedViewCompanion(args: {
   packagePath: string;
   modelPath: string;
   modelDef: unknown;
   materializer: ModelMaterializer;
   getRuntime: SynthesizedRuntimeFactory;
   /** The model's `#(materialize)` companion, when it compiled one. */
   materialized?: MaterializedBases;
}): Promise<{
   generated?: GeneratedPersistedViews;
   refused: PersistedViewRefusal[];
   refusedQueries: PersistedQueryRefusal[];
   /** The views and queries a failure left out of the plan (see omitted_sources). */
   omitted?: Omission[];
   /** Why the companion failed, for the caller to report. */
   failure?: CompanionFailure;
}> {
   const modelFile = path.resolve(args.packagePath, args.modelPath);
   const companionHref = persistedViewCompanionHref(
      pathToFileURL(modelFile).href,
   );
   const refused: PersistedViewRefusal[] = [];
   const refusedQueries: PersistedQueryRefusal[] = [];
   // The build sources admitted so far; until planned a failure cannot name them.
   let plannedNames: string[] | undefined;
   try {
      const declaredHere = declaredInModel(modelFile, false);
      const planned = planModelPersistedViews(args.modelDef, declaredHere);
      refused.push(...planned.refused);
      const plans: PersistedViewPlan[] = [];
      const map = SourceMap.of(args.modelDef);
      for (const plan of planned.plans) {
         const reason =
            storedUpstreamRefusal(
               plan.storage,
               map.node(plan.sourceID)?.def,
               args.modelDef,
               map,
            ) ??
            (await persistedViewRefusalReason(
               plan,
               args.modelDef,
               args.materializer,
            ));
         if (reason === undefined) plans.push(plan);
         else {
            refused.push({
               ...plan,
               reason: reason.reason,
               outcome: { refused: reason.refusal },
            });
         }
      }
      const plannedQueries = planModelPersistedQueries(
         args.modelDef,
         declaredHere,
      );
      refusedQueries.push(...plannedQueries.refused);
      const viewNames = new Set(plans.map((p) => p.buildSourceName));
      const queries: PersistedQueryPlan[] = [];
      for (const plan of plannedQueries.plans) {
         const refusal = viewNames.has(plan.buildSourceName)
            ? {
                 reason: `its generated build source \`${plan.buildSourceName}\` collides with a persisted view's; rename one of them`,
                 refusal: "build_name_collision" as const,
              }
            : (storedUpstreamRefusal(
                 plan.storage,
                 (
                    args.modelDef as {
                       contents?: Record<string, unknown>;
                    }
                 ).contents?.[plan.queryName],
                 args.modelDef,
                 map,
              ) ??
              (await persistedQueryRefusalReason(
                 plan,
                 args.modelDef,
                 args.materializer,
              )));
         if (refusal === undefined) queries.push(plan);
         else refusedQueries.push(queryRefusalOf(plan, refusal));
      }
      plannedNames = [
         ...plans.map((p) => p.buildSourceName),
         ...queries.map((p) => p.buildSourceName),
      ];
      const text = generatePersistedResultFile(
         plans,
         queries,
         path.basename(args.modelPath),
      );
      if (!text) return { refused, refusedQueries };
      const files = GeneratedFiles.over(args.getRuntime);
      const overlay = await files.blanked(args.modelDef);
      if (!overlay) {
         // A stored base must never stand in for the rows a result reads.
         const reason =
            "a source-level `#@` line in its files could not be set aside, so its build would read a stored table";
         const refusal = "generated_build_failed" as const;
         for (const plan of plans) {
            refused.push({ ...plan, reason, outcome: { refused: refusal } });
         }
         for (const plan of queries) {
            refusedQueries.push(queryRefusalOf(plan, { reason, refusal }));
         }
         return { refused, refusedQueries };
      }
      const url = new URL(companionHref);
      overlay.set(url.href, text);
      const { runtime, materializer, model } = await files.compile(
         url,
         overlay,
      );
      const overStored = overStoredUpstreamNotes(
         args.modelDef,
         plans,
         queries,
         modelFile,
         args.materialized,
      );
      const overStoredSources = await compileOverStoredUpstreams({
         files,
         modelDef: args.modelDef,
         modelFile,
         url,
         plans,
         queries,
         overStored,
         materialized: args.materialized,
      });
      return {
         generated: {
            plans,
            queries,
            model,
            materializer,
            runtime,
            text,
            overStoredSources,
            overStoredCandidates: overStored.candidates,
         },
         refused,
         refusedQueries,
      };
   } catch (error) {
      return {
         refused,
         refusedQueries,
         failure: companionFailure("persisted_view", error),
         omitted: companionOmissions("persisted_view", {
            file: companionHref,
            names: plannedNames,
         }),
      };
   }
}

/**
 * The persisted views and queries that may be built over their upstreams'
 * stored tables, and what their second compile needs: a database view or
 * query (no `storage=`) whose definition reaches a source whose own notes
 * declare a database `#@ persist` (each such note kept, `keep`), or one
 * database `#(materialize)` source planned as a plain table in this model
 * ({@link storedMaterializeUpstream}: its copy re-rooted on its build
 * source, `stored`, by the build sourceID), and no source stored where the
 * view cannot read it in place (a `storage=` destination, a second
 * `#(materialize)`, another zone). The rest build from their definitions,
 * as planned.
 */
function overStoredUpstreamNotes(
   modelDef: unknown,
   views: readonly PersistedViewPlan[],
   queries: readonly PersistedQueryPlan[],
   modelFile: string,
   materialized: MaterializedBases | undefined,
): {
   keep: Map<string, Set<string>>;
   candidates: Set<string>;
   stored: Map<string, StoredMaterializeBase>;
} {
   const map = SourceMap.of(modelDef);
   const contents =
      (modelDef as { contents?: Record<string, unknown> }).contents ?? {};
   // By build sourceID: each candidate's own upstreams' notes.
   const keep = new Map<string, Set<string>>();
   const candidates = new Set<string>();
   const stored = new Map<string, StoredMaterializeBase>();
   const plans: {
      buildSourceID: string;
      storage?: string;
      root: unknown;
   }[] = [
      ...views.map((p) => ({ ...p, root: map.node(p.sourceID)?.def })),
      ...queries.map((p) => ({ ...p, root: contents[p.queryName] })),
   ];
   for (const plan of plans) {
      if (plan.storage !== undefined || plan.root === undefined) continue;
      const spans: string[] = [];
      let chainable = true;
      for (const sourceID of reachedSourceIDsOf(plan.root, modelDef, map)) {
         const upstream = upstreamTableOf(map, sourceID);
         if (upstream === undefined) continue;
         if (upstream === "unreadable") {
            chainable = false;
            break;
         }
         spans.push(...upstream);
      }
      const base = storedMaterializeUpstream(
         map,
         plan.root,
         modelDef,
         modelFile,
         materialized,
      );
      if (!chainable || base === "unusable") continue;
      if (spans.length === 0 && base === undefined) continue;
      keep.set(plan.buildSourceID, new Set(spans));
      if (base) stored.set(plan.buildSourceID, base);
      candidates.add(plan.buildSourceID);
   }
   return { keep, candidates, stored };
}

/**
 * What the source `sourceID` is as an upstream a view or query may read the
 * table of: the spans of its own database `#@ persist` notes; `unreadable`
 * when it is stored where the view cannot read it in place (`storage=`) or
 * its notes cannot be placed; undefined when its own notes do not declare
 * `#@ persist`.
 */
function upstreamTableOf(
   map: SourceMap,
   sourceID: string,
): string[] | "unreadable" | undefined {
   const def = map.node(sourceID)?.def as
      | { annotations?: AnnotationsDef }
      | undefined;
   // A `#(materialize)` upstream is read through its build source
   // (storedMaterializeUpstream), never its author's notes.
   const own = ownLevelNotes(def?.annotations).filter(
      (n) => routeOfNote(n.text) === "@",
   );
   if (!notesDeclarePersist(own)) return undefined;
   const tag = readNotesIfAny(own, "@");
   const placed = tag ? placementOf(tag, "persist") : undefined;
   if (!placed || !("placement" in placed)) return "unreadable";
   if (placed.placement.tier !== "database") return "unreadable";
   const spans = own.flatMap((n) => {
      const at = (n as { at?: NotePosition }).at;
      return at ? [noteSpanKey(at.url, at.range)] : [];
   });
   return spans.length === own.length ? spans : "unreadable";
}

/**
 * Each candidate compiled a second time, alone, in a companion of its own
 * over the author's files with its own upstreams' notes kept and, over a
 * `#(materialize)` upstream, its source imported from a copy of the model
 * that re-roots that upstream ({@link overStoredUpstreamNotes}): its build
 * source by its sourceID. One candidate's compile never decides another's.
 * A candidate whose compile fails, or whose copy or upstream build source
 * cannot be made, is left out, and builds from its definition, as planned.
 */
async function compileOverStoredUpstreams(args: {
   files: GeneratedFiles;
   modelDef: unknown;
   modelFile: string;
   url: URL;
   plans: readonly PersistedViewPlan[];
   queries: readonly PersistedQueryPlan[];
   overStored: ReturnType<typeof overStoredUpstreamNotes>;
   materialized: MaterializedBases | undefined;
}): Promise<ReadonlyMap<string, OverStoredBase>> {
   const out = new Map<string, OverStoredBase>();
   for (const buildSourceID of args.overStored.candidates) {
      const found = await compileOneOverStored(args, buildSourceID);
      if (found) out.set(buildSourceID, found);
   }
   return out;
}

/** One candidate's compile ({@link compileOverStoredUpstreams}), or undefined. */
async function compileOneOverStored(
   args: Parameters<typeof compileOverStoredUpstreams>[0],
   buildSourceID: string,
): Promise<OverStoredBase | undefined> {
   const { overStored, files, modelFile } = args;
   const views = args.plans.filter((p) => p.buildSourceID === buildSourceID);
   const queries = args.queries.filter(
      (p) => p.buildSourceID === buildSourceID,
   );
   try {
      const kept = await files.blanked(
         args.modelDef,
         overStored.keep.get(buildSourceID) ?? new Set(),
      );
      if (!kept) return undefined;
      const base = overStored.stored.get(buildSourceID);
      // Over a `#(materialize)` upstream: a copy of the model, re-rooted on
      // the upstream's build source, which its source is imported from.
      let copy: { url: string; text: string } | undefined;
      if (base) {
         const modelURL = pathToFileURL(modelFile);
         copy = storedBaseCopies(
            new Map([[base.plan.sourceName, base]]),
            kept.get(overlayKey(modelURL)) ?? (await files.readIfAny(modelURL)),
            modelFile,
         ).get(base.plan.sourceName);
         if (!copy) return undefined;
      }
      const from = copy ? path.basename(new URL(copy.url).pathname) : undefined;
      const text = generatePersistedResultFile(
         views,
         queries,
         path.basename(modelFile),
         () => from,
      );
      if (text === undefined) return undefined;
      const overlay = new Map(kept);
      for (const [file, fileText] of args.materialized?.files ?? []) {
         overlay.set(file, fileText);
      }
      if (copy) overlay.set(copy.url, copy.text);
      overlay.set(args.url.href, text);
      const compiled = await files.compile(args.url, overlay);
      const sources = plannedSources(
         await compiled.runtime.getBuildTargets(compiled.model),
      );
      const source = sources.find((s) => s.sourceID === buildSourceID);
      if (!source) return undefined;
      if (!base) return { source };
      const declaring = pathToFileURL(
         path.join(path.dirname(modelFile), base.importFrom),
      ).href;
      const baseBuildSource = sources.find(
         (s) =>
            s.name === base.plan.buildSourceName &&
            overlayKey(splitSourceID(s.sourceID)?.url ?? "") ===
               overlayKey(declaring),
      );
      if (!baseBuildSource) return undefined;
      return {
         source,
         baseBuildSource,
         rerootedSourceID: base.plan.sourceID,
      };
   } catch {
      return undefined;
   }
}
