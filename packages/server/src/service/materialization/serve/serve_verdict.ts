// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The serve verdict: for each source the package materializes, whether
 * queries will be answered from its table, and when not, why and what to
 * change: as package state at publish, as the same answer before anything is
 * published (a preview), and as a reason in place of a silent live fallback.
 *
 * A pure function of facts the package already holds (the build plan, the
 * published surface, the entry-point gates, the storage mode, what is bound),
 * so a dry run, a load and every bind compute it the same way. Per query, the
 * path report says which table answered one query; this says, per table,
 * whether any query can be answered from it at all.
 *
 * Reasons are open strings, not an enum, so a new one can be added without
 * breaking a strict client (the control plane rejects unknown enum values).
 */

import type { components } from "../../../api";
import {
   isPersistedQuerySourceID,
   isPersistedResultSourceID,
   isRollupOrigin,
   type TableOrigin,
} from "../domain/table_plans";

type BuildPlan = components["schemas"]["BuildPlan"];
type PersistSourcePlan = components["schemas"]["PersistSourcePlan"];

/** The `code` of the package warning for a materialized source no published query can reach. */
export const MATERIALIZATION_UNREACHABLE_CODE = "materialization-unreachable";

/**
 * The `code` of the package warning for a `storage=` source whose destination
 * the environment does not configure: every build into it fails, so the
 * source serves live.
 */
const MATERIALIZATION_DESTINATION_MISSING_CODE =
   "materialization-destination-missing";

/**
 * The `code` of the package warning for a database source whose connection
 * cannot hold a table: a database build writes into that connection, so
 * the source serves live.
 */
const MATERIALIZATION_CONNECTION_CANNOT_HOLD_TABLE_CODE =
   "materialization-connection-cannot-hold-table";

/**
 * The live verdict reasons that carry a publish warning of their own, with its
 * code. The other live reasons are warned where their cause is found (storage
 * mode, refusals).
 */
export const VERDICT_WARNING_CODES: Readonly<
   Partial<Record<ServeVerdictReason, string>>
> = {
   unreachable: MATERIALIZATION_UNREACHABLE_CODE,
   destination_missing: MATERIALIZATION_DESTINATION_MISSING_CODE,
   connection_cannot_hold_table:
      MATERIALIZATION_CONNECTION_CANNOT_HOLD_TABLE_CODE,
};

/** Why a materialized source's table answers no query. Open: see the module doc. */
type ServeVerdictReason =
   /** The plan refused to build it (the eligibility gate). */
   | "refused"
   /** `storage=` while PERSIST_STORAGE_MODE is not `on`. */
   | "storage_mode"
   /** Nothing on the package's published surface reaches it. */
   | "unreachable"
   /** Every entry point over it carries a row-level gate the tier cannot keep. */
   | "row_level_gate"
   /** Its table is past its freshness window, and its fallback does not serve stale. */
   | "stale"
   /** Its storage table is bound, but every model's source of that name is another source. */
   | "shadowed"
   /** `storage=` names a destination the environment does not configure. */
   | "destination_missing"
   /** Database, on a connection whose type creates no tables. */
   | "connection_cannot_hold_table"
   /**
    * A `#(materialize)` source, which only the served copy reads, and the
    * served copy is off (PUBLISHER_MATERIALIZE, or PERSIST_STORAGE_MODE for a
    * `storage=` table): the same name `RefusedSource.reason` gives the setting.
    */
   | "materialization_off"
   /**
    * A `#(materialize)` source, which only the served copy reads, bound, but
    * no model's served copy re-roots it (see the `served-copy-not-served`
    * warning).
    */
   | "served_copy";

/** What a persisted result's build source stores: a view of a source, or a named query. */
export type PersistedResultOf =
   | {
        sourceID: string;
        sourceName: string;
        viewName: string;
        queryName?: never;
     }
   | { queryName: string; sourceID?: string; sourceName?: string };

/**
 * Whether one named view of a stored source compiles against a compile-only
 * served copy over bindings declared from the plan's output columns, on a
 * preview.
 */
export interface ViewServeVerdict {
   view: string;
   verdict: "serves" | "live";
   /** The compile error that sends it live. */
   detail?: string;
}

/** One materialized source's serve verdict. */
export interface ServeVerdict {
   sourceName: string;
   sourceID: string;
   modelPath?: string;
   /**
    * The mechanism that declared it, in the table kinds' words: `persist`
    * (`#@ persist` on a source), `materialize` (`#(materialize)`), `view`
    * (`#@ persist` on a view), `query` (`#@ persist` on a named query), or
    * `preaggregate` (a rollup).
    */
   origin: TableOrigin;
   /** Where its table lives: the source's own connection, or a `storage=` destination. */
   tier: "database" | "storage";
   /**
    * `serves`: bound, and queries can be answered from its table.
    * `awaiting_build`: nothing is bound yet; queries will be answered from
    * its table once a build binds it. `live`: no query will be, for `reason`.
    */
   verdict: "serves" | "awaiting_build" | "live";
   reason?: ServeVerdictReason;
   /** What the verdict means for this source, in a sentence. */
   message: string;
   /** For a `live` verdict, the changes that would let it serve. */
   fixes?: string[];
   /** Per named view, for a stored source on a preview. */
   views?: ViewServeVerdict[];
}

/** What {@link computeServeVerdicts} reads. Every predicate is per source. */
export interface ServeVerdictFacts {
   plan: BuildPlan | null;
   /** PERSIST_STORAGE_MODE. */
   storageMode: "on" | "off" | "write-only" | string;
   /**
    * Whether some query on the published surface can read this source. A
    * rollup asks about its base, by name: no sourceID of the base is planned.
    */
   reachable(sourceID: string | undefined, sourceName: string): boolean;
   /**
    * Whether every published entry point that reads this source carries a
    * row-level gate, so the router skips the tier for every query that could
    * reach it (route, then govern: a gated entry point is answered live).
    */
   gated(
      sourceID: string | undefined,
      sourceName: string,
      modelPath?: string,
   ): boolean;
   /**
    * Whether a query let past a gated entry point may be answered from this
    * source's table, as the gate policy (`domain/gate_policy.ts`) decides it
    * for the router: the served copy over a table it keeps the gate on (a
    * database one, a `storage=` one admitted past its own gate, any under
    * `materialize.gatedSources`), or a rollup whose variant keeps its base's
    * gates.
    */
   keepsGate(sourceID: string, tier: "database" | "storage"): boolean;
   /**
    * Why the bind refuses this source although the plan holds it (the
    * eligibility gate the bind applies, such as a rollup name two models
    * share), or undefined when the bind keeps it.
    */
   refusedAtBind(
      sourceID: string,
      source: PersistSourcePlan,
      tier: "database" | "storage",
   ): string | undefined;
   /** Database: the plan's sourceEntityIds the bound manifest has a table for. */
   boundEntityIds: ReadonlySet<string>;
   /**
    * Database: the bound sourceEntityIds still within their freshness window
    * (or whose fallback serves stale). Absent: every bound one is.
    */
   freshEntityIds?: ReadonlySet<string>;
   /** Storage: the source names the package holds a serve binding for. */
   boundStorageNames: ReadonlySet<string>;
   /** Storage: the source names some model kept a binding for. */
   servedStorageNames: ReadonlySet<string>;
   /** Storage: the bound source names still fresh. Absent: every bound one is. */
   freshStorageNames?: ReadonlySet<string>;
   /**
    * The plan's `#(materialize)` sources, by sourceID, and whether the served
    * copy answers each from its table: `serves` when some model's compiled
    * served copy re-roots it (or a served copy has yet to compile), `off` when
    * the served copy is off, `not_served` otherwise. Absent: no source is a
    * `#(materialize)` source.
    */
   materialize(sourceID: string): "serves" | "off" | "not_served" | undefined;
   /**
    * A persisted view's generated build source (`#@ persist` on a view),
    * by its plan sourceID: the source and view it stores. A query reaches it
    * by calling the view on that source, so reach and gates are the source's.
    * A persisted named query's build source carries `queryName`, and its run
    * target's `sourceID` and name when it runs a named source (absent for an
    * inline run target, whose reach is not judged).
    */
   persistedViewOf(sourceID: string): PersistedResultOf | undefined;
   /**
    * Whether the author's source of this sourceID is declared with
    * `#(materialize)`, for a refused source the plan holds no plan for.
    * Absent: none is.
    */
   declaredMaterialize?(sourceID: string): boolean;
   /** Per stored source's plan sourceID, its views' verdicts (a preview). */
   views?: ReadonlyMap<string, ViewServeVerdict[]>;
   /**
    * Whether the environment configures this storage destination: `false`
    * only when the destination list is known and does not name it. Absent, or
    * `undefined` (the list is not known), never warns: a deployment fact that
    * cannot be read must not send a source live.
    */
   destinationConfigured(destinationName: string): boolean | undefined;
   /**
    * Whether this connection can hold a table, read from its capability:
    * `false` only when its configured type creates no tables. Absent, or
    * `undefined` (not an environment connection, or an unknown type), never
    * warns.
    */
   connectionCanHoldTable(connectionName: string): boolean | undefined;
}

/**
 * The mechanism that declared a stored source ({@link ServeVerdict.origin}),
 * read off the plan: never off its name alone.
 */
function originOf(
   sourceID: string,
   rollup: boolean,
   facts: Pick<ServeVerdictFacts, "materialize" | "declaredMaterialize">,
): ServeVerdict["origin"] {
   if (rollup) return "rollup";
   if (isPersistedResultSourceID(sourceID)) {
      return isPersistedQuerySourceID(sourceID) ? "query" : "view";
   }
   // A planned `#(materialize)` source has a plan; a refused one has only
   // its declaration.
   if (
      facts.materialize(sourceID) !== undefined ||
      facts.declaredMaterialize?.(sourceID) === true
   ) {
      return "materialize";
   }
   return "persist";
}

/** The tier a plan source's table lives in. */
function planTier(source: PersistSourcePlan): "database" | "storage" {
   return source.annotationFields?.storage?.trim() ? "storage" : "database";
}

/**
 * Every materialized source's verdict, in plan order: planned sources first,
 * then refused ones.
 */
export function computeServeVerdicts(facts: ServeVerdictFacts): ServeVerdict[] {
   const out: ServeVerdict[] = [];
   const sources = facts.plan?.sources ?? {};
   for (const [sourceID, source] of Object.entries(sources)) {
      out.push(verdictFor(sourceID, source, facts));
   }
   for (const [sourceID, refused] of Object.entries(
      facts.plan?.refusedSources ?? {},
   )) {
      // A rollup's refusal may sit beside its own plan entry, which already
      // has a verdict.
      if (sourceID in sources) continue;
      out.push({
         sourceName: refused.name,
         sourceID,
         ...(refused.modelPath ? { modelPath: refused.modelPath } : {}),
         origin: originOf(
            sourceID,
            // `preaggregate` on `tier` is how a record before this release
            // said rollup.
            isRollupOrigin(refused.origin) || refused.tier === "preaggregate",
            facts,
         ),
         tier: refused.tier === "storage" ? "storage" : "database",
         verdict: "live",
         reason: "refused",
         message: `Not built, so queries on it are answered live: ${refused.message}`,
         fixes: [
            "Remove what the refusal names, or drop the annotation if the source is not meant to be materialized.",
         ],
      });
   }
   return out;
}

function verdictFor(
   sourceID: string,
   source: PersistSourcePlan,
   facts: ServeVerdictFacts,
): ServeVerdict {
   const s = verdictSubjectOf(sourceID, source, facts);
   const { origin, tier, rollupBase, view, subject, live } = s;

   // Deployment facts, ahead of the storage mode: under `write-only` the
   // build runs and fails on them, so they are the reason worth naming. Under
   // `off` the annotation is ignored, so a destination is never read. A table
   // already bound is never called unbuildable: what serves is reported as it
   // is, and the fact is read only to explain a table nothing has bound.
   const destination = source.annotationFields?.storage?.trim();
   // Read once, here and in the storage branch below: one name-keyed lookup.
   const storageBound = facts.boundStorageNames.has(source.name);
   const deployment = deploymentVerdict(s, destination, storageBound);
   if (deployment) return deployment;

   if (tier === "storage" && facts.storageMode !== "on") {
      return live(
         "storage_mode",
         facts.storageMode === "off"
            ? `${subject} declares storage=, which PERSIST_STORAGE_MODE=off ignores.`
            : `${subject} is built into storage, but PERSIST_STORAGE_MODE is ` +
                 `${facts.storageMode}, so no query is routed to it.`,
         ["Set PERSIST_STORAGE_MODE=on on the server, or drop storage=."],
      );
   }

   const readName = rollupBase ?? view?.sourceName ?? source.name;
   const readID = rollupBase ? undefined : view ? view.sourceID : sourceID;
   // A named query over an inline run target has no source to reach it by.
   const inlineQuery = view?.queryName !== undefined && !view.sourceID;
   if (!inlineQuery && !facts.reachable(readID, readName)) {
      return live(
         "unreachable",
         `${subject} is materialized, but no source on the package's published ` +
            `surface reaches it, so no query can be answered from its table.`,
         [
            `Publish \`${readName}\`, or a source built on it, in index.malloy.`,
            "Or drop the annotation if nothing is meant to query it.",
         ],
      );
   }

   // `#(materialize)` is served by the served copy alone, on either tier
   // (never through the database build manifest, which Malloy never
   // substitutes for it), so the served copy decides its verdict.
   const materialized = facts.materialize(sourceID);

   // A gated entry point is answered from the table only when the gate
   // policy keeps the gate on it, as the router asks it.
   if (
      !facts.keepsGate(sourceID, tier) &&
      facts.gated(readID, readName, source.modelPath)
   ) {
      return live(
         "row_level_gate",
         `Every published entry point that reads ${subject} carries a ` +
            `row-level gate (#(authorize) or #(access_filter)), which ` +
            `${origin === "rollup" ? "a rollup" : tier === "storage" ? "the storage tier" : "this table"} cannot keep ` +
            `yet, so every query that reaches it is answered live.`,
         [
            // A database table already follows the first fix: only the
            // gate's shape can change there.
            tier === "storage"
               ? "Materialize the source without storage= (database), where the gate is kept."
               : "Make the gate the entry point's own row-level gate, which a table in the source's database keeps.",
            "Or publish an ungated entry point over it, for the queries that need no gate.",
         ],
      );
   }

   const refusedAtBind = facts.refusedAtBind(sourceID, source, tier);
   if (refusedAtBind !== undefined) {
      return live(
         "refused",
         `${subject} is planned, but the bind refuses to serve it, so queries ` +
            `on it are answered live: ${refusedAtBind}`,
         [
            "Remove what the refusal names, or drop the annotation if the source is not meant to be materialized.",
         ],
      );
   }

   if (materialized === "off") {
      return live(
         "materialization_off",
         `${subject} is materialized with #(materialize), which only the ` +
            `served copy reads, and the served copy is off, so its queries are ` +
            `answered live.`,
         [
            tier === "storage"
               ? "Set PUBLISHER_MATERIALIZE=true and PERSIST_STORAGE_MODE=on."
               : "Set PUBLISHER_MATERIALIZE=true.",
         ],
      );
   }
   return boundVerdict(s, storageBound, materialized);
}

/** One planned source, as every phase of its verdict names and reads it. */
interface VerdictSubject {
   sourceID: string;
   source: PersistSourcePlan;
   facts: ServeVerdictFacts;
   origin: ServeVerdict["origin"];
   tier: "database" | "storage";
   base: Omit<ServeVerdict, "verdict" | "message">;
   rollupBase: string | undefined;
   view: ReturnType<NonNullable<ServeVerdictFacts["persistedViewOf"]>>;
   /** How a message names the source: a rollup, a persisted view, or itself. */
   subject: string;
   live(
      reason: ServeVerdictReason,
      message: string,
      fixes: string[],
   ): ServeVerdict;
}

/** What a source's verdict says it about: its names and its `live` verdict. */
function verdictSubjectOf(
   sourceID: string,
   source: PersistSourcePlan,
   facts: ServeVerdictFacts,
): VerdictSubject {
   const origin = originOf(sourceID, isRollupOrigin(source.origin), {
      materialize: facts.materialize,
   });
   const tier = planTier(source);
   const base: Omit<ServeVerdict, "verdict" | "message"> = {
      sourceName: source.name,
      sourceID,
      ...(source.modelPath ? { modelPath: source.modelPath } : {}),
      origin,
      tier,
   };
   // A rollup is never queried by name: its base is what a query names.
   const rollupBase = source.preaggregate?.baseSourceName;
   // Nor is a persisted view's build source: a call of the view on its source.
   const view = facts.persistedViewOf(sourceID);
   // malloy-text: markdown code formatting in a verdict message, not Malloy
   const named = rollupBase
      ? `The rollup of \`${rollupBase}\` at (${source.preaggregate?.grainDimensions.join(", ")})`
      : `\`${source.name}\``;
   const subject = view
      ? view.queryName !== undefined
         ? `The persisted query \`${view.queryName}\``
         : `The persisted view \`${view.viewName}\` of \`${view.sourceName}\``
      : named;
   const live = (
      reason: ServeVerdictReason,
      message: string,
      fixes: string[],
   ): ServeVerdict => ({
      ...base,
      verdict: "live",
      reason,
      message,
      fixes,
   });
   return {
      sourceID,
      source,
      facts,
      origin,
      tier,
      base,
      rollupBase,
      view,
      subject,
      live,
   };
}

/**
 * A deployment fact that sends the source live: a `storage=`
 * destination the environment does not configure, or a database connection
 * that cannot hold a table. Undefined when neither does.
 */
function deploymentVerdict(
   s: VerdictSubject,
   destination: string | undefined,
   storageBound: boolean,
): ServeVerdict | undefined {
   const { sourceID, source, facts, tier, subject, live } = s;
   if (
      tier === "storage" &&
      destination &&
      facts.storageMode !== "off" &&
      !storageBound &&
      facts.destinationConfigured(destination) === false
   ) {
      // A preview's per-view verdicts still say what would serve once the
      // destination exists, so they are kept.
      const views = facts.views?.get(sourceID);
      return {
         ...(views && views.length > 0 ? { views } : {}),
         ...live(
            "destination_missing",
            `${subject} declares storage="${destination}", but this environment ` +
               `configures no storage destination of that name, so no build can ` +
               `write its table and queries on it are answered live.`,
            [
               `Configure a storage destination named "${destination}" on the environment ` +
                  `(one the config lists but the server rejected, such as an unsupported type, is skipped with the reason in the server log).`,
               "Or name a configured destination in storage=, or drop storage= to build it in its own warehouse.",
            ],
         ),
      };
   }
   if (
      tier === "database" &&
      !facts.boundEntityIds.has(source.sourceEntityId) &&
      facts.connectionCanHoldTable(source.connectionName) === false
   ) {
      return live(
         "connection_cannot_hold_table",
         `${subject} is materialized in its own warehouse, but its connection ` +
            `'${source.connectionName}' cannot create tables, so no build can ` +
            `write its table and queries on it are answered live.`,
         [
            "Add storage= naming a configured storage destination, so the table is written there instead.",
            "Or drop the annotation if the source is not meant to be materialized.",
         ],
      );
   }
   return undefined;
}

/**
 * The verdict of a source no deployment fact, storage mode, reach, gate or
 * refusal sends live: served from its bound table, not built yet, or live
 * for what its binding says (stale, shadowed, not read by the served copy).
 */
function boundVerdict(
   s: VerdictSubject,
   storageBound: boolean,
   materialized: ReturnType<NonNullable<ServeVerdictFacts["materialize"]>>,
): ServeVerdict {
   const { sourceID, source, facts, origin, tier, base, subject, live } = s;
   const notServed = (): ServeVerdict =>
      live(
         "served_copy",
         `${subject}'s table is bound, but no model's served copy reads ` +
            `it, so no query is answered from it: the served copy reads a ` +
            `source it is built on from that source's table instead, or ` +
            `leaves it as written for the reason a served-copy-not-served ` +
            `warning gives.`,
         [
            "Fix what the served-copy-not-served warning names, if there is one.",
            "Or drop #(materialize) from it if a source it is built on already answers its queries.",
         ],
      );

   const stale = (): ServeVerdict =>
      live(
         "stale",
         `${subject}'s table is past its freshness window, so no query is ` +
            `answered from it until a build refreshes it.`,
         [
            "Run a build, or widen the freshness window, or set its fallback to stale_ok.",
         ],
      );

   if (tier === "storage") {
      const views = facts.views?.get(sourceID);
      const withViews = views && views.length > 0 ? { views } : {};
      if (!storageBound) {
         return {
            ...base,
            ...withViews,
            verdict: "awaiting_build",
            message: `${subject} will serve from storage once a build binds its table.`,
         };
      }
      if (materialized === "not_served") return notServed();
      if (
         materialized === undefined &&
         origin !== "rollup" &&
         !facts.servedStorageNames.has(source.name)
      ) {
         return live(
            "shadowed",
            `${subject}'s table is bound, but every model's source of that name ` +
               `is a different source, so none is served from it.`,
            [
               "Give the materialized source a name no other model's source uses.",
            ],
         );
      }
      if (
         facts.freshStorageNames &&
         !facts.freshStorageNames.has(source.name)
      ) {
         return stale();
      }
      return {
         ...base,
         ...withViews,
         verdict: "serves",
         message:
            materialized === "serves"
               ? `${subject} serves from storage, through its served copy.`
               : `${subject} serves from storage.`,
      };
   }

   if (!facts.boundEntityIds.has(source.sourceEntityId)) {
      return {
         ...base,
         verdict: "awaiting_build",
         message: `${subject} will serve from its table once a build binds it.`,
      };
   }
   if (
      facts.freshEntityIds &&
      !facts.freshEntityIds.has(source.sourceEntityId)
   ) {
      return stale();
   }
   if (materialized === "not_served") return notServed();
   return {
      ...base,
      verdict: "serves",
      message:
         materialized === "serves"
            ? `${subject} serves from its table, through its served copy.`
            : `${subject} serves from its table.`,
   };
}
