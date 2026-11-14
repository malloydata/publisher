// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The serve verdict (materialization v2, D2): for each source the package
 * materializes, whether queries will be answered from its table, and when not,
 * why and what to change. ms2data/service#6277 asks for it as package state at
 * publish, #7090 for the same answer before anything is published, and #6446
 * for a reason in place of a silent live fallback.
 *
 * A pure function of facts the package already holds (the build plan, the
 * published surface, the entry-point gates, the storage mode, what is bound),
 * so a dry run, a load and every bind compute it the same way. Per query, the
 * path report says which table answered one query; this says, per table,
 * whether any query can be answered from it at all.
 *
 * Reasons are open strings, not an enum, so a later phase can add one without
 * breaking a strict client (the control plane rejects unknown enum values).
 */

import type { components } from "../../../api";

type BuildPlan = components["schemas"]["BuildPlan"];
type PersistSourcePlan = components["schemas"]["PersistSourcePlan"];

/** The `code` of the package warning for a materialized source no published query can reach. */
export const MATERIALIZATION_UNREACHABLE_CODE = "materialization-unreachable";

/**
 * The `code` of the package warning for a `storage=` source whose destination
 * the environment does not configure (P13): every build into it fails, so the
 * source serves live.
 */
const MATERIALIZATION_DESTINATION_MISSING_CODE =
   "materialization-destination-missing";

/**
 * The `code` of the package warning for a colocated source whose connection
 * cannot hold a table (P13): a colocated build writes into that connection, so
 * the source serves live.
 */
const MATERIALIZATION_CONNECTION_CANNOT_HOLD_TABLE_CODE =
   "materialization-connection-cannot-hold-table";

/**
 * The live verdict reasons that carry a publish warning of their own, with its
 * code. The other live reasons are warned where their cause is found (storage
 * mode, refusals, rollups over a stored base).
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
   /** A rollup over a `storage=` base, left out of the plan (#6293). */
   | "rollup_over_stored_base"
   /** Nothing on the package's published surface reaches it (#6277). */
   | "unreachable"
   /** Every entry point over it carries a row-level gate the tier cannot keep. */
   | "row_level_gate"
   /** Its table is past its freshness window, and its fallback does not serve stale. */
   | "stale"
   /** Its storage table is bound, but every model's source of that name is another source. */
   | "shadowed"
   /** `storage=` names a destination the environment does not configure (P13). */
   | "destination_missing"
   /** Colocated, on a connection whose type creates no tables (P13). */
   | "connection_cannot_hold_table"
   /**
    * A `#(materialize)` source, which only the served copy reads: the served
    * copy is off (PUBLISHER_MATERIALIZE or PERSIST_STORAGE_MODE), or no
    * model's served copy re-roots it (see the `served-copy-not-served`
    * warning), or it is colocated, which the served copy does not serve yet.
    */
   | "served_copy";

/** Whether one named view of a stored source compiles against its serve shape (#7090). */
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
   origin: "persist" | "preaggregate";
   /** Where its table lives: the source's own connection, or a `storage=` destination. */
   tier: "colocated" | "storage";
   /**
    * `serves`: bound, and queries can be answered from its table.
    * `not_built`: nothing is bound yet; queries will be answered from its
    * table once a build binds it. `live`: no query will be, for `reason`.
    */
   verdict: "serves" | "not_built" | "live";
   reason?: ServeVerdictReason;
   /** What the verdict means for this source, in a sentence. */
   message: string;
   /** For a `live` verdict, the changes that would let it serve. */
   fixes?: string[];
   /** Per named view, for a stored source on a preview (#7090). */
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
    * Storage: the served copy keeps the author's gates and answers a gated
    * entry point over a stored table, the gate grafted on it (R3,
    * `materialize.gatedSources`), so {@link gated} no longer means live.
    * Rollups are never served past a gate.
    */
   servedCopyKeepsGates?: boolean;
   /**
    * A `#(preaggregate)` rollup over a gated base whose gates the router
    * grafts on its variant at read (R3 for rollups, row 1): a gated entry
    * point is answered from it, so it is not `row_level_gate`.
    */
   rollupKeepsGates?(sourceID: string): boolean;
   /**
    * Why the bind refuses this source although the plan holds it (the
    * eligibility gate the bind applies, such as a rollup name two models
    * share), or undefined when the bind keeps it.
    */
   refusedAtBind?(
      sourceID: string,
      source: PersistSourcePlan,
      tier: "colocated" | "storage",
   ): string | undefined;
   /** Colocated: the plan's sourceEntityIds the bound manifest has a table for. */
   boundEntityIds: ReadonlySet<string>;
   /**
    * Colocated: the bound sourceEntityIds still within their freshness window
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
   materialize?(sourceID: string): "serves" | "off" | "not_served" | undefined;
   /** Rollups over a stored base (#6293), left out of the plan. */
   rollupsOverStoredBase?: readonly { baseName: string; modelPath?: string }[];
   /**
    * A persisted view's generated build source (`#@ persist` on a view, P14),
    * by its plan sourceID: the source and view it stores. A query reaches it
    * by calling the view on that source, so reach and gates are the source's.
    */
   persistedViewOf?(
      sourceID: string,
   ): { sourceID: string; sourceName: string; viewName: string } | undefined;
   /** Per stored source's plan sourceID, its views' verdicts (a preview). */
   views?: ReadonlyMap<string, ViewServeVerdict[]>;
   /**
    * Whether the environment configures this storage destination: `false`
    * only when the destination list is known and does not name it. Absent, or
    * `undefined` (the list is not known), never warns: a deployment fact that
    * cannot be read must not send a source live.
    */
   destinationConfigured?(destinationName: string): boolean | undefined;
   /**
    * Whether this connection can hold a table, read from its capability:
    * `false` only when its configured type creates no tables. Absent, or
    * `undefined` (not an environment connection, or an unknown type), never
    * warns.
    */
   connectionCanHoldTable?(connectionName: string): boolean | undefined;
}

/** The tier a plan source's table lives in. */
function planTier(source: PersistSourcePlan): "colocated" | "storage" {
   return source.annotationFields?.storage?.trim() ? "storage" : "colocated";
}

/**
 * Every materialized source's verdict, in plan order: planned sources first,
 * then refused ones, then rollups left out of the plan.
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
      // A preaggregate-tier refusal sits beside its base's own plan entry,
      // which already has a verdict.
      if (sourceID in sources) continue;
      out.push({
         sourceName: refused.name,
         sourceID,
         ...(refused.modelPath ? { modelPath: refused.modelPath } : {}),
         origin: refused.tier === "preaggregate" ? "preaggregate" : "persist",
         tier: refused.tier === "storage" ? "storage" : "colocated",
         verdict: "live",
         reason: "refused",
         message: `Not built, so queries on it are answered live: ${refused.message}`,
         fixes: [
            "Remove what the refusal names, or drop the annotation if the source is not meant to be materialized.",
         ],
      });
   }
   for (const rollup of facts.rollupsOverStoredBase ?? []) {
      out.push({
         sourceName: rollup.baseName,
         sourceID: `${rollup.baseName}#preaggregate`,
         ...(rollup.modelPath ? { modelPath: rollup.modelPath } : {}),
         origin: "preaggregate",
         tier: "storage",
         verdict: "live",
         reason: "rollup_over_stored_base",
         message:
            `The rollups of \`${rollup.baseName}\` are not built, because it is ` +
            `stored with storage=; its queries are answered from its own table.`,
         fixes: [
            `Declare the rollup on a source \`${rollup.baseName}\` is built from, or drop storage= from \`${rollup.baseName}\`.`,
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
   const origin = source.origin === "preaggregate" ? "preaggregate" : "persist";
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
   const view = facts.persistedViewOf?.(sourceID);
   // malloy-text: markdown code formatting in a verdict message, not Malloy
   const named = rollupBase
      ? `The rollup of \`${rollupBase}\` at (${source.preaggregate?.grainDimensions.join(", ")})`
      : `\`${source.name}\``;
   const subject = view
      ? `The persisted view \`${view.viewName}\` of \`${view.sourceName}\``
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

   // Deployment facts (P13), ahead of the storage mode: under `write-only` the
   // build runs and fails on them, so they are the reason worth naming. Under
   // `off` the annotation is ignored, so a destination is never read. A table
   // already bound is never called unbuildable: what serves is reported as it
   // is, and the fact is read only to explain a table nothing has bound.
   const destination = source.annotationFields?.storage?.trim();
   // Read once, here and in the storage branch below: one name-keyed lookup.
   const storageBound = facts.boundStorageNames.has(source.name);
   if (
      tier === "storage" &&
      destination &&
      facts.storageMode !== "off" &&
      !storageBound &&
      facts.destinationConfigured?.(destination) === false
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
      tier === "colocated" &&
      !facts.boundEntityIds.has(source.sourceEntityId) &&
      facts.connectionCanHoldTable?.(source.connectionName) === false
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
   const readID = rollupBase ? undefined : (view?.sourceID ?? sourceID);
   if (!facts.reachable(readID, readName)) {
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
   // (never the serve shape, which drops its bindings, nor the colocated
   // manifest, which Malloy never substitutes for it), so the served copy
   // decides its verdict.
   const materialized = facts.materialize?.(sourceID);

   // Colocated persist is substituted inside Malloy, gates and all; storage,
   // a `#(materialize)` table and rollups go through the router, which skips
   // a gated entry point, unless the served copy keeps the gates (R3), or a
   // `#(preaggregate)` rollup's variant keeps them (row 1).
   if (
      ((origin === "preaggregate" && !facts.rollupKeepsGates?.(sourceID)) ||
         ((tier === "storage" || materialized !== undefined) &&
            !facts.servedCopyKeepsGates)) &&
      facts.gated(readID, readName, source.modelPath)
   ) {
      return live(
         "row_level_gate",
         `Every published entry point that reads ${subject} carries a ` +
            `row-level gate (#(authorize) or #(access_filter)), which ` +
            `${origin === "preaggregate" ? "a rollup" : tier === "storage" ? "the storage tier" : "the served copy"} cannot keep ` +
            `yet, so every query that reaches it is answered live.`,
         [
            "Materialize the source without storage= (colocated), where the gate is kept.",
            "Or publish an ungated entry point over it, for the queries that need no gate.",
         ],
      );
   }

   const refusedAtBind = facts.refusedAtBind?.(sourceID, source, tier);
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
         "served_copy",
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
            verdict: "not_built",
            message: `${subject} will serve from storage once a build binds its table.`,
         };
      }
      if (materialized === "not_served") return notServed();
      if (
         materialized === undefined &&
         origin === "persist" &&
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
         verdict: "not_built",
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
