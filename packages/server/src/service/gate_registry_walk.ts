// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The gate groups in force on a struct, per route, for `extractSourcesFromModelDef`
 * (package load: what is validated and reported per source) and the join-bypass
 * sweep. A thin adapter over `gatePlan` (`./governance/gate_plan.ts`), the one
 * gate-inheritance walk the request path enforces with, so what load reports
 * is what a query is held to.
 *
 * How Malloy carries a base's annotations onto a derived struct:
 *  - `extend {}` (also with `rename:`/`except:`/`accept:`) carrying NO
 *    annotation of its own: the base's note objects are copied, by reference,
 *    onto the derived struct's OWN `annotations`. Reading the struct's own
 *    notes already finds the gate, so authored-vs-inherited must be decided by
 *    note identity, not source identity (see `validateAuthorizeProbes`).
 *  - a deriving statement that DOES carry an annotation demotes the base's
 *    notes to `annotations.inherits`, which the walk follows.
 *  - a `query_source` (`Z is X -> {...}`) carries no `annotations` at all and
 *    its registry entry self-references; the only link back to `X` is
 *    `Z.query.structRef` (`resolveQuerySourceBase`).
 *
 * Bundled into the package-load worker via `source_extraction.ts`: keep
 * imports to `@malloydata/malloy` types and pure annotation helpers.
 */

import type { ModelDef, SourceDef } from "@malloydata/malloy";
import { gatePlan, modelDefGateGraph } from "./governance/gate_plan";

/**
 * The gate groups in force on a struct for one route, one group per declaring
 * level, in walk order: its own notes when it declares any, else what it
 * inherits; for a `query_source`, PLUS its base's and its resolved composite
 * member's (derivation is ADDITIVE, docs/authorize.md). An extension of a
 * query source that declares its own gate on a route replaces its ancestor's
 * on that route (own wins), exactly as enforcement does.
 *
 * Groups, not a flat list (see `AuthorizeMap`): two declaring levels AND, and
 * flattening them would OR them, a row-level leak.
 *
 * Fail-closed: anything the walk cannot follow is a `["false"]` group. A note
 * that does not parse throws, which fails the load.
 */
export function gateExprsForRoute(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   route: string,
): string[][] {
   const { steps } = gatePlan(struct, modelDefGateGraph(modelDef), {
      composite: "resolved",
      routes: [route],
      treatAsOwnGate: true,
   });
   const groups: string[][] = [];
   for (const step of steps) {
      if (step.kind !== "gate") continue;
      if (step.failure?.kind === "unreadable") throw step.failure.error;
      groups.push(step.exprs);
   }
   return groups;
}
