// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The join-bypass check: a gate fires only on the source a query enters
 * through, so a join the model AUTHOR writes from a published source to a
 * locked or row-filtered source hands the joined rows to every caller the
 * published source admits, with the joined source's gate never consulted.
 * (A join the CALLER writes is already gated, as the caller entering that
 * source; see docs/authorize.md.) The docs tell an author to handle this by
 * hand: carry the scoping key in the `on`, or reference the joined column in
 * the entry source's own `#(access_filter)`. This check is what says they
 * did not, and a bypass REFUSES the package load, as a `#(carries)` leak does.
 *
 * Two tests, both mechanical, over every join a published source declares,
 * {@link MAX_JOIN_RECURSION_DEPTH} deep, against the entry source's own gates:
 *
 * - the joined struct is locked outright (`#(authorize) false`) and the entry
 *   source has no gate at all: the "open source over a locked base" leak;
 * - the joined struct's gates read a given the entry source's gates do not
 *   read: the joined rows are scoped by something the entry never asks for.
 *
 * A published source gated on the same givens as what it joins passes, since
 * that is the designed pattern. Only published, reachable entry sources are
 * read: a hidden source is not a place a caller can enter.
 */

import type { ModelDef, SourceDef } from "@malloydata/malloy";
import {
   collectAuthorizeExprsForRoute,
   referencedGivenNames,
} from "./authorize";
import { ownLevelNoteTexts, type AnnotationsDef } from "./annotations";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { findIrSource, isDeniedToAll } from "./classification";
import {
   MAX_JOIN_PATHS,
   MAX_JOIN_RECURSION_DEPTH,
} from "./filter_binding_guard";
import { effectiveAncestorGateExprs } from "./gate_registry_walk";
import type { Model } from "./model";

/** The slice of a compiled join field the check reads. */
interface IrJoin {
   name?: string;
   as?: string;
   join?: string;
   fields?: IrJoin[];
   annotations?: AnnotationsDef;
}

const isDenyAll = (expr: string) => expr.trim().toLowerCase() === "false";
const isAdmitAll = (expr: string) => expr.trim().toLowerCase() === "true";

/**
 * The gate expressions a joined struct's own annotation chain declares on one
 * route: the nearest level that declares any, the way an `extend` inherits a
 * gate until it declares its own. A join carries an inlined copy of its
 * source, not a registered one, so this is read off the copy's annotations.
 */
function chainGateExprs(
   annotations: AnnotationsDef | undefined,
   route: string,
): string[] {
   let node: AnnotationsDef | undefined = annotations;
   for (let hops = 0; node && hops < 64; hops++) {
      const exprs = collectAuthorizeExprsForRoute(
         ownLevelNoteTexts(node),
         route,
      );
      if (exprs.length > 0) return exprs;
      node = node.inherits;
   }
   return [];
}

/**
 * Every gate expression in force on a joined struct, both routes, flattened:
 * its annotation chain, plus whatever the registry walk resolves for a
 * derivation the chain does not show. A gate that cannot be read is treated
 * as a lock, since the query route would deny on it.
 */
function gatesOn(struct: IrJoin, modelDef: ModelDef | undefined): string[] {
   const known = gatesMemo.get(struct);
   if (known) return known;
   const gates = readGatesOn(struct, modelDef);
   gatesMemo.set(struct, gates);
   return gates;
}

/** A joined struct's gates, read once: the registry walk behind them is the cost. */
const gatesMemo = new WeakMap<IrJoin, string[]>();

function readGatesOn(struct: IrJoin, modelDef: ModelDef | undefined): string[] {
   const exprs: string[] = [];
   for (const route of [AUTHORIZE_ROUTE, ACCESS_FILTER_ROUTE]) {
      try {
         exprs.push(...chainGateExprs(struct.annotations, route));
         for (const group of effectiveAncestorGateExprs(
            struct as unknown as SourceDef,
            modelDef,
            route,
         )) {
            exprs.push(...group);
         }
      } catch {
         exprs.push("false");
      }
   }
   return Array.from(new Set(exprs));
}

function givensOf(exprs: readonly string[]): Set<string> {
   const out = new Set<string>();
   for (const expr of exprs) {
      if (isDenyAll(expr) || isAdmitAll(expr)) continue;
      for (const name of referencedGivenNames(expr)) out.add(name);
   }
   return out;
}

/** Whether any source in the package, listed or not, carries a gate. */
function packageHasGate(models: ReadonlyMap<string, Model>): boolean {
   for (const model of models.values()) {
      try {
         for (const source of model.getSources() ?? []) {
            const gated = [
               ...(source.authorize ?? []),
               ...(source.accessFilter ?? []),
            ].some((expr) => !isAdmitAll(expr));
            if (gated) return true;
         }
      } catch {
         continue;
      }
   }
   return false;
}

/**
 * Every author join in the package that bypasses a gate, one message each,
 * in the words the author fixes it by. Empty when there are none.
 */
export function joinBypasses(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): string[] {
   const messages: string[] = [];
   const truncated: string[] = [];
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let sourceInfos: ReturnType<Model["getSourceInfos"]>;
      let apiSources: ReturnType<Model["getSources"]>;
      let modelDef: ModelDef | undefined;
      try {
         sourceInfos = model.getSourceInfos();
         apiSources = model.getSources();
         modelDef = model.getModelDef();
      } catch {
         continue;
      }
      for (const sourceInfo of sourceInfos ?? []) {
         const entry = findIrSource(modelDef, sourceInfo.name) as
            | IrJoin
            | undefined;
         if (!entry) continue;
         const apiSource = apiSources?.find((s) => s.name === sourceInfo.name);
         if (apiSource && isDeniedToAll(apiSource)) continue;
         const entryGates = [
            ...(apiSource?.authorize ?? []),
            ...(apiSource?.accessFilter ?? []),
         ].filter((expr) => !isAdmitAll(expr));
         const entryGivens = givensOf(entryGates);
         const where = `source ${sourceInfo.name} (${modelPath})`;

         let joinPaths = 0;
         let cut = false;
         const walk = (fields: IrJoin[], prefix: string, joins: number) => {
            for (const field of fields) {
               if (
                  typeof field.join !== "string" ||
                  !Array.isArray(field.fields)
               )
                  continue;
               if (
                  joins >= MAX_JOIN_RECURSION_DEPTH ||
                  ++joinPaths > MAX_JOIN_PATHS
               ) {
                  cut = true;
                  continue;
               }
               const name = field.as ?? field.name ?? "?";
               const path = prefix ? `${prefix}.${name}` : name;
               const gates = gatesOn(field, modelDef);
               if (gates.some(isDenyAll)) {
                  if (entryGates.length === 0) {
                     messages.push(
                        `${where} is open to every caller and joins ${path}, ` +
                           `which is locked (#(authorize) false). A gate fires ` +
                           `only on the source a query enters through, so the ` +
                           `lock does not fire through this join and every ` +
                           `caller reads ${path}'s rows. Gate ${sourceInfo.name} ` +
                           `with #(authorize) or #(access_filter), or drop the ` +
                           `join.`,
                     );
                  }
               } else if (gates.length > 0) {
                  const missing = Array.from(givensOf(gates)).filter(
                     (g) => !entryGivens.has(g),
                  );
                  if (missing.length > 0) {
                     messages.push(
                        `${where} joins ${path}, whose gate reads ` +
                           `$${missing.join(", $")}, which no gate on ` +
                           `${sourceInfo.name} reads. A gate fires only on the ` +
                           `source a query enters through, so ${path}'s rows ` +
                           `reach every caller ${sourceInfo.name} admits ` +
                           `unscoped. Scope the entry source on the same given ` +
                           `(#(access_filter) ${path}.<column> in $${missing[0]}, ` +
                           `or carry the key in the join's on), or drop the join.`,
                     );
                  }
               }
               walk(field.fields, path, joins + 1);
            }
         };
         walk(entry.fields ?? [], "", 0);
         if (cut) truncated.push(where);
      }
   }
   // Past a bound a gate may sit unread; with no gate anywhere there is nothing to bypass.
   if (truncated.length > 0 && packageHasGate(models)) {
      for (const where of truncated) {
         messages.push(
            `${where} joins more than ${MAX_JOIN_RECURSION_DEPTH} deep or ` +
               `past ${MAX_JOIN_PATHS} join paths, beyond which Publisher ` +
               `cannot read the gates on what it joins, and this package ` +
               `gates sources. Drop the joins past that, or split the source.`,
         );
      }
   }
   return messages;
}
