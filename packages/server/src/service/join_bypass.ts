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
 * {@link MAX_JOIN_DEPTH} deep, against the entry source's own gates:
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
import { referencedGivenNames } from "./authorize";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { findIrSource, isDeniedToAll, MAX_JOIN_DEPTH } from "./classification";
import { effectiveAncestorGateExprs } from "./gate_registry_walk";
import type { Model } from "./model";

/** The slice of a compiled join field the check reads. */
interface IrJoin {
   name?: string;
   as?: string;
   join?: string;
   fields?: IrJoin[];
}

const isDenyAll = (expr: string) => expr.trim().toLowerCase() === "false";
const isAdmitAll = (expr: string) => expr.trim().toLowerCase() === "true";

/** Every gate expression in force on a struct, both routes, flattened. */
function gatesOn(struct: IrJoin, modelDef: ModelDef | undefined): string[] {
   const exprs: string[] = [];
   for (const route of [AUTHORIZE_ROUTE, ACCESS_FILTER_ROUTE]) {
      try {
         for (const group of effectiveAncestorGateExprs(
            struct as unknown as SourceDef,
            modelDef,
            route,
         )) {
            exprs.push(...group);
         }
      } catch {
         // A gate the walk cannot read is a gate the query route would deny
         // on; the join still passes it through. Fail closed: treat as locked.
         exprs.push("false");
      }
   }
   return exprs;
}

function givensOf(exprs: readonly string[]): Set<string> {
   const out = new Set<string>();
   for (const expr of exprs) {
      if (isDenyAll(expr) || isAdmitAll(expr)) continue;
      for (const name of referencedGivenNames(expr)) out.add(name);
   }
   return out;
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

         const walk = (fields: IrJoin[], prefix: string, depth: number) => {
            for (const field of fields) {
               if (
                  typeof field.join !== "string" ||
                  !Array.isArray(field.fields)
               )
                  continue;
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
               if (depth < MAX_JOIN_DEPTH) {
                  walk(field.fields, path, depth + 1);
               }
            }
         };
         walk(entry.fields ?? [], "", 1);
      }
   }
   return messages;
}
