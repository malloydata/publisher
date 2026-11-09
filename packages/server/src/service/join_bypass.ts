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
 * did not. A bypass is always a `join-bypass` warning, never a load refusal
 * (see `Package.joinBypassWarnings`); a compliance review is what fails one.
 *
 * Three tests, all mechanical, over every join a published source declares
 * ({@link MAX_JOIN_RECURSION_DEPTH} deep, `->` pipeline joins included),
 * against the entry source's own gates:
 *
 * - the joined struct is locked outright (`#(authorize) false`): an entry
 *   that can be entered is never as locked as that, so this refuses from an
 *   ungated entry (a gated entry over a locked base is the curated-extension
 *   pattern);
 * - the joined struct's `#(authorize)` lock has a term the entry's lock
 *   lacks, compared as parsed (given, operator, decoded literal), never as
 *   text;
 * - the joined struct's row filters read a given the entry's gates do not
 *   read: the joined rows are scoped by something the entry never asks for.
 *   Row-level terms are compared by given name only, so a filter on the same
 *   given over a column the join does not tie still passes.
 *
 * A published source locked and filtered the same way as what it joins passes,
 * since that is the designed pattern. Only published, reachable entry sources
 * are read: a hidden source is not a place a caller can enter.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";
import { referencedGivenNames } from "./authorize";
import type { AnnotationsDef } from "./annotations";
import { splitSourceId } from "./source_id";
import {
   parseAuthorizeGrammarBody,
   type AuthorizeGrammarParsedTerm,
} from "./authorize_grammar";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { isDeniedToAll, isDenyAll } from "./classification";
import { packageSources } from "./classification_report";
import {
   MAX_JOIN_PATHS,
   MAX_JOIN_RECURSION_DEPTH,
   stageJoins,
} from "./filter_binding_guard";
import { gateExprsForRoute } from "./gate_registry_walk";
import {
   resolveCompositeResolvedBase,
   resolveQuerySourceBase,
} from "./governance/ir/source_graph";
import type { Model } from "./model";

/** The warning code a join bypass carries. */
export const JOIN_BYPASS = "join-bypass";

const CANNOT_READ_PAST =
   "beyond which Publisher cannot read the gates on what it joins";

/** Whether a finding is a walk cut short at the bound, which exposes no named rows. */
export function isTruncatedWalkFinding(message: string): boolean {
   return message.includes(CANNOT_READ_PAST);
}

/** The slice of a compiled join field the check reads. */
interface IrJoin {
   name?: string;
   as?: string;
   join?: string;
   type?: string;
   fields?: IrJoin[];
   annotations?: AnnotationsDef;
   sourceID?: string;
   extends?: string;
   query?: { pipeline?: Parameters<typeof stageJoins>[0] };
}

/** A join's path, with the source it joins when that differs from the join's own name. */
function joinedLabel(field: IrJoin, path: string): string {
   // A join to `src extend { … }` has no sourceID of its own; `extends` names src.
   const source = splitSourceId(field.sourceID ?? field.extends)?.source;
   const name = field.as ?? field.name;
   return source && source !== name ? `${path} (source ${source})` : path;
}

const isAdmitAll = (expr: string) => expr.trim().toLowerCase() === "true";

/** A joined struct's gates by route, flattened across declaring sources. */
interface JoinGates {
   authorize: string[];
   accessFilter: string[];
}

/**
 * Every gate expression in force on a joined struct, per route. A join
 * carries an inlined copy of its source, not a registered one, so this reads
 * the copy. A gate that cannot be read is treated as a lock, since the query
 * route would deny on it.
 */
function gatesOn(struct: IrJoin, modelDef: ModelDef | undefined): JoinGates {
   const known = gatesMemo.get(struct);
   if (known) return known;
   const gates = readGatesOn(struct, modelDef);
   gatesMemo.set(struct, gates);
   return gates;
}

/** A joined struct's gates, read once: the registry walk behind them is the cost. */
const gatesMemo = new WeakMap<IrJoin, JoinGates>();

function readGatesOn(
   struct: IrJoin,
   modelDef: ModelDef | undefined,
): JoinGates {
   try {
      const read = (route: string) =>
         Array.from(
            new Set(
               gateExprsForRoute(
                  struct as unknown as SourceDef,
                  modelDef,
                  route,
               ).flat(),
            ),
         );
      return {
         authorize: read(AUTHORIZE_ROUTE),
         accessFilter: read(ACCESS_FILTER_ROUTE),
      };
   } catch {
      return { authorize: ["false"], accessFilter: [] };
   }
}

function givensOf(exprs: readonly string[]): Set<string> {
   const out = new Set<string>();
   for (const expr of exprs) {
      if (isDenyAll(expr) || isAdmitAll(expr)) continue;
      for (const name of referencedGivenNames(expr)) out.add(name);
   }
   return out;
}

/** One `#(authorize)` term: what a caller must hold. */
export type LockTerm = Pick<
   Extract<AuthorizeGrammarParsedTerm, { scope: "source_level" }>,
   "given" | "operator" | "literal"
>;

const termKey = (t: LockTerm) =>
   JSON.stringify([t.given, t.operator, t.literal]);

const termText = (t: LockTerm) =>
   `'${t.literal.replace(/[\\']/g, "\\$&")}' ${t.operator} $${t.given}`;

/**
 * The terms an `#(authorize)` lock demands, parsed. An expression that does
 * not parse is a lock nobody can be shown to meet, so it reads as `false`.
 */
export function lockTermsOf(exprs: readonly string[]): LockTerm[] | "false" {
   const terms: LockTerm[] = [];
   for (const expr of exprs) {
      let parsed: ReturnType<typeof parseAuthorizeGrammarBody>;
      try {
         parsed = parseAuthorizeGrammarBody(
            "join",
            expr,
            new Map(),
            AUTHORIZE_ROUTE,
         );
      } catch {
         return "false";
      }
      for (const term of parsed) {
         if (term.scope === "deny_all") return "false";
         if (term.scope === "source_level") terms.push(term);
      }
   }
   return terms;
}

/** The first joined term the entry's lock does not also demand, as text. */
export function missingLockTerm(
   joined: readonly LockTerm[],
   entry: readonly LockTerm[],
): string | undefined {
   const held = new Set(entry.map(termKey));
   const missing = joined.find((t) => !held.has(termKey(t)));
   return missing && termText(missing);
}

/**
 * Whether any source in the package carries a gate. Reads every compiled
 * top-level source, not the discovery listing: a gated source hidden from
 * discovery is exactly what a deep join reaches. A model that cannot be read
 * counts as gated.
 */
function packageHasGate(models: ReadonlyMap<string, Model>): boolean {
   for (const model of models.values()) {
      let modelDef: ModelDef | undefined;
      try {
         modelDef = model.getModelDef();
      } catch {
         return true;
      }
      if (!modelDef) return true;
      for (const source of Object.values(modelDef.contents)) {
         if (!isSourceDef(source)) continue;
         try {
            for (const route of [AUTHORIZE_ROUTE, ACCESS_FILTER_ROUTE]) {
               const gated = gateExprsForRoute(source, modelDef, route)
                  .flat()
                  .some((expr) => !isAdmitAll(expr));
               if (gated) return true;
            }
         } catch {
            return true;
         }
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
   includeHidden = false,
): string[] {
   const messages: string[] = [];
   const truncated: string[] = [];
   for (const source of packageSources(models, isListed, includeHidden)) {
      const { modelPath, sourceInfo, apiSource, modelDef } = source;
      const entry = source.ir as IrJoin;
      if (apiSource && isDeniedToAll(apiSource)) continue;
      const entryGates = [
         ...(apiSource?.authorize ?? []),
         ...(apiSource?.accessFilter ?? []),
      ].filter((expr) => !isAdmitAll(expr));
      const entryGivens = givensOf(entryGates);
      const entryLock = lockTermsOf(apiSource?.authorize ?? []);
      const entryTerms = entryLock === "false" ? [] : entryLock;
      const where = `source ${sourceInfo.name} (${modelPath})`;

      let joinPaths = 0;
      let cut = false;
      // A `->` source's base is a source the entry derives from: its joins are the entry's.
      const joinsOf = (struct: IrJoin, hops = 0): IrJoin[] => {
         const joins = (struct.fields ?? []).filter(
            (f) => typeof f.join === "string" && Array.isArray(f.fields),
         );
         if (struct.type !== "query_source") return joins;
         // The compiler reads a composite's resolved member before structRef.
         const base = (resolveCompositeResolvedBase(
            struct as unknown as SourceDef,
         ) ??
            resolveQuerySourceBase(
               struct as unknown as SourceDef,
               modelDef,
            )) as IrJoin | undefined;
         if (hops >= MAX_JOIN_RECURSION_DEPTH) {
            cut = true;
            return joins;
         }
         try {
            return [
               ...joins,
               ...(stageJoins(struct.query?.pipeline) as IrJoin[]),
               ...(base ? joinsOf(base, hops + 1) : []),
            ];
         } catch {
            cut = true;
            return joins;
         }
      };
      const walk = (struct: IrJoin, prefix: string, joins: number) => {
         for (const field of joinsOf(struct)) {
            if (
               joins >= MAX_JOIN_RECURSION_DEPTH ||
               ++joinPaths > MAX_JOIN_PATHS
            ) {
               cut = true;
               continue;
            }
            const name = field.as ?? field.name ?? "?";
            const path = prefix ? `${prefix}.${name}` : name;
            const joined = joinedLabel(field, path);
            const gates = gatesOn(field, modelDef);
            const lock = lockTermsOf(gates.authorize);
            if (lock === "false") {
               // A gated entry over a locked base is the curated-extension pattern.
               if (entryGates.length === 0) {
                  messages.push(
                     `${where} is open to every caller and joins ${joined}, ` +
                        `which is locked (#(authorize) false). A gate ` +
                        `fires only on the source a query enters through, ` +
                        `so the lock does not fire through this join and ` +
                        `every caller reads ${path}'s rows. Gate ` +
                        `${sourceInfo.name} with #(authorize) or ` +
                        `#(access_filter), or drop the join.`,
                  );
               }
            } else {
               const missingTerm = missingLockTerm(lock, entryTerms);
               if (missingTerm !== undefined) {
                  messages.push(
                     `${where} joins ${joined}, which is locked to ` +
                        `${missingTerm}, a term no #(authorize) on ` +
                        `${sourceInfo.name} requires. A gate fires only ` +
                        `on the source a query enters through, so every ` +
                        `caller ${sourceInfo.name} admits reads ${path}'s ` +
                        `rows without holding it. The grammar has no OR: ` +
                        `to serve a broader role, join an extension of ` +
                        `the source joined as ${path} that declares ` +
                        `${sourceInfo.name}'s own lock, or add ` +
                        `${missingTerm} to ${sourceInfo.name}'s ` +
                        `#(authorize), or drop the join.`,
                  );
               }
               const missing = Array.from(givensOf(gates.accessFilter)).filter(
                  (g) => !entryGivens.has(g),
               );
               if (missing.length > 0) {
                  messages.push(
                     `${where} joins ${joined}, whose gate reads ` +
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
            walk(field, path, joins + 1);
         }
      };
      walk(entry, "", 0);
      if (cut) truncated.push(where);
   }
   // Past a bound a gate may sit unread; with no gate anywhere there is nothing to bypass.
   if (truncated.length > 0 && packageHasGate(models)) {
      for (const where of truncated) {
         messages.push(
            `${where} joins more than ${MAX_JOIN_RECURSION_DEPTH} deep or ` +
               `past ${MAX_JOIN_PATHS} join paths, ${CANNOT_READ_PAST}, and this package ` +
               `gates sources. Drop the joins past that, or split the source.`,
         );
      }
   }
   return messages;
}
