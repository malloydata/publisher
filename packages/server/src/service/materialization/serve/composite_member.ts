// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A composite source (`compose(a, b)`) resolves a query to the first member
 * that can answer it, and that member's own `where:` and fields are part of
 * the answer. The served copy compiles the author's text over re-rooted
 * tables, where a stored column may be redeclared (a big integer read
 * natively), and Malloy may then resolve the same query to another member: a
 * tenant filter on the first member dropped, every tenant's rows returned. A
 * served answer is kept only when every composite it reaches, the query's own
 * source, a member nested in one, or a source reached through a join,
 * resolves to the member, by position, that live resolves it to.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import { LegDecline } from "../domain/leg_decline";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";

interface StructLike {
   type?: unknown;
   sources?: unknown;
   partitionComposite?: unknown;
   fields?: unknown;
   filterList?: unknown;
   join?: unknown;
   name?: unknown;
   as?: unknown;
}

/** An IR value as text, its source locations left out. */
function canon(x: unknown): string {
   return JSON.stringify(x ?? null, (k, v: unknown) =>
      k === "location" || k === "at" || k === "drillExpression" ? undefined : v,
   );
}

/** Whether `x` holds a composite anywhere in it. */
function holdsComposite(x: unknown, depth = 0): boolean {
   if (depth > 200 || x === null || typeof x !== "object") return false;
   if (Array.isArray(x)) return x.some((v) => holdsComposite(v, depth + 1));
   const s = x as StructLike;
   if (s.type === "composite" || s.partitionComposite !== undefined) {
      return true;
   }
   return Object.values(x).some((v) => holdsComposite(v, depth + 1));
}

function activeName(f: StructLike): string {
   return String(f.as ?? f.name);
}

function fieldsOf(s: StructLike): StructLike[] {
   return Array.isArray(s.fields) ? (s.fields as StructLike[]) : [];
}

function filtersOf(s: StructLike): unknown[] {
   return Array.isArray(s.filterList) ? s.filterList : [];
}

/**
 * Whether `resolved` is `member` as Malloy resolves a composite to it: the
 * member with the composite's own fields and filters before its own. Its own
 * fields, definitions included, and its own filters are the suffixes of the
 * resolved source's, so two members with the same filters and different
 * fields are told apart. A joined field that itself holds a composite is
 * compared by name, since its resolution is placed on its own.
 */
function isResolvedMember(member: StructLike, resolved: StructLike): boolean {
   const identity = (s: StructLike) => {
      const {
         fields: _f,
         filterList: _w,
         annotations: _a,
         arguments: _g,
         // How a joined member is bound to its parent, not what it is.
         as: _as,
         join: _j,
         onExpression: _on,
         matrixOperation: _m,
         ...rest
      } = s as StructLike & {
         annotations?: unknown;
         arguments?: unknown;
         onExpression?: unknown;
         matrixOperation?: unknown;
      };
      return canon(rest);
   };
   if (identity(member) !== identity(resolved)) return false;
   const own = fieldsOf(member);
   const all = fieldsOf(resolved);
   if (all.length < own.length) return false;
   const tail = all.slice(all.length - own.length);
   for (let i = 0; i < own.length; i++) {
      const sameField = holdsComposite(own[i])
         ? activeName(own[i]) === activeName(tail[i])
         : canon(own[i]) === canon(tail[i]);
      if (!sameField) return false;
   }
   const ownWhere = filtersOf(member);
   const allWhere = filtersOf(resolved);
   if (allWhere.length < ownWhere.length) return false;
   return (
      canon(ownWhere) ===
      canon(allWhere.slice(allWhere.length - ownWhere.length))
   );
}

/**
 * Append, to `out`, the member each composite in `source` resolved to in
 * `resolved` (`<path>:<index>`), and return whether every one was placed.
 */
function placeMembers(
   source: StructLike,
   resolved: StructLike,
   path: string,
   out: string[],
): boolean {
   if (source.partitionComposite !== undefined) return false;
   if (source.type === "composite") {
      // A composite's own joins (`compose(a, b) extend { join_one: … }`)
      // are not followed here.
      if (
         fieldsOf(source).some((f) => f.join !== undefined && holdsComposite(f))
      ) {
         return false;
      }
      const members = Array.isArray(source.sources)
         ? (source.sources as StructLike[])
         : [];
      for (let i = 0; i < members.length; i++) {
         const m = members[i];
         if (m.type === "composite" || m.partitionComposite !== undefined) {
            const inner: string[] = [];
            if (placeMembers(m, resolved, `${path}/${i}`, inner)) {
               out.push(`${path}:${i}`, ...inner);
               return true;
            }
            continue;
         }
         if (isResolvedMember(m, resolved)) {
            out.push(`${path}:${i}`);
            return placeJoins(m, resolved, path, out);
         }
      }
      return false;
   }
   return placeJoins(source, resolved, path, out);
}

/** {@link placeMembers} for each joined source of `source` that holds a composite. */
function placeJoins(
   source: StructLike,
   resolved: StructLike,
   path: string,
   out: string[],
): boolean {
   const byName = new Map(fieldsOf(resolved).map((f) => [activeName(f), f]));
   for (const f of fieldsOf(source)) {
      if (!holdsComposite(f)) continue;
      // Only a join holds a source; anything else holding a composite is a
      // shape this check does not follow.
      if (f.join === undefined) return false;
      const r = byName.get(activeName(f));
      if (r === undefined) return false;
      if (!placeMembers(f, r, `${path}.${activeName(f)}`, out)) return false;
   }
   return true;
}

/**
 * The member each composite a compiled query reaches resolved to, in a fixed
 * order: `[]` when it reaches none, undefined when one cannot be placed.
 */
async function membersResolved(
   runnable: QueryMaterializer,
): Promise<string[] | undefined> {
   const view = await readPreparedQuery(runnable);
   if (view.modelDef === undefined) return undefined;
   const source = resolveStructRefSource<StructLike>(
      view.structRef as never,
      view.modelDef as never,
   );
   if (source === undefined) return undefined;
   if (!holdsComposite(source) && !holdsComposite(view.pipeline)) return [];
   // A composite the query's own stages reach (a query-level `extend`, a
   // nest's join) is not placed here.
   if (holdsComposite(view.pipeline)) return undefined;
   const resolved = view.compositeResolvedSourceDef as StructLike | undefined;
   if (resolved === undefined || resolved === null) return undefined;
   const out: string[] = [];
   return placeMembers(source, resolved, "", out) ? out : undefined;
}

/**
 * Throw a decline unless `served` resolves every composite it reaches to the
 * member `author` (the live compile) resolves it to. Fails closed: a compile
 * whose resolution cannot be read, or a composite that cannot be placed,
 * declines, and live answers.
 */
export async function assertSameCompositeMember(
   author: QueryMaterializer,
   served: QueryMaterializer,
): Promise<void> {
   let same: boolean;
   try {
      const live = await membersResolved(author);
      if (live !== undefined && live.length === 0) return;
      const routed = await membersResolved(served);
      same =
         live !== undefined &&
         routed !== undefined &&
         live.join("\u0000") === routed.join("\u0000");
   } catch {
      same = false;
   }
   if (!same) {
      throw new LegDecline(
         "served_copy_not_served",
         "the query's composite source resolves to another member over the stored tables than live",
      );
   }
}
