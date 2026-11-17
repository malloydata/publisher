// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What an entry point's row-level gates read from a stored table, for the
 * checks that decide whether a routed answer filters the stored rows as live
 * filters the source's: a query's own time zone (a gate over a stored
 * truncation, `serve/served_copy_serve.ts`, `serve/rollup_variants.ts`) and a
 * gate column's stored type (`domain/gate_policy.ts`).
 *
 * The gate is grafted on the routed compile after the leg answers, so a check
 * that reads only the caller's query never sees the fields the gate reads.
 * Every check that judges stored values against live ones must read these too.
 */

import { collectEntryPointGates } from "../../gate_classification";
import { malloyIdentifiers } from "../../query_text";
import type { ModelDef, SourceDef } from "@malloydata/malloy";

/** Every field path an expression reads (`node: "field"`), at any depth. */
export function fieldPathsIn(
   e: unknown,
   out: string[][] = [],
   depth = 0,
): string[][] {
   if (depth > 64 || e === null || typeof e !== "object") return out;
   if (Array.isArray(e)) {
      for (const x of e) fieldPathsIn(x, out, depth + 1);
      return out;
   }
   const n = e as { node?: unknown; path?: unknown };
   if (
      n.node === "field" &&
      Array.isArray(n.path) &&
      n.path.every((p) => typeof p === "string")
   ) {
      out.push(n.path as string[]);
   }
   for (const v of Object.values(e)) fieldPathsIn(v, out, depth + 1);
   return out;
}

/** The names a gate's text reads: each identifier it names, fields or not. */
export function gateIdentifiers(exprs: readonly string[]): string[] {
   return [...new Set(exprs.flatMap((expr) => malloyIdentifiers(expr)))];
}

/**
 * Every name the gates (`exprs`) read from `struct`, and every field each of
 * those is computed from, through its expression, transitively: a gate on
 * `d_month is d.month` reads `d` too, and a rename reads the field it
 * renames. Over-collects on purpose: each identifier the gate text names is
 * kept whether or not it is a field (a given's name, a joined field's last
 * name), since a reader only ever declines on a match, so an extra name can
 * send an answer live and never serve a wrong one.
 */
export function gateReadNames(
   struct: unknown,
   exprs: readonly string[],
): Set<string> {
   const fields = new Map<string, { name?: unknown; e?: unknown }>();
   for (const f of ((struct as { fields?: unknown[] } | undefined)?.fields ??
      []) as { name?: unknown; as?: unknown; e?: unknown }[]) {
      fields.set(String(f.as ?? f.name), f);
   }
   const out = new Set<string>();
   const visit = (name: string, depth: number): void => {
      if (out.has(name) || depth > 64) return;
      out.add(name);
      const f = fields.get(name);
      if (!f) return;
      if (typeof f.name === "string" && f.name !== name) {
         visit(f.name, depth + 1);
      }
      for (const p of fieldPathsIn(f.e)) {
         for (const segment of p) visit(segment, depth + 1);
      }
   };
   for (const id of gateIdentifiers(exprs)) visit(id, 0);
   return out;
}

/**
 * The gate texts that apply to `struct` as an entry point, both routes (a
 * lock reads only givens and literals, so its names never match a column),
 * or undefined when the walk cannot read them: a reader fails closed on that.
 */
export function entryPointGateExprs(
   struct: unknown,
   modelDef: unknown,
): string[] | undefined {
   if (struct === undefined || modelDef === undefined) return undefined;
   try {
      const entries = collectEntryPointGates(
         struct as SourceDef,
         modelDef as ModelDef,
         new Set(),
         true,
      );
      // The walk's fail-closed deny (no entry point to graft on).
      if (entries.some((e) => e.struct === undefined)) return undefined;
      return entries.flatMap((e) => [...e.exprs]);
   } catch {
      return undefined;
   }
}
