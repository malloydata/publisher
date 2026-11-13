// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's shadow comparison (`PUBLISHER_V1_SERVED_COPY=shadow`,
 * materialization v2 phase 3 pull request 6): a query a retiring v1 path
 * answered (the storage serve shape, the `#@ preaggregate` composite, or
 * Malloy's build manifest on the live compile) is answered again as the
 * cutover would, and the two are compared here.
 *
 * Rows are compared as a multiset, each value within a float tolerance (two
 * plans may order ties differently and sum floats in another order). Result
 * metadata is compared field by field: name, type and annotations, with two
 * differences every stored read has set aside, as the conformance fixture
 * tolerates them: a number's integer or decimal subtype (known limitation 12,
 * accepted for the cutover) and the `#(malloy)` drill annotations.
 *
 * A difference is described by its place (a row index and column, a field
 * and key), never by a value, since the description is logged.
 */

/** How many query texts a model remembers having compared. */
export const SHADOW_SEEN_LIMIT = 1000;

/** One side of a comparison. */
export interface ShadowSide {
   /** The rows, as `compactResult` carries them. */
   rows: unknown;
   /** The result schema's fields (`{ name, type, annotations }`). */
   fields: readonly unknown[];
}

export type ShadowOutcome =
   | "equal"
   | "live"
   | "rows_differ"
   | "metadata_differ";

const FLOAT_TOLERANCE = 1e-9;

/**
 * The outcome of comparing today's answer with the cutover's. `live` when the
 * cutover answered live (no stored table), whatever the rows.
 */
export function shadowOutcome(
   today: ShadowSide,
   cutover: ShadowSide & { servedBy: string | undefined },
): { outcome: ShadowOutcome; detail?: string } {
   if (cutover.servedBy === undefined || cutover.servedBy === "live") {
      return { outcome: "live" };
   }
   const rows = shadowRowDifference(asRows(today.rows), asRows(cutover.rows));
   if (rows) return { outcome: "rows_differ", detail: rows };
   const fields = shadowFieldDifference(today.fields, cutover.fields);
   if (fields) return { outcome: "metadata_differ", detail: fields };
   return { outcome: "equal" };
}

function asRows(rows: unknown): Record<string, unknown>[] {
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

function normalize(value: unknown): unknown {
   if (typeof value === "bigint") return Number(value);
   if (value instanceof Date) return value.toISOString();
   if (typeof value === "string" && /^-?\d+$/.test(value) && value.length < 16)
      return Number(value);
   if (Array.isArray(value)) return value.map(normalize);
   if (value !== null && typeof value === "object") {
      return Object.fromEntries(
         Object.keys(value)
            .sort()
            .map((k) => [k, normalize((value as Record<string, unknown>)[k])]),
      );
   }
   return value;
}

function valuesEqual(a: unknown, b: unknown): boolean {
   const x = normalize(a);
   const y = normalize(b);
   if (typeof x === "number" && typeof y === "number") {
      if (x === y) return true;
      const scale = Math.max(Math.abs(x), Math.abs(y), 1);
      return Math.abs(x - y) <= FLOAT_TOLERANCE * scale;
   }
   return JSON.stringify(x) === JSON.stringify(y);
}

/** Where two row sets differ (ignoring order), or undefined when they match. */
export function shadowRowDifference(
   a: readonly Record<string, unknown>[],
   b: readonly Record<string, unknown>[],
): string | undefined {
   const columns = Object.keys(a[0] ?? b[0] ?? {}).sort();
   const other = Object.keys(b[0] ?? a[0] ?? {}).sort();
   if (JSON.stringify(columns) !== JSON.stringify(other)) {
      return `columns differ: ${columns.join(",")} against ${other.join(",")}`;
   }
   if (a.length !== b.length) {
      return `row counts differ: ${a.length} against ${b.length}`;
   }
   const key = (r: Record<string, unknown>) =>
      JSON.stringify(columns.map((c) => normalize(r[c])));
   const x = [...a].sort((p, q) => key(p).localeCompare(key(q)));
   const y = [...b].sort((p, q) => key(p).localeCompare(key(q)));
   for (let i = 0; i < x.length; i++) {
      for (const c of columns) {
         if (!valuesEqual(x[i][c], y[i][c])) return `row ${i} column ${c}`;
      }
   }
   return undefined;
}

/** A type with every number's `subtype` set aside, nested fields included. */
function withoutSubtype(type: unknown, depth = 0): unknown {
   if (depth > 32 || type === null || typeof type !== "object") return type;
   if (Array.isArray(type))
      return type.map((t) => withoutSubtype(t, depth + 1));
   return Object.fromEntries(
      Object.entries(type as Record<string, unknown>)
         .filter(([k]) => k !== "subtype")
         .map(([k, v]) => [k, withoutSubtype(v, depth + 1)]),
   );
}

function notesOf(field: unknown): string[] {
   const annotations =
      (field as { annotations?: { value?: unknown }[] })?.annotations ?? [];
   return annotations
      .map((a) =>
         String(a?.value ?? "")
            .trim()
            // A reference id is minted per compile.
            .replace(/reference_id = "[^"]*"/g, ""),
      )
      .filter((n) => !n.startsWith("#(malloy)"));
}

/** Where two result schemas differ, or undefined when they match. */
export function shadowFieldDifference(
   a: readonly unknown[],
   b: readonly unknown[],
): string | undefined {
   const name = (f: unknown) => String((f as { name?: unknown })?.name);
   if (a.length !== b.length) {
      return `fields ${a.map(name).join(",")} against ${b.map(name).join(",")}`;
   }
   for (let i = 0; i < a.length; i++) {
      if (name(a[i]) !== name(b[i])) return `field ${i} name`;
      const ta = withoutSubtype((a[i] as { type?: unknown })?.type);
      const tb = withoutSubtype((b[i] as { type?: unknown })?.type);
      if (JSON.stringify(ta) !== JSON.stringify(tb)) {
         return `${name(a[i])}.type`;
      }
      if (JSON.stringify(notesOf(a[i])) !== JSON.stringify(notesOf(b[i]))) {
         return `${name(a[i])}.annotations`;
      }
   }
   return undefined;
}
