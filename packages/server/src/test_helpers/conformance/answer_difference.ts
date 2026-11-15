// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a stored answer differs from live: the comparison the v1 cutover's
 * conformance runner (`v1_cutover_runner.ts`) asks of each served answer.
 * It began as the cutover's shadow comparison, which retired with the switch
 * (6g).
 *
 * Rows are compared as a multiset, each value within a float tolerance (two
 * plans may order ties differently and sum floats in another order). Result
 * metadata is compared field by field: name, type and annotations, with two
 * differences every stored read has set aside, as the conformance fixture
 * tolerates them: a number's integer or decimal subtype (known limitation 12,
 * accepted for the cutover) and the `#(malloy)` drill annotations.
 *
 * A difference is described by its place (a row index and column, a field
 * and key), never by a value.
 */

const FLOAT_TOLERANCE = 1e-9;

/**
 * A value in a form that compares exactly: a bigint and a date as text, an
 * object's keys sorted. A string stays a string, so `"007"` and `"7"` differ.
 */
function normalize(value: unknown): unknown {
   if (typeof value === "bigint") return `${value}n`;
   if (value instanceof Date) return value.toISOString();
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

/**
 * Equal values. Numbers compare exactly, except when either is a non-integer
 * number: two plans may sum floats in another order, which can differ in the
 * last bits and even land one side on an integer (`1` against
 * `0.9999999999999999`), so they are equal within a relative tolerance. Two
 * integers, or a bigint, never are. A bigint equals a number only when the
 * number is that integer exactly.
 */
function valuesEqual(a: unknown, b: unknown): boolean {
   if (typeof a === "bigint" || typeof b === "bigint") {
      const exact = (v: unknown) =>
         typeof v === "bigint"
            ? v.toString()
            : typeof v === "number" && Number.isSafeInteger(v)
              ? String(v)
              : undefined;
      const x = exact(a);
      return x !== undefined && x === exact(b);
   }
   if (typeof a === "number" && typeof b === "number") {
      if (a === b) return true;
      if (Number.isInteger(a) && Number.isInteger(b)) return false;
      const scale = Math.max(Math.abs(a), Math.abs(b), 1);
      return Math.abs(a - b) <= FLOAT_TOLERANCE * scale;
   }
   return JSON.stringify(normalize(a)) === JSON.stringify(normalize(b));
}

/**
 * A row value's sort key: numbers and bigints as one kind (an integer as its
 * exact text, any other number to 12 significant digits, so float noise in the
 * last bits does not reorder rows {@link valuesEqual} calls equal), so the same
 * row sorts alike on both sides.
 */
function sortable(value: unknown): unknown {
   if (typeof value === "bigint") return value.toString();
   if (typeof value === "number") {
      return Number.isInteger(value)
         ? String(value)
         : String(Number(value.toPrecision(12)));
   }
   return normalize(value);
}

/** Where two row sets differ (ignoring order), or undefined when they match. */
export function strictRowDifference(
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
      JSON.stringify(columns.map((c) => sortable(r[c])));
   const x = [...a].sort((p, q) => key(p).localeCompare(key(q)));
   const y = [...b].sort((p, q) => key(p).localeCompare(key(q)));
   for (let i = 0; i < x.length; i++) {
      for (const c of columns) {
         if (!valuesEqual(x[i][c], y[i][c])) return `row ${i} column ${c}`;
      }
   }
   return undefined;
}

/**
 * A type with every number's `subtype` set aside, nested fields included, and
 * a nested field's annotations without the reference ids minted per compile.
 */
function withoutSubtype(type: unknown, depth = 0): unknown {
   if (typeof type === "string") {
      return type.replace(/reference_id = "[^"]*"/g, "");
   }
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
export function strictFieldDifference(
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
