// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a stored answer differs from live, the one comparison module of the
 * package-case runner (`package_runner.ts`) and its three reporters:
 *
 * - {@link rowDifference}, R4's comparison: rows paired as a multiset, each
 *   value within a float tolerance, and held to the ordering their query
 *   states for the top level and every nest ({@link withOrdering}), so only
 *   rows that tie on every sort key may come back in either order. The R4
 *   harness, the conformance fixture and the golden capture read it.
 * - {@link strictRowDifference} and {@link strictFieldDifference}, the v1
 *   cutover's conformance runner's (`v1_cutover_runner.ts`): rows as a
 *   multiset, strings exactly, and result metadata field by field (name,
 *   type and annotations), with two differences every stored read has set
 *   aside, as the conformance fixture tolerates them: a number's integer or
 *   decimal subtype (known limitation 12, accepted for the cutover) and the
 *   `#(malloy)` drill annotations. A difference is described by its place (a
 *   row index and column, a field and key), never by a value.
 */

/** Relative tolerance for floats, as R4 states it. */
const FLOAT_TOLERANCE = 1e-9;

/** A result row, as `compactResult` carries it. */
export type Row = Record<string, unknown>;

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

/**
 * How a query orders its rows, and each of its nests' rows, as its compiled
 * result shape states it. Malloy orders every nest: by its `order_by`, else by
 * its default (the first aggregate descending, and so on), so the order of a
 * nest is part of its answer, the same as its values.
 */
export interface RowOrdering {
   /** The output fields the rows are sorted by, in order; empty when unordered. */
   orderBy: { field: string; dir: "asc" | "desc" }[];
   /** Each nested result's own ordering, by its output name. */
   nests: Record<string, RowOrdering>;
}

/** The ordering each run's rows were answered under, by the rows array. */
const orderings = new WeakMap<Row[], RowOrdering>();

/** Record the ordering `rows` were answered under, for {@link rowDifference}. */
export function withOrdering(rows: Row[], ordering?: RowOrdering): Row[] {
   if (ordering) orderings.set(rows, ordering);
   return rows;
}

export interface ResultStructShape {
   fields?: ResultFieldShape[];
   resultMetadata?: {
      orderBy?: { field: string | number; dir?: "asc" | "desc" }[];
   };
}
interface ResultFieldShape extends ResultStructShape {
   name: string;
   as?: string;
}

/** A compiled result struct's ordering, and its nests', from Malloy's result metadata. */
export function orderingOfStruct(struct: ResultStructShape): RowOrdering {
   const fields = struct.fields ?? [];
   const orderBy = (struct.resultMetadata?.orderBy ?? []).flatMap((o) => {
      const field =
         typeof o.field === "number"
            ? (fields[o.field - 1]?.as ?? fields[o.field - 1]?.name)
            : o.field;
      return field ? [{ field, dir: o.dir ?? "asc" }] : [];
   });
   const nests: Record<string, RowOrdering> = {};
   for (const f of fields) {
      if (Array.isArray(f.fields)) nests[f.as ?? f.name] = orderingOfStruct(f);
   }
   return { orderBy, nests };
}

/** A value in the form two runs are compared in. */
function normalizeValue(value: unknown): unknown {
   if (typeof value === "bigint") {
      // Past 2^53 a Number would round two different integers together.
      return value >= Number.MIN_SAFE_INTEGER &&
         value <= Number.MAX_SAFE_INTEGER
         ? Number(value)
         : value.toString();
   }
   if (value instanceof Date) return value.toISOString();
   if (
      typeof value === "string" &&
      /^-?\d+$/.test(value) &&
      value.length < 16
   ) {
      // DuckDB returns some integer aggregates as strings over raw SQL.
      return Number(value);
   }
   if (Array.isArray(value)) return value.map(normalizeValue);
   if (isRecord(value)) {
      return Object.fromEntries(
         Object.entries(value).map(([k, v]) => [k, normalizeValue(v)]),
      );
   }
   return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
   return (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value) &&
      !(value instanceof Date)
   );
}

/** JSON with object keys sorted, so key order never decides a row's place. */
function canonicalJson(value: unknown): string {
   return JSON.stringify(value, (_key, v: unknown) =>
      isRecord(v)
         ? Object.fromEntries(
              Object.keys(v)
                 .sort()
                 .map((k) => [k, v[k]]),
           )
         : v,
   );
}

/**
 * A row's sort key for pairing it with its counterpart: its normalized value
 * with nested rows in a canonical order, so two rows that differ only in how
 * tied nested rows came back still pair. Only the pairing uses it; the nested
 * rows' order is judged separately, against the nest's own ordering.
 */
function pairingKey(value: unknown): string {
   const canonical = (v: unknown): unknown => {
      if (Array.isArray(v)) {
         const items = v.map(canonical);
         return items.every(isRecord)
            ? items
                 .map((item) => ({ item, key: canonicalJson(item) }))
                 .sort((p, q) => p.key.localeCompare(q.key))
                 .map(({ item }) => item)
            : items;
      }
      if (isRecord(v)) {
         return Object.fromEntries(
            Object.entries(v).map(([k, x]) => [k, canonical(x)]),
         );
      }
      return v;
   };
   return canonicalJson(canonical(normalizeValue(value)));
}

/**
 * Integers (counts, sums of integers) must match exactly; other numbers
 * within the relative float tolerance, floored at 1 so two values near zero
 * are compared absolutely. NaN matches NaN: DuckDB divides 0.0 by 0 as NaN,
 * which a ratio over a childless parent's sum and count is.
 */
function numbersEqual(x: number, y: number): boolean {
   if (x === y || (Number.isNaN(x) && Number.isNaN(y))) return true;
   if (Number.isSafeInteger(x) && Number.isSafeInteger(y)) return false;
   const scale = Math.max(Math.abs(x), Math.abs(y), 1);
   return Math.abs(x - y) <= FLOAT_TOLERANCE * scale;
}

/** Two normalized scalars (or nests with no known ordering, in order) equal. */
function normalizedEqual(x: unknown, y: unknown): boolean {
   if (typeof x === "number" && typeof y === "number")
      return numbersEqual(x, y);
   if (Array.isArray(x) && Array.isArray(y)) {
      return (
         x.length === y.length && x.every((v, i) => normalizedEqual(v, y[i]))
      );
   }
   if (isRecord(x) && isRecord(y)) {
      const keys = Object.keys(x).sort();
      return (
         JSON.stringify(keys) === JSON.stringify(Object.keys(y).sort()) &&
         keys.every((k) => normalizedEqual(x[k], y[k]))
      );
   }
   return canonicalJson(x) === canonicalJson(y);
}

/**
 * The difference between two sets of rows, or undefined when they match.
 *
 * With an ordering that sorts by some fields, the rows must come back in that
 * order: row i's sort-key values must equal row i's on the other side, so the
 * only freedom left is among rows that tie on every sort key, whose order the
 * engine decides. Unordered (no ordering, at the top level, or a nest whose
 * ordering is empty), the rows are compared as a set. A nest whose ordering is
 * not known at all is compared in order, the strict reading.
 */
function rowsDifference(
   expected: unknown[],
   actual: unknown[],
   ordering: RowOrdering | undefined,
   where: string,
): string | undefined {
   if (expected.length !== actual.length) {
      return `${where}row counts differ: ${expected.length} against ${actual.length}`;
   }
   const records = expected.every(isRecord) && actual.every(isRecord);
   if (!records) {
      return normalizedEqual(normalizeValue(expected), normalizeValue(actual))
         ? undefined
         : `${where}${JSON.stringify(normalizeValue(expected))} against ${JSON.stringify(normalizeValue(actual))}`;
   }
   const sortedBy = (ordering?.orderBy ?? []).filter(
      (o) =>
         expected.every((r) => o.field in r) &&
         actual.every((r) => o.field in r),
   );
   for (let i = 0; i < expected.length && sortedBy.length > 0; i++) {
      for (const { field } of sortedBy) {
         const x = normalizeValue(expected[i][field]);
         const y = normalizeValue(actual[i][field]);
         if (!normalizedEqual(x, y)) {
            return `${where}row ${i} is out of order on ${field}: ${JSON.stringify(x)} against ${JSON.stringify(y)}`;
         }
      }
   }
   const pair = (rows: Record<string, unknown>[]) =>
      ordering === undefined && where !== ""
         ? rows
         : rows
              .map((row) => ({ row, key: pairingKey(row) }))
              .sort((p, q) => p.key.localeCompare(q.key))
              .map(({ row }) => row);
   const a = pair(expected);
   const b = pair(actual);
   for (let i = 0; i < a.length; i++) {
      const columns = Object.keys(a[i]).sort();
      const otherColumns = Object.keys(b[i]).sort();
      if (JSON.stringify(columns) !== JSON.stringify(otherColumns)) {
         return `${where}columns differ: ${columns.join(",")} against ${otherColumns.join(",")}`;
      }
      for (const c of columns) {
         const x = a[i][c];
         const y = b[i][c];
         if (Array.isArray(x) && Array.isArray(y)) {
            const inner = rowsDifference(
               x,
               y,
               ordering?.nests[c],
               `${where}row ${i} column ${c}: `,
            );
            if (inner) return inner;
         } else if (!normalizedEqual(normalizeValue(x), normalizeValue(y))) {
            return `${where}row ${i} column ${c}: ${JSON.stringify(normalizeValue(x))} against ${JSON.stringify(normalizeValue(y))}`;
         }
      }
   }
   return undefined;
}

/**
 * The difference between two row sets, or undefined when they match: same
 * columns, same row count, each value equal (integers exactly, other numbers
 * within the float tolerance), and rows in the order the query asked for, up
 * to ties on its sort keys.
 *
 * The ordering is the one recorded for the rows ({@link withOrdering}; every
 * run of a fixture records its query's). The top level is held to it only
 * when both sides are such answers: rows from raw SQL (a test's truth, a
 * stored table) come back in no order the query decides, so against them the
 * top level is compared as a set, and each nest is still held to its own
 * ordering. With no ordering on either side, the top level is a set and each
 * nest is compared in order.
 */
export function rowDifference(
   expected: Row[],
   actual: Row[],
   ordering?: RowOrdering,
): string | undefined {
   const columns = Object.keys(expected[0] ?? actual[0] ?? {}).sort();
   const actualColumns = Object.keys(actual[0] ?? expected[0] ?? {}).sort();
   if (JSON.stringify(columns) !== JSON.stringify(actualColumns)) {
      return `columns differ: ${columns.join(",")} against ${actualColumns.join(",")}`;
   }
   const recorded = orderings.get(expected) ?? orderings.get(actual);
   const known = ordering ?? recorded;
   const bothAnswers =
      ordering !== undefined ||
      (orderings.has(expected) && orderings.has(actual));
   return rowsDifference(
      expected,
      actual,
      known && !bothAnswers ? { orderBy: [], nests: known.nests } : known,
      "",
   );
}
