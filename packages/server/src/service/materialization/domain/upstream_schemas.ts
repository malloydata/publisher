// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The warehouse column types of the tables a persist source's build reads,
 * as one package load read them from each warehouse's catalog, and what a
 * source's content address folds in from them, so a table retyped upstream
 * moves the address of every source that reads it and its table is rebuilt.
 *
 * A reading is per table, keyed by connection and the table path as the
 * model writes it ({@link upstreamTableKey}): its columns with their native
 * types (`NUMERIC(12,4)`, `FLOAT64`, `DOUBLE`), or why it holds none: the
 * dialect gives the table no catalog to read (`no_catalog`), or the read
 * failed and no earlier reading of the table is known (`read_failed`). Both
 * are stable values: the same table read the same way folds the same text,
 * so a failure never moves an address back and forth between loads.
 *
 * A source whose tables cannot be known (`reads` omitted: an `.sql()`
 * source, a raw SQL expression, an unresolved reference in its lineage)
 * folds nothing, and keeps the address its SQL alone gives it.
 *
 * Pure, and plain data, so it crosses the package-load worker.
 */

import type { Connection } from "@malloydata/malloy";
import type { components } from "../../../api";

/** One table's columns as its warehouse's catalog names their types. */
export type UpstreamTableSchema = components["schemas"]["UpstreamTableSchema"];

/** One table's reading: its columns, or why there are none. */
export type UpstreamSchemaReading =
   | {
        columns: { name: string; nativeType: string }[];
        /**
         * The dialect the types are named in (Snowflake's `FLOAT` is a
         * double, DuckDB's a float4). Not folded into the address: the
         * connection's digest already is.
         */
        dialect?: string;
     }
   | {
        /**
         * Why there are no columns. Permanent, folded into the address as a
         * stable value: `no_catalog` (the dialect gives the table no catalog
         * to read), `not_found` (the catalog holds no such table: a BigQuery
         * wildcard table, a table not created yet), `ambiguous` (an unqualified
         * name the catalog holds in more than one schema). Transient, so the
         * plan refuses each source that reads it until a read succeeds:
         * `read_failed` (the catalog query or the connection failed, or timed
         * out).
         */
        unread: "no_catalog" | "not_found" | "ambiguous" | "read_failed";
     };

/** Every table a plan's sources read, by {@link upstreamTableKey}. */
export type UpstreamSchemaReadings = Readonly<
   Record<string, UpstreamSchemaReading>
>;

/** One table a build reads, as the plan's `reads` names it. */
export interface UpstreamTable {
   connectionName: string;
   table: string;
}

/**
 * A column of a table a build reads: what a stored column is a plain copy
 * of, when it is one (`TablePlan.columnOrigins`).
 */
export interface UpstreamColumn extends UpstreamTable {
   /** The column's name in that table. */
   column: string;
}

/**
 * The warehouse column each stored column of a source is a plain copy of,
 * from its compiled `SourceDef` (`def`), by the name its build SQL emits the
 * column under (the field's `name`, which a rename keeps beside its `as`):
 * every column of a `.table()` source's own relation, and every column a
 * query-shaped source selects unchanged, in one `select:` stage, from such
 * a column of the source it runs on (`src -> { select: a, b }`, the shape
 * of a `#(materialize)` build source). A field with an expression or a
 * join, a grouping or aggregating stage, a `.sql()` source and a source
 * named by reference rather than inlined have none.
 */
export function columnOriginsOf(
   def: unknown,
): ReadonlyMap<string, UpstreamColumn> {
   return new Map(plainColumns(def, 0).map((c) => [c.name, c.origin]));
}

/** One stored column that copies a warehouse column, and the name the source reads it by. */
interface PlainColumn {
   name: string;
   active: string;
   origin: UpstreamColumn;
}

/** A field definition, as {@link plainColumns} reads it. */
interface FieldDefLike {
   name?: unknown;
   as?: unknown;
   e?: unknown;
   join?: unknown;
}

/** {@link columnOriginsOf}'s walk, with the name each column is read by. */
function plainColumns(def: unknown, depth: number): PlainColumn[] {
   const d = def as {
      type?: unknown;
      tablePath?: unknown;
      connection?: unknown;
      fields?: unknown;
      query?: { structRef?: unknown; pipeline?: unknown };
   } | null;
   if (depth > 16 || !d || !Array.isArray(d.fields)) return [];
   const plain = (d.fields as FieldDefLike[]).filter(
      (f): f is FieldDefLike & { name: string } =>
         typeof f.name === "string" &&
         f.e === undefined &&
         f.join === undefined,
   );
   const named = (
      f: { name: string; as?: unknown },
      origin: UpstreamColumn,
   ) => ({
      name: f.name,
      active: typeof f.as === "string" ? f.as : f.name,
      origin,
   });
   if (d.type === "table") {
      const { tablePath, connection } = d;
      if (typeof tablePath !== "string" || typeof connection !== "string") {
         return [];
      }
      return plain.map((f) =>
         named(f, {
            connectionName: connection,
            table: tablePath,
            column: f.name,
         }),
      );
   }
   const stages = d.query?.pipeline;
   const stage =
      Array.isArray(stages) && stages.length === 1 ? stages[0] : undefined;
   const ref = d.query?.structRef;
   if (
      d.type !== "query_source" ||
      (stage as { type?: unknown } | undefined)?.type !== "project" ||
      !ref ||
      typeof ref !== "object"
   ) {
      return [];
   }
   const base = new Map(
      plainColumns(ref, depth + 1).map((c) => [c.active, c.origin]),
   );
   const selected = new Map<string, UpstreamColumn>();
   for (const qf of ((stage as { queryFields?: unknown }).queryFields ??
      []) as {
      type?: unknown;
      path?: unknown;
   }[]) {
      const path = qf.path;
      if (qf.type !== "fieldref" || !Array.isArray(path) || path.length !== 1) {
         continue;
      }
      const origin = base.get(path[0] as string);
      if (origin) selected.set(path[0] as string, origin);
   }
   return plain.flatMap((f) => {
      const origin = selected.get(f.name);
      return origin ? [named(f, origin)] : [];
   });
}

/**
 * The warehouse type of each stored column in `origins`, by its stored
 * name, from `readings` ({@link upstreamColumnType}); undefined when none is
 * known. What a `storage=` build judges each column's `gateExact` by.
 */
export function upstreamTypesOf(
   origins: ReadonlyMap<string, UpstreamColumn> | undefined,
   readings: UpstreamSchemaReadings | undefined,
): ReadonlyMap<string, { dialect: string; nativeType: string }> | undefined {
   if (!origins || !readings) return undefined;
   const types = new Map<string, { dialect: string; nativeType: string }>();
   for (const [name, origin] of origins) {
      const type = upstreamColumnType(origin, readings);
      if (type) types.set(name, type);
   }
   return types.size > 0 ? types : undefined;
}

/**
 * The type `column` has in its table's reading, with the dialect the type is
 * named in, or undefined: the table holds no reading, an unread one or one
 * with no dialect, or not exactly one column of that name (matched exactly,
 * else case-blind).
 */
function upstreamColumnType(
   column: UpstreamColumn,
   readings: UpstreamSchemaReadings | undefined,
): { dialect: string; nativeType: string } | undefined {
   const reading = readings?.[upstreamTableKey(column)];
   if (!reading || "unread" in reading || !reading.dialect) return undefined;
   const exact = reading.columns.filter((c) => c.name === column.column);
   const folded = reading.columns.filter(
      (c) => c.name.toLowerCase() === column.column.toLowerCase(),
   );
   const found =
      exact.length === 1
         ? exact[0]
         : folded.length === 1
           ? folded[0]
           : undefined;
   return found
      ? { dialect: reading.dialect, nativeType: found.nativeType }
      : undefined;
}

/** The key of one table's reading: its connection and its path. */
export function upstreamTableKey(table: UpstreamTable): string {
   return JSON.stringify([table.connectionName, table.table]);
}

/**
 * A native type spelled one way whatever the catalog's spacing and case:
 * `numeric(12, 4)` and `NUMERIC(12,4)` are one type.
 */
function canonicalType(nativeType: string): string {
   return nativeType
      .replace(/\s+/g, " ")
      .replace(/\s*([(),])\s*/g, "$1")
      .trim()
      .toUpperCase();
}

/**
 * Whether `sql` names the column `name`: as a word, case-blind, quoted or
 * not. Over-inclusive on purpose (a string literal or an alias of that name
 * counts too): a column it misses would leave a retype of it unseen.
 */
function namesColumn(sql: string, name: string): boolean {
   const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   return new RegExp(`(^|[^A-Za-z0-9_])${escaped}($|[^A-Za-z0-9_])`, "i").test(
      sql,
   );
}

/**
 * What the content address of a source reading `reads` with build SQL `sql`
 * folds in: each table it reads with its reading, keeping only the columns
 * the SQL names (sorted by name), so a column it does not read (one added,
 * dropped or retyped beside it) leaves its address alone. Undefined, so the
 * address is its SQL's alone, when the tables cannot be known (`reads`
 * undefined) or none of them has a reading (a plan derived without reading
 * any, which only a spec does).
 */
export function upstreamSchemaFold(
   reads: readonly UpstreamTable[] | undefined,
   readings: UpstreamSchemaReadings | undefined,
   sql: string,
): string | undefined {
   if (!reads || !readings) return undefined;
   const parts: unknown[] = [];
   for (const table of reads) {
      const reading = readings[upstreamTableKey(table)];
      if (!reading) continue;
      parts.push([
         table.connectionName,
         table.table,
         "unread" in reading
            ? reading.unread
            : reading.columns
                 .filter((c) => namesColumn(sql, c.name))
                 .map((c) => [c.name, canonicalType(c.nativeType)])
                 .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
      ]);
   }
   return parts.length === 0 ? undefined : JSON.stringify(parts);
}

/**
 * The tables a build of a source reading `reads` read, with their readings,
 * as a manifest entry records them (`ManifestEntry.upstreamSchemas`).
 * Undefined when the address folded none ({@link upstreamSchemaFold}).
 */
export function upstreamSchemasOf(
   reads: readonly UpstreamTable[] | undefined,
   readings: UpstreamSchemaReadings | undefined,
): UpstreamTableSchema[] | undefined {
   if (!reads || !readings) return undefined;
   const out: UpstreamTableSchema[] = [];
   for (const table of reads) {
      const reading = readings[upstreamTableKey(table)];
      if (!reading) continue;
      out.push({
         connectionName: table.connectionName,
         table: table.table,
         ...("unread" in reading
            ? { unread: reading.unread }
            : {
                 columns: reading.columns.map((c) => ({ ...c })),
                 ...(reading.dialect ? { dialect: reading.dialect } : {}),
              }),
      });
   }
   return out.length === 0 ? undefined : out;
}

/** What a reading needs of a connection. */
export type UpstreamReadConnection = Pick<
   Connection,
   "runSQL" | "dialectName"
> & {
   getDigest?(): string | Promise<string>;
};

/**
 * Reads upstream tables' catalog types on the main thread
 * (run/upstream_schema_read.ts). The package-load pool answers a worker's
 * request with the one its job was handed, so it imports no run/ code.
 */
export type UpstreamSchemaReader = (args: {
   tables: readonly UpstreamTable[];
   connection: (connectionName: string) => Promise<UpstreamReadConnection>;
   refresh: boolean;
}) => Promise<Record<string, UpstreamSchemaReading>>;
