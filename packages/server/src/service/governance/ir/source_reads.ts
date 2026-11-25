// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The warehouse tables a persist source's build reads, from its compiled
 * definition: the plan's `reads`. A host rebuilds only the sources whose
 * `reads` hold a table that was replaced.
 *
 * Read off the same fully inlined definition the plan's `sql` and
 * `sourceEntityId` are computed from, so a table a persisted upstream reads is
 * listed for its downstream too: the downstream's data derives from it, and a
 * downstream built while the upstream has no table reads it directly.
 *
 * A join counts only when the build reads it: a query stage's `activeJoins`
 * (the joins its fields, filters and nests reach), resolved against the
 * stage's input and its `extend:` block. A join declared on the source and
 * never used is not read; neither is any join of a source a build copies whole.
 *
 * Unknown, and so `undefined`, whenever the definition carries SQL its author
 * wrote: an `.sql()` source anywhere in what the build reads (its text names
 * tables Malloy never parses), or a raw SQL expression (`sql_number()` and its
 * siblings, which may hold a subquery) anywhere in the definition. Also unknown
 * for a shape this walk does not recognise or a join path it cannot resolve.
 * The caller omits the field then, never reports a partial list: a host reads a
 * missing `reads` as "may read any table" and an empty one as "reads none".
 */

import { sourceDefOf } from "./compiled_model";

/** One warehouse table a build reads. */
interface TableRead {
   connectionName: string;
   table: string;
}

type Obj = Record<string, unknown>;

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_DEPTH = 200;

class Unknown extends Error {}

function isObj(value: unknown): value is Obj {
   return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Whether `def` holds a raw SQL expression. A built-in call carries its
 * overload's per-dialect template as one, which is the compiler's SQL, so a
 * call's `overload` is skipped and its arguments walked.
 */
function holdsRawSqlExpression(def: unknown): boolean {
   const seen = new Set<object>();
   const walk = (node: unknown, depth: number): boolean => {
      if (depth > MAX_DEPTH) return true;
      if (node === null || typeof node !== "object") return false;
      if (seen.has(node)) return false;
      seen.add(node);
      if (Array.isArray(node)) return node.some((c) => walk(c, depth + 1));
      const record = node as Obj;
      if (record.node === "genericSQLExpr") return true;
      return Object.entries(record).some(
         ([key, child]) =>
            !(record.node === "function_call" && key === "overload") &&
            walk(child, depth + 1),
      );
   };
   return walk(def, 0);
}

/** The join named `name` among `fields`, or undefined. */
function joinNamed(fields: unknown, name: string): Obj | undefined {
   if (!Array.isArray(fields)) return undefined;
   return fields.find(
      (f): f is Obj =>
         isObj(f) && f.join !== undefined && (f.as ?? f.name) === name,
   );
}

/** Every join declared among `fields`, nested joins included. */
function allJoins(fields: unknown): Obj[] {
   if (!Array.isArray(fields)) return [];
   return fields
      .filter((f): f is Obj => isObj(f) && f.join !== undefined)
      .flatMap((join) => [join, ...allJoins(join.fields)]);
}

/** The join a path of aliases reaches, from the first stage's roots. */
function resolveJoinPath(roots: unknown[][], path: string[]): Obj {
   for (const fields of roots) {
      let at: Obj | undefined;
      let scope: unknown = fields;
      for (const alias of path) {
         at = joinNamed(scope, alias);
         if (!at) break;
         scope = at.fields;
      }
      if (at) return at;
   }
   throw new Unknown(`join path ${path.join(".")} did not resolve`);
}

/** The tables a relation's own build reads (its rows, not its joins). */
function relationReads(def: unknown, out: TableRead[], depth: number): void {
   if (depth > MAX_DEPTH || !isObj(def)) throw new Unknown("unreadable");
   switch (def.type) {
      case "table":
         if (
            typeof def.connection !== "string" ||
            typeof def.tablePath !== "string"
         ) {
            throw new Unknown("table without a path");
         }
         out.push({ connectionName: def.connection, table: def.tablePath });
         return;
      case "query_source":
         queryReads(def.query, out, depth + 1);
         return;
      case "composite":
         // The build reads the member a query resolves to; a composite seen
         // here unresolved may read any of them.
         if (!Array.isArray(def.sources)) throw new Unknown("composite");
         for (const member of def.sources) {
            relationReads(member, out, depth + 1);
         }
         return;
      default:
         // `sql_select` (author SQL), `virtual`, or a shape not known here.
         throw new Unknown(`relation type ${String(def.type)}`);
   }
}

/** The tables a query reads: its input's rows and the joins each stage uses. */
function queryReads(query: unknown, out: TableRead[], depth: number): void {
   if (!isObj(query)) throw new Unknown("query");
   const input = query.compositeResolvedSourceDef ?? query.structRef;
   // A string structRef names a model source this definition does not hold.
   if (!isObj(input)) throw new Unknown("unresolved structRef");
   relationReads(input, out, depth + 1);
   const pipeline = Array.isArray(query.pipeline) ? query.pipeline : [];
   pipeline.forEach((segment, index) => {
      if (!isObj(segment)) throw new Unknown("segment");
      const extended = Array.isArray(segment.extendSource)
         ? segment.extendSource
         : [];
      const roots = index === 0 ? [extended, input.fields] : [extended];
      const joins = Array.isArray(segment.activeJoins)
         ? (segment.activeJoins as { path?: unknown }[]).map((usage) => {
              if (!Array.isArray(usage.path)) throw new Unknown("join usage");
              return resolveJoinPath(
                 roots as unknown[][],
                 usage.path as string[],
              );
           })
         : // No usage recorded: every join the stage could reach.
           roots.flatMap(allJoins);
      for (const join of joins) relationReads(join, out, depth + 1);
   });
}

/**
 * The warehouse tables the build of persist source `source` reads, sorted and
 * distinct; undefined when that cannot be known (see the module comment).
 */
export function sourceReads(source: {
   _sourceDef: unknown;
}): TableRead[] | undefined {
   try {
      const def: unknown = sourceDefOf(source);
      if (holdsRawSqlExpression(def)) return undefined;
      const out: TableRead[] = [];
      relationReads(def, out, 0);
      const distinct = new Map(
         out.map((r) => [JSON.stringify([r.connectionName, r.table]), r]),
      );
      return [...distinct.entries()]
         .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
         .map(([, r]) => r);
   } catch {
      return undefined;
   }
}

/**
 * The warehouse tables a compiled query reads: its entry point's rows (the
 * resolved struct, or a composite's resolved member) and every join its
 * stages reach, sorted and distinct; undefined when that cannot be known, by
 * the same rule as {@link sourceReads} (author SQL anywhere in it).
 */
export function queryTableReads(
   entry: unknown,
   pipeline: unknown,
): TableRead[] | undefined {
   return sourceReads({
      _sourceDef: {
         type: "query_source",
         query: { structRef: entry, pipeline },
      },
   });
}
