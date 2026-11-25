// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a source and a `storage=` destination are the same DuckLake catalog
 * ({@link isSameCatalog}), and whether a source's definition carries author
 * SQL ({@link carriesAuthorSql}): together, whether a build may run on a
 * read-write session on that catalog. Read by the build session and the
 * `storage=` tier.
 *
 * Pure. Both answers fail closed: a config it cannot compare (a
 * `connectionString`) is not the same catalog, and an unreadable definition
 * carries author SQL.
 */

import { components } from "../../../api";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Whether a source's connection and a storage destination are the SAME DuckLake
 * catalog, decided by what locates the catalog, never by name: a connection and
 * a destination may share a name and mean different catalogs (the namespaces are
 * separate on purpose), and two names may reach one catalog. A catalog is its
 * metadata: the catalog database (host, port, database) and the metadata schema
 * in it. The data path is recorded in the catalog, so it is compared only when
 * both configs state one: two configs that state different paths would write
 * the one catalog's files to two places, and are not treated as the same.
 * Only the discrete `host`/`port`/`databaseName` fields are compared: a
 * `connectionString` config never matches, which fails closed (the build is
 * refused as an unsupported source, never run on the wrong catalog).
 */
export function isSameCatalog(
   source: ApiConnection | undefined,
   destination: ApiConnection | undefined,
): boolean {
   const a =
      source?.type === "ducklake" ? source.ducklakeConnection : undefined;
   const b =
      destination?.type === "ducklake"
         ? destination.ducklakeConnection
         : undefined;
   if (!a || !b) return false;
   const pa = a.catalog?.postgresConnection;
   const pb = b.catalog?.postgresConnection;
   if (!pa || !pb) return false;
   const path = (url: string | undefined) => (url ?? "").replace(/\/+$/, "");
   const pathA = path(a.storage?.bucketUrl);
   const pathB = path(b.storage?.bucketUrl);
   return (
      !!pa.host &&
      pa.host === pb.host &&
      (pa.port ?? 5432) === (pb.port ?? 5432) &&
      !!pa.databaseName &&
      pa.databaseName === pb.databaseName &&
      (a.catalog.metadataSchema ?? "") === (b.catalog.metadataSchema ?? "") &&
      (pathA === "" || pathB === "" || pathA === pathB)
   );
}

/** A catalog table path: dot-separated plain identifiers, nothing a reader resolves. */
const CATALOG_TABLE_PATH =
   /^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$/;

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_DEPTH = 200;

/**
 * Whether a compiled source definition carries SQL its author wrote, or reads
 * something other than the catalog's own tables: an `.sql()` source
 * (`type: 'sql_select'`), a raw SQL expression (`node: 'genericSQLExpr'`, which
 * `sql_number()` and its siblings compile to), or a table whose path is a file
 * or URL rather than a dotted identifier, anywhere beneath it, joins and
 * query-source pipelines included.
 *
 * A same-catalog build runs on a READ-WRITE session on the catalog, so it may run
 * only SQL Malloy generated; this is what keeps author text off that session.
 *
 * A built-in function call (`round`, `nullif`, a `.day` truncation's helper) carries
 * its overload's definition in the IR, and that definition's per-dialect template is a
 * `genericSQLExpr`: the compiler's SQL, not the author's. The walk skips a call's
 * `overload` and keeps walking its arguments, where an author's `sql_number()` would
 * be. Without that, any source using a built-in, a pre-aggregate rollup over one
 * included, read as author SQL.
 * Fails closed: an unreadable definition, or one deeper than the walk allows,
 * counts as carrying author SQL.
 */
export function carriesAuthorSql(def: unknown): boolean {
   return authorSqlCause(def) !== undefined;
}

/** Where, inside a source, the walk is: a join, a source a query reads, a field. */
type Frame =
   | { kind: "join"; as: string; source: string }
   | { kind: "reads"; source: string }
   | { kind: "field"; name: string };

/** What {@link authorSqlCause} found, before it is put in words. */
type Found =
   | { kind: "sql_source" | "sql_expression" | "unreadable" }
   | { kind: "file_table"; path: string };

/**
 * What {@link carriesAuthorSql} found and where, in words that complete "the
 * source …": `it is an .sql() source`, `its join 'sn' reads source
 * 'status_names', which is an .sql() source`, `its field 'x' uses a sql_*
 * expression`, `it reads '/x.csv' as a table`. Undefined exactly when
 * {@link carriesAuthorSql} is false.
 */
export function authorSqlCause(def: unknown): string | undefined {
   const seen = new Set<object>();
   const walk = (
      node: unknown,
      depth: number,
      frames: readonly Frame[],
   ): { found: Found; frames: readonly Frame[] } | undefined => {
      if (depth > MAX_DEPTH) return { found: UNREADABLE, frames };
      if (node === null || typeof node !== "object") return undefined;
      if (seen.has(node)) return undefined;
      seen.add(node);
      if (Array.isArray(node)) {
         for (const child of node) {
            const hit = walk(child, depth + 1, frames);
            if (hit) return hit;
         }
         return undefined;
      }
      const record = node as Record<string, unknown>;
      if (typeof record.join === "string") {
         frames = [
            ...frames,
            {
               kind: "join",
               as: String(record.as ?? record.name ?? ""),
               source: sourceNameOf(record),
            },
         ];
      }
      if (record.type === "sql_select") {
         return { found: { kind: "sql_source" }, frames };
      }
      if (record.node === "genericSQLExpr") {
         return { found: { kind: "sql_expression" }, frames };
      }
      // DuckDB reads a file or URL named as a table (`lake.table('/x.csv')`,
      // `lake.table('s3://other/y.parquet')`), which on this session would run
      // with the destination's storage credential: only a catalog table, a
      // dotted identifier path, is Malloy reading the catalog.
      if (
         record.type === "table" &&
         typeof record.tablePath === "string" &&
         !CATALOG_TABLE_PATH.test(record.tablePath)
      ) {
         return {
            found: { kind: "file_table", path: record.tablePath },
            frames,
         };
      }
      for (const [key, child] of Object.entries(record)) {
         // A call's overload is the compiler's own SQL; its arguments are walked.
         if (record.node === "function_call" && key === "overload") continue;
         const hit =
            key === "fields" && Array.isArray(child)
               ? walkFields(child, depth + 1, frames)
               : walk(
                    child,
                    depth + 1,
                    key === "structRef" &&
                       child !== null &&
                       typeof child === "object"
                       ? [
                            ...frames,
                            {
                               kind: "reads",
                               source: sourceNameOf(
                                  child as Record<string, unknown>,
                               ),
                            },
                         ]
                       : frames,
                 );
         if (hit) return hit;
      }
      return undefined;
   };
   /** A source's fields, each walked in a frame naming it (a join names itself). */
   const walkFields = (
      fields: unknown[],
      depth: number,
      frames: readonly Frame[],
   ): ReturnType<typeof walk> => {
      if (depth > MAX_DEPTH) return { found: UNREADABLE, frames };
      if (seen.has(fields)) return undefined;
      seen.add(fields);
      for (const field of fields) {
         const f = field as Record<string, unknown> | null;
         const named =
            f !== null &&
            typeof f === "object" &&
            typeof f.name === "string" &&
            typeof f.join !== "string";
         const hit = walk(
            field,
            depth + 1,
            named
               ? [...frames, { kind: "field", name: String(f.name) }]
               : frames,
         );
         if (hit) return hit;
      }
      return undefined;
   };
   try {
      if (def === null || typeof def !== "object")
         return describe([], UNREADABLE);
      const hit = walk(def, 0, []);
      return hit ? describe(hit.frames, hit.found) : undefined;
   } catch {
      return describe([], UNREADABLE);
   }
}

const UNREADABLE: Found = { kind: "unreadable" };

/** A source's name as its author wrote it: its sourceID before the `@`. */
function sourceNameOf(record: Record<string, unknown>): string {
   const id = typeof record.sourceID === "string" ? record.sourceID : "";
   const name = typeof record.name === "string" ? record.name : "";
   return id.split("@")[0] || (name.startsWith("sql://") ? "" : name);
}

/** {@link authorSqlCause}'s words for a finding beneath `frames`. */
function describe(frames: readonly Frame[], found: Found): string {
   if (found.kind === "unreadable") {
      return "its definition could not be read to check for SQL its author wrote";
   }
   // A source a query reads is named only when it is what carries the SQL
   // (an .sql() source, a file read as a table): its joins and fields are the
   // query's own, and read as such.
   frames = frames.filter(
      (frame, i) =>
         frame.kind !== "reads" ||
         (i === frames.length - 1 && found.kind !== "sql_expression"),
   );
   const parts: string[] = [];
   let lead = true;
   for (const frame of frames) {
      const possessive = lead ? "its" : "whose";
      const subject = lead ? "it" : "which";
      lead = false;
      if (frame.kind === "join") {
         parts.push(
            `${possessive} join '${frame.as}'` +
               (frame.source ? ` reads source '${frame.source}'` : ""),
         );
      } else if (frame.kind === "reads") {
         parts.push(
            frame.source
               ? `${subject} reads source '${frame.source}'`
               : `${subject} reads a source`,
         );
      } else {
         parts.push(`${possessive} field '${frame.name}'`);
      }
   }
   const last = frames[frames.length - 1];
   const subject = frames.length === 0 ? "it" : ", which";
   const what =
      found.kind === "sql_source"
         ? `${subject} is an .sql() source`
         : found.kind === "file_table"
           ? `${subject} reads '${found.path.replace(/^'|'$/g, "")}' as a table`
           : last?.kind === "field"
             ? " uses a sql_* expression"
             : `${subject} uses a sql_* expression`;
   return (parts.join(", ") + what).replace(/^ /, "");
}
