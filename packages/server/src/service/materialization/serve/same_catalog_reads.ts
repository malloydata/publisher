// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A served copy's join across connections that are one catalog, read through
 * the storage destination.
 *
 * On a managed database the host configures each catalog twice: as a storage
 * destination and as a `managed` DuckLake connection. A stored source on the
 * connection is built into the destination, and its served copy re-roots it
 * there, onto the destination's alias; a source it joins stays on the
 * connection, so Malloy refuses the join (cross_connection_text.ts). When the
 * build's own catalog identity check (`isSameCatalog`) says the destination
 * and the joined source's connection are one catalog, the copy instead reads
 * the connection's tables through the destination too: each
 * `<connection>.table('<path>')` in the copy's files becomes
 * `<alias>.table('<destination>.<schema>.<table>')`, the path qualified as
 * the managed connection resolves it (its `main` by default). The pair then
 * sits on one Malloy connection and the join compiles.
 *
 * That is what the build already does (`buildSameCatalogIntoStorage`): it
 * reads a managed connection's tables with the destination's credential, and
 * holds what it reads to what the connection could read itself. The
 * destination's credential reads the whole catalog, so the same holds here,
 * and a connection's tables are moved only when every one of them:
 *
 * - is on a connection recorded `managed`, whose scope check
 *   (`managedTablePathRefusal`) admits its path, read as the connection reads
 *   it;
 * - is a base table: a DuckLake view's body is not held to that scope, and on
 *   the destination would read with the destination's credential;
 * - is named in a plain `.table('<dotted path>')`, with no `.sql()` (or any
 *   other call) on the connection in the files, and no source on the
 *   connection the copy keeps as written carries author SQL (`authorSqlCause`:
 *   an `.sql()` source, a `sql_*` expression, a file read as a table), which
 *   would otherwise run on the destination's session.
 *
 * Anything else leaves the files as they are, and the join is left out as
 * before (its decline names it). Only text changes: each moved source keeps
 * its own annotations, `where:`, givens and gates as written, so it answers
 * as live does, and admission and collation still judge each table by its own
 * recorded facts (the author's compile, and each stored table's contract).
 */

import { lexAll, type Token } from "../../malloy_lexer_core";
import { identifier, stringLiteral } from "../../malloy_text";
import { managedTablePathRefusal } from "../../managed_catalog_scope";
import { authorSqlCause } from "../domain/same_catalog_build";
import { authorModelReachContext } from "../text/author_model";
import { refusedJoinPairs } from "../text/cross_connection_text";
import { destinationOfAlias } from "./served_copy_config";

/** A plain SQL identifier, as a managed connection's scope check admits one. */
const PLAIN = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** One connection whose tables a copy reads through a destination. */
interface SameCatalogMove {
   /** The connection the author's text names. */
   connection: string;
   /** The destination's served-copy alias. */
   alias: string;
   /** The destination's configured name: the catalog's name on its session. */
   destination: string;
   /** The schemas the managed connection may read. */
   schemas: ReadonlySet<string>;
}

/**
 * `path` as the managed connection `connection` resolves it, qualified with
 * the catalog's name on the destination's session: `t`, `main.t`,
 * `<connection>.t` and `<connection>.main.t` are `main`'s; `s.t` and
 * `<connection>.s.t` are schema `s`'s, for a schema `schemas` holds.
 * Undefined for any other path, or one a name of which is not plain.
 * Case-insensitive, as DuckDB resolves names.
 */
export function destinationTablePath(
   path: string,
   move: Pick<SameCatalogMove, "connection" | "destination" | "schemas">,
): string | undefined {
   const parts = path.split(".");
   if (!PLAIN.test(move.destination) || !parts.every((p) => PLAIN.test(p))) {
      return undefined;
   }
   const self = move.connection.toLowerCase();
   const readable = (s: string) => move.schemas.has(s.toLowerCase());
   const at = (schema: string, table: string) =>
      `${move.destination}.${schema}.${table}`;
   if (parts.length === 1) return at("main", parts[0]);
   if (parts.length === 2) {
      const [first, table] = parts;
      // `<connection>.t` is main's, unless a schema of that name makes it ambiguous.
      if (first.toLowerCase() === self) {
         return readable(first) ? undefined : at("main", table);
      }
      return readable(first) ? at(first, table) : undefined;
   }
   if (parts.length === 3) {
      const [catalog, schema, table] = parts;
      return catalog.toLowerCase() === self && readable(schema)
         ? at(schema, table)
         : undefined;
   }
   return undefined;
}

/** A Malloy single- or double-quoted string literal's value, with no escape in it. */
function literalValue(token: Token | undefined): string | undefined {
   const m = /^(['"])([^'"\\]*)\1$/.exec(token?.text ?? "");
   return m ? m[2] : undefined;
}

/** Whether `token` names connection `name`, bare or backquoted. */
function namesConnection(token: Token, name: string): boolean {
   return (
      (token.kind === "word" && token.text === name) ||
      token.text === `\`${name}\``
   );
}

/**
 * `text` with each `<connection>.table('<path>')` read through `move`'s
 * destination, and the paths it moved; unchanged (with no paths) when it
 * names no table on the connection. Undefined when the connection is named
 * in any other call (`.sql()`, `.virtual()`), with a literal it cannot read,
 * or with a path {@link destinationTablePath} cannot place, or the text
 * does not lex.
 */
export function readThroughDestination(
   text: string,
   move: Pick<
      SameCatalogMove,
      "connection" | "alias" | "destination" | "schemas"
   >,
): { text: string; paths: string[] } | undefined {
   const tokens = lexAll(text);
   if (!tokens) return undefined;
   const edits: { start: number; end: number; text: string }[] = [];
   const paths: string[] = [];
   for (let k = 0; k < tokens.length; k++) {
      const t = tokens[k];
      if (!namesConnection(t, move.connection)) continue;
      // A path through a join of that name, `x.credible.y`, names no connection.
      if (tokens[k - 1]?.text === ".") continue;
      if (tokens[k + 1]?.text !== ".") continue;
      const method = tokens[k + 2];
      if (method?.kind !== "word") continue;
      if (method.text !== "table") return undefined;
      const value = literalValue(tokens[k + 4]);
      if (
         tokens[k + 3]?.text !== "(" ||
         value === undefined ||
         tokens[k + 5]?.text !== ")"
      ) {
         return undefined;
      }
      const moved = destinationTablePath(value, move);
      if (moved === undefined) return undefined;
      paths.push(value);
      edits.push({
         start: t.start,
         end: tokens[k + 5].end,
         text: `${identifier(move.alias)}.table(${stringLiteral(moved)})`,
      });
      k += 5;
   }
   let out = text;
   for (const e of edits.reverse()) {
      out = out.slice(0, e.start) + e.text + out.slice(e.end);
   }
   return { text: out, paths };
}

/**
 * What a copy reads of its connections to decide (same_catalog_destination.ts,
 * over the copy's own connections).
 */
interface SameCatalogPorts {
   /**
    * The schemas managed connection `connection` may read, when it and the
    * destination `alias` stands for are one catalog; undefined otherwise.
    */
   scope(
      connection: string,
      alias: string,
   ): Promise<ReadonlySet<string> | undefined>;
   /** Whether each `[catalog, schema, table]` is a base table on the destination `alias` stands for. */
   baseTables(
      alias: string,
      tables: readonly (readonly [string, string, string])[],
   ): Promise<boolean>;
}

/**
 * The connections a cross-connection refusal joins that are one catalog with
 * the destination they are joined into (either way round), each a managed
 * one; undefined when any refused join is not such a pair, since the copy
 * then leaves joins out anyway.
 */
async function sameCatalogMoves(
   error: unknown,
   ports: SameCatalogPorts,
): Promise<SameCatalogMove[] | undefined> {
   const pairs = refusedJoinPairs(error);
   if (!pairs?.length) return undefined;
   const moves = new Map<string, SameCatalogMove>();
   for (const { joined, into } of pairs) {
      const intoDestination = destinationOfAlias(into);
      const joinedDestination = destinationOfAlias(joined);
      // One side the destination's alias, the other a connection.
      const [connection, alias, destination] =
         intoDestination !== undefined && joinedDestination === undefined
            ? [joined, into, intoDestination]
            : joinedDestination !== undefined && intoDestination === undefined
              ? [into, joined, joinedDestination]
              : [];
      if (!connection || !alias || !destination) return undefined;
      const known = moves.get(connection);
      if (known) {
         if (known.alias !== alias) return undefined;
         continue;
      }
      const schemas = await ports.scope(connection, alias);
      if (!schemas) return undefined;
      moves.set(connection, { connection, alias, destination, schemas });
   }
   return [...moves.values()];
}

/**
 * Why a source on one of `connections` in the author's model may not be read
 * through a destination, or undefined when none carries author SQL: each such
 * source, and each join of a re-rooted one (`rerooted`, by sourceID), whose
 * own text the copy replaced, is walked with `authorSqlCause`.
 */
function movedAuthorSqlCause(
   modelDef: unknown,
   connections: ReadonlySet<string>,
   rerooted: ReadonlySet<string>,
): string | undefined {
   const { contents } = authorModelReachContext(modelDef);
   for (const [name, def] of Object.entries(contents)) {
      const d = def as {
         connection?: unknown;
         sourceID?: unknown;
         fields?: unknown;
      };
      if (typeof d.connection !== "string" || !connections.has(d.connection)) {
         continue;
      }
      if (typeof d.sourceID === "string" && rerooted.has(d.sourceID)) {
         for (const field of Array.isArray(d.fields) ? d.fields : []) {
            const f = field as { join?: unknown; name?: unknown };
            if (f.join === undefined) continue;
            const cause = authorSqlCause(field);
            if (cause)
               return `the join '${String(f.name)}' of '${name}': ${cause}`;
         }
         continue;
      }
      const cause = authorSqlCause(def);
      if (cause) return `the source '${name}': ${cause}`;
   }
   return undefined;
}

/** The files each source on one of `connections` in the author's model is declared in. */
function declaringFiles(
   modelDef: unknown,
   connections: ReadonlySet<string>,
): string[] {
   const { contents } = authorModelReachContext(modelDef);
   const urls = new Set<string>();
   for (const def of Object.values(contents)) {
      const d = def as { connection?: unknown; location?: { url?: unknown } };
      if (
         typeof d.connection === "string" &&
         connections.has(d.connection) &&
         typeof d.location?.url === "string"
      ) {
         urls.add(d.location.url);
      }
   }
   return [...urls];
}

/**
 * The copy's files with every table on a connection the refusal `error`
 * joins across one catalog read through the destination
 * ({@link readThroughDestination}), by URL, and the destinations they are
 * read through, which the copy's config looks up under their aliases
 * (`servedCopyConfig`'s `tablesUnderAlias`). `declined` says why not, for
 * the log, when the refusal is not only such joins or any condition of the
 * file's doc fails, so the caller leaves the joins out as before.
 */
export async function readSameCatalogThroughDestination(args: {
   error: unknown;
   /** The copy's files, by URL. */
   files: ReadonlyMap<string, string>;
   /** A file's text by URL, when `files` does not hold it. */
   readText: (url: string) => string | undefined;
   /** The author's compiled model. */
   modelDef: unknown;
   /** The sourceIDs the copy re-roots. */
   rerooted: ReadonlySet<string>;
   /** What it reads of the copy's connections. */
   ports: SameCatalogPorts;
}): Promise<
   | { files: Map<string, string>; destinations: Set<string> }
   | { declined: string }
> {
   const moves = await sameCatalogMoves(args.error, args.ports);
   if (!moves) {
      return {
         declined:
            "a refused join is not into a managed connection's own catalog",
      };
   }
   const names = new Set(moves.map((m) => m.connection));
   const sql = movedAuthorSqlCause(args.modelDef, names, args.rerooted);
   if (sql) return { declined: `${sql}, which carries author SQL` };
   const urls = new Set([
      ...args.files.keys(),
      ...declaringFiles(args.modelDef, names),
   ]);
   const out = new Map(args.files);
   for (const move of moves) {
      const paths: string[] = [];
      for (const url of urls) {
         const text = out.get(url) ?? args.readText(url);
         if (text === undefined) return { declined: "a file it cannot read" };
         const read = readThroughDestination(text, move);
         if (!read) {
            return {
               declined: `connection '${move.connection}' is named other than in a plain .table() it can place`,
            };
         }
         if (read.paths.length === 0) continue;
         paths.push(...read.paths);
         out.set(url, read.text);
      }
      for (const path of paths) {
         const refused = await managedTablePathRefusal(
            path,
            move.connection,
            move.schemas,
         );
         if (refused !== undefined) {
            return {
               declined: `table '${path}' is outside the connection's scope`,
            };
         }
      }
      const tables = [...new Set(paths)].map((path) => {
         const [catalog, schema, table] = destinationTablePath(
            path,
            move,
         )!.split(".");
         return [catalog, schema, table] as const;
      });
      if (!(await args.ports.baseTables(move.alias, tables))) {
         return { declined: "a table it reads is not a base table" };
      }
   }
   return {
      files: out,
      destinations: new Set(moves.map((m) => m.destination)),
   };
}
