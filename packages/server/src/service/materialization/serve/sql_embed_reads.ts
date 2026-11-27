// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which stored sources a served copy cannot re-root because a `.sql()`
 * source reads them through `%{ … }` on another connection.
 *
 * `conn.sql("""select … from %{ x -> { select: * } }""")` compiles `x`'s
 * query into the SQL text, which runs on `conn`. The served copy re-roots `x`
 * on its table: in its own database that is `conn` still, and the embed reads
 * the table. In a storage destination (or a managed catalog's, read through
 * the destination) it is the destination's alias, and Malloy refuses a `%{}`
 * whose query is on another connection than its SQL ("Query is on connection
 * …, but this SQL is for …"), which fails the whole copy. So such a source is
 * left as written, alone, with its own reason, and the rest of the copy still
 * compiles.
 *
 * What an embed reads is read as the load's gate check reads it
 * (`sql_embed_gates.ts`), and followed as the reach walk follows a source
 * (`reachableSourceIDs`): its base, its joins, its pipeline.
 *
 * Pure.
 */

import type { ModelDef } from "@malloydata/malloy";
import { labelOf, sqlSelectsWithin } from "../../governance/ir/sql_embeds";
import { reachableSourceIDs } from "../text/source_map";

/** One `.sql()` whose text embeds stored sources. */
interface SqlEmbedRead {
   /** The `.sql()` source, as the author named it (or `<inline sql()>`). */
   via: string;
   /** The connection its SQL runs on. */
   connection: string;
   /** The `.sql()` source's own `sourceID`, when it has one. */
   sourceID?: string;
   /** Every `sourceID` its query embeds (`%{ x -> { … } }`) read. */
   reaches: ReadonlySet<string>;
   /**
    * Every `sourceID` its bare embeds (`%{ x }`) read. Malloy embeds a
    * source bare only when it is persistable (a table, SQL or query source),
    * which a re-root, a `virtual()` source, is not.
    */
   bareReaches: ReadonlySet<string>;
}

/** Why a re-root cannot be compiled under a `.sql()` that reads it. */
export type SqlEmbedConflict = "cross_connection" | "whole_source";

/**
 * Every `.sql()` with a `%{}` in `modelDefs` (a model and the models of the
 * files it imports, which its copy compiles whole), with what its embeds
 * read. One `.sql()` seen in several models is listed once.
 */
export function sqlEmbedReads(modelDefs: readonly unknown[]): SqlEmbedRead[] {
   const out = new Map<string, SqlEmbedRead>();
   for (const modelDef of modelDefs) {
      for (const select of sqlSelectsWithin(modelDef)) {
         const connection = select.connection;
         if (typeof connection !== "string") continue;
         const segments = (select.selectSegments as unknown[]).filter(
            (s) =>
               typeof s === "object" &&
               s !== null &&
               !("sql" in s && (s as { type?: unknown }).type === undefined),
         );
         if (segments.length === 0) continue;
         const isQuery = (s: unknown) =>
            (s as { type?: unknown }).type === "query";
         const read: SqlEmbedRead = {
            via: labelOf(select),
            connection,
            ...(typeof select.sourceID === "string"
               ? { sourceID: select.sourceID }
               : {}),
            reaches: reachableSourceIDs(
               segments.filter(isQuery),
               modelDef as ModelDef,
            ),
            bareReaches: reachableSourceIDs(
               segments.filter((s) => !isQuery(s)),
               modelDef as ModelDef,
            ),
         };
         // An extension of a `.sql()` source carries its SQL again.
         const key = [
            read.via,
            connection,
            [...read.reaches].sort().join(","),
            [...read.bareReaches].sort().join(","),
         ].join("\u0000");
         if (!out.has(key)) out.set(key, read);
      }
   }
   return [...out.values()];
}

/**
 * The `.sql()` sources whose text the copy cannot compile with `sourceID`
 * re-rooted on a connection `servedOn`, each with why: a bare `%{ x }` of
 * it (`whole_source`, on any connection), or a `%{ x -> { … } }` whose SQL
 * runs on another connection than `servedOn` (`cross_connection`). Empty
 * when none. A `.sql()` source the copy re-roots itself (`rerooted`, by its
 * `sourceID`, or one it is built from) is replaced by its table, so its text
 * is not compiled and it is not counted.
 */
export function conflictingSqlEmbeds(
   reads: readonly SqlEmbedRead[],
   sourceID: string,
   servedOn: string,
   rerooted: (sqlSourceID: string) => boolean,
): { read: SqlEmbedRead; conflict: SqlEmbedConflict }[] {
   const out: { read: SqlEmbedRead; conflict: SqlEmbedConflict }[] = [];
   for (const read of reads) {
      if (read.sourceID !== undefined && rerooted(read.sourceID)) continue;
      if (read.bareReaches.has(sourceID)) {
         out.push({ read, conflict: "whole_source" });
      } else if (read.connection !== servedOn && read.reaches.has(sourceID)) {
         out.push({ read, conflict: "cross_connection" });
      }
   }
   return out;
}

/**
 * Every `sourceID` the queried source's own `%{}` embeds read, when it is a
 * `.sql()` source (or an extension of one); empty otherwise. The tables
 * these name are the ones a served union reads in its own place.
 */
export function ownSqlEmbedReach(
   struct: unknown,
   modelDef: unknown,
): ReadonlySet<string> {
   const select = struct as { type?: unknown; selectSegments?: unknown };
   if (select?.type !== "sql_select" || !Array.isArray(select.selectSegments)) {
      return new Set();
   }
   return reachableSourceIDs(
      select.selectSegments.filter(
         (s: unknown) =>
            typeof s === "object" &&
            s !== null &&
            !("sql" in s && (s as { type?: unknown }).type === undefined),
      ),
      modelDef as ModelDef,
   );
}
