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
   /** Every `sourceID` its embeds read. */
   reaches: ReadonlySet<string>;
}

/** Every `.sql()` in `modelDef` with a `%{}`, with what its embeds read. */
export function sqlEmbedReads(modelDef: unknown): SqlEmbedRead[] {
   const out: SqlEmbedRead[] = [];
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
      out.push({
         via: labelOf(select),
         connection,
         ...(typeof select.sourceID === "string"
            ? { sourceID: select.sourceID }
            : {}),
         reaches: reachableSourceIDs(segments, modelDef as ModelDef),
      });
   }
   return out;
}

/**
 * The `.sql()` sources that read `sourceID` through a `%{}` on another
 * connection than `servedOn`, the connection its re-root reads it on; empty
 * when none does. A `.sql()` source the copy re-roots itself (`rerooted`, by
 * `sourceID`, its own or one it is built from) is replaced by its table, so
 * its text is not compiled and it is not counted.
 */
export function crossingSqlEmbeds(
   reads: readonly SqlEmbedRead[],
   sourceID: string,
   servedOn: string,
   rerooted: (sqlSourceID: string) => boolean,
): SqlEmbedRead[] {
   return reads.filter(
      (r) =>
         r.connection !== servedOn &&
         r.reaches.has(sourceID) &&
         !(r.sourceID !== undefined && rerooted(r.sourceID)),
   );
}
