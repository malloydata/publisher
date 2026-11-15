// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The typed source a persisted result's `storage=` table is read through,
 * shared by the view and query legs: a `type:` with one field per stored
 * column, a source over the table's `virtual()` handle cast to it, and the
 * timeframe and cast restatements its columns need to read as live's.
 *
 * Each leg writes its own last line: the view leg puts the restatements on
 * the redeclared source beside the view, the query leg on the stored source.
 *
 * Pure.
 */

import { identifier, stringLiteral } from "../../malloy_text";
import type { Binding } from "../domain/binding";
import { timeframeRestatements, typeDeclaration } from "./served_copy";
import { servedCopyAlias } from "./served_copy_config";

/** How a stored column is read: through a faithful cast, or a big integer's native type. */
export type ColumnCast =
   | { castTo: "DATE" | "BOOLEAN" }
   | { nativeCast: string };

/** One output column of the result, as the leg read it from live. */
interface ResultColumn {
   name: string;
   timeframe?: string;
   notes: string[];
   kind: "measure" | "dimension" | "nest";
}

/**
 * `typeText`: the `type:` and `source: <stored> is <destination>.virtual(…)::
 * <type>` lines; `timeframes`: the restatements to write in an `extend`.
 *
 * A stored column carries the notes live's output column does (its field's,
 * and the result's own on it): a `type:` field keeps its annotations, so the
 * result column reads them as live's does. A timeframe column's and a cast
 * column's go on its redefined field (the restatements) instead, and a
 * measure's on the result's re-aggregation of it.
 */
export function storedTypedSource(args: {
   columns: readonly ResultColumn[];
   casts: ReadonlyMap<string, ColumnCast>;
   binding: Pick<Binding, "schema" | "destinationName" | "virtualHandle">;
   typeName: string;
   stored: string;
}): { typeText: string; timeframes: string } {
   const { columns, casts, binding, typeName, stored } = args;
   const columnNotes = new Map(
      columns.map((c) => [
         c.name,
         c.timeframe || casts.has(c.name) || c.kind === "measure"
            ? []
            : c.notes,
      ]),
   );
   const typed = binding.schema.map((c) => {
      const notes = columnNotes.get(c.name);
      return {
         name: c.name,
         type: c.type,
         ...(notes && notes.length > 0 ? { notes } : {}),
      };
   });
   const timeframes = timeframeRestatements(
      columns.map((c) => {
         const cast = casts.get(c.name);
         if (!cast) return c;
         // A measure's notes go on its re-aggregation.
         const notes = c.kind === "measure" ? [] : c.notes;
         return { ...c, notes, ...cast };
      }),
      binding.schema.map((c) => c.name),
   );
   // malloy-text: the type emitter and names through identifier
   const typeText = `${typeDeclaration({ typeName, columns: typed })}
source: ${identifier(stored)} is ${identifier(servedCopyAlias(binding.destinationName))}.virtual(${stringLiteral(binding.virtualHandle)})::${identifier(typeName)}`;
   return { typeText, timeframes };
}
