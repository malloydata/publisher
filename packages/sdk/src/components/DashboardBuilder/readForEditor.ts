// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument } from "./document";
import { conversionRefused, convertLegacyNotebook } from "./legacyNotebook";
import { readDashboardDocument, readFailed } from "./readDocument";

/** A file opened for editing: its document, and the conversion it is the result of when the file is a cell-format notebook. */
export type EditorOpen =
   | {
        ok: true;
        document: DashboardDocument;
        /** `from` is the text on disk, `to` the layout text it converts to. */
        conversion?: { from: string; to: string };
     }
   | { ok: false; reason: string };

const withLine = (reason: string, line: number | undefined) =>
   line !== undefined && !/\bline \d+/i.test(reason)
      ? `${reason} (line ${line})`
      : reason;

/** Read `text` for the builder, converting a cell-format notebook rather than refusing it. */
export async function readForEditor(
   text: string,
   modelPath?: string,
   textHeld = false,
): Promise<EditorOpen> {
   const read = await readDashboardDocument(text, modelPath, textHeld);
   if (!readFailed(read)) return { ok: true, document: read.document };
   if (!read.legacyNotebook)
      return { ok: false, reason: withLine(read.reason, read.line) };

   const converted = await convertLegacyNotebook(text);
   if (conversionRefused(converted))
      return { ok: false, reason: withLine(converted.refused, converted.line) };
   const reread = await readDashboardDocument(
      converted.text,
      modelPath,
      textHeld,
   );
   if (readFailed(reread))
      return {
         ok: false,
         reason: `The converted notebook cannot be opened: ${withLine(reread.reason, reread.line)}`,
      };
   return {
      ok: true,
      document: reread.document,
      conversion: { from: text, to: converted.text },
   };
}
