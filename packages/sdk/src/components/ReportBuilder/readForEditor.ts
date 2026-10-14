// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DashboardDocument } from "./document";
import { readDashboardDocument, readFailed } from "./readDocument";

/** A file opened for editing: its document, or why it cannot be opened. */
export type EditorOpen =
   | { ok: true; document: DashboardDocument }
   | { ok: false; reason: string };

const withLine = (reason: string, line: number | undefined) =>
   line !== undefined && !/\bline \d+/i.test(reason)
      ? `${reason} (line ${line})`
      : reason;

/** Read `text` for the builder, with the line a refusal points at in its reason. */
export async function readForEditor(text: string): Promise<EditorOpen> {
   const read = await readDashboardDocument(text);
   if (!readFailed(read)) return { ok: true, document: read.document };
   return { ok: false, reason: withLine(read.reason, read.line) };
}
