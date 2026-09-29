// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NotebookCell } from "../../client";

/**
 * Whether the viewer sends a cell to the server to run. A served notebook's
 * definition cell is `type: code` too, so `type` alone would execute it.
 */
export function cellRuns(cell: Pick<NotebookCell, "type" | "kind">): boolean {
   return cell.kind ? cell.kind === "query" : cell.type === "code";
}

/**
 * A query cell's caption: the `#"` lines in the tag block above its `run:`.
 * The API carries no caption field, so the cell text is the source of truth.
 */
export function cellCaption(text: string | undefined): string | undefined {
   const lines: string[] = [];
   for (const raw of (text ?? "").split("\n")) {
      const line = raw.trim();
      if (line.startsWith("//")) continue;
      if (!line.startsWith("#")) break;
      if (line.startsWith('#"')) lines.push(line.slice(2).trim());
   }
   return lines.length > 0 ? lines.join(" ") : undefined;
}

/** One-line label for a folded definition cell: its statement kind and name. */
export function definitionSummary(text: string | undefined): string {
   const line =
      (text ?? "")
         .split("\n")
         .map((raw) => raw.trim())
         .find((l) => l && !l.startsWith("#") && !l.startsWith("//")) ?? "";
   const named = /^(source|query|given|type):?\s+([A-Za-z_]\w*)/.exec(line);
   if (named) return `${named[1]}: ${named[2]}`;
   const keyword = /^(import|export)\b/.exec(line);
   return keyword ? keyword[1] : line;
}
