// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NotebookCell } from "../../client";
import type { ApiError } from "../ApiErrorDisplay";

/**
 * Text-source mode: the document is a string the host keeps, and every query
 * it runs is that document's own definitions followed by one `run:`, sent as
 * the caller's text against a model the caller can already read. Nothing is run
 * as the document's author, so each viewer sees what their own identity allows.
 */

/** What a tile says in place of a result the viewer may not read. */
export const RESTRICTED_NOTICE = "You don't have access to this data";

/**
 * The text a tile's `run:` is appended to: the document's definition cells in
 * file order. Never an `import`, a `##!` flag or a `given:`; the model supplies
 * those, and caller text may not carry them.
 */
export function documentPreamble(
   cells: readonly NotebookCell[] | undefined,
): string {
   return (cells ?? [])
      .filter((cell) => cell.kind === "definition" && !cell.restricted)
      .map((cell) => cell.text ?? "")
      .join("\n\n");
}

/** `preamble` then `text`, as one query. */
export function withPreamble(preamble: string, text: string): string {
   return preamble === "" ? text : `${preamble}\n\n${text}`;
}

/** The server refused this viewer the source, as opposed to the query failing. */
export function isForbidden(error: unknown): boolean {
   return (error as ApiError | undefined)?.status === 403;
}
