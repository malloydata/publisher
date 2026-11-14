// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the `.malloynb` notebook format (cells delimited by
 * `>>>markdown` and `>>>malloy`), read but never authored. A `.malloy` report
 * replaces it. The format's own parsing lives here so its removal starts here.
 */

import { MalloySQLParser } from "@malloydata/malloy-sql";
import { NOTEBOOK_FILE_SUFFIX } from "../../constants";

/** Whether a package path is a `.malloynb`. */
export function isMalloynbPath(modelPath: string): boolean {
   return modelPath.endsWith(NOTEBOOK_FILE_SUFFIX);
}

/**
 * Split a `.malloynb` into its statements. The cell parser splits on "\n"
 * alone, so a CRLF file (any Windows checkout) is normalized first; otherwise
 * each cell's text would start with a stray line break.
 */
export function parseMalloynb(
   fileContents: string,
   modelPath: string,
): ReturnType<typeof MalloySQLParser.parse> {
   return MalloySQLParser.parse(fileContents.replace(/\r\n/g, "\n"), modelPath);
}
