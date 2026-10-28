// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { rangeText, type ParsedModelText } from "./modelTextParse";

/**
 * A model-level named query's declaration (`query: name is …`), read from the
 * model file with Malloy's parser. The compiled model lists a query's name and
 * annotations only, never its definition (see ExplorerQuery), so the file is
 * the one place its code exists; this is what a query row's Code shows.
 *
 * Undefined where the file isn't available, doesn't declare the name, or
 * declares it twice: the row's Code then says it has nothing to show, never a
 * guess.
 */
export function namedQueryCode(
   parsed: ParsedModelText | undefined,
   name: string,
): string | undefined {
   if (!parsed) return undefined;
   const declared = parsed.symbols.filter(
      (s) => s.type === "query" && s.name.replace(/^`(.*)`$/, "$1") === name,
   );
   if (declared.length !== 1) return undefined;
   return `query: ${rangeText(parsed.lines, declared[0].range).trimEnd()}`;
}
