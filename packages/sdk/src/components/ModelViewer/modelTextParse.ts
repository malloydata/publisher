// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useMemo, useState } from "react";
import type { DocumentRange, DocumentSymbol } from "@malloydata/malloy";

type MalloyModule = typeof import("@malloydata/malloy");

/**
 * A model file's text with the symbols Malloy's parser finds in it: each
 * source, its fields, views and joins, and the file's named queries, each with
 * the range of text that declares it. Parsing does not compile, so a file
 * mid-edit still yields what the parser could read.
 */
export interface ParsedModelText {
   /** The file's lines, CRLF read as LF, which the symbols' ranges index. */
   lines: string[];
   symbols: DocumentSymbol[];
}

// The last text parsed: a page's views and its named queries read the same file.
let lastParse:
   | { text: string; parsed: ParsedModelText | undefined }
   | undefined;

/** Undefined when the parser throws: the rows then say they have no code. */
export function parseModelText(
   malloy: MalloyModule,
   text: string,
): ParsedModelText | undefined {
   if (lastParse?.text === text) return lastParse.parsed;
   const source = text.replace(/\r\n/g, "\n");
   let parsed: ParsedModelText | undefined;
   try {
      parsed = {
         lines: source.split("\n"),
         symbols: malloy.Malloy.parse({ source }).symbols,
      };
   } catch {
      parsed = undefined;
   }
   lastParse = { text, parsed };
   return parsed;
}

/** The text a range covers. */
export function rangeText(
   lines: readonly string[],
   range: DocumentRange,
): string {
   const { start, end } = range;
   if (start.line === end.line)
      return (lines[start.line] ?? "").slice(start.character, end.character);
   return [
      (lines[start.line] ?? "").slice(start.character),
      ...lines.slice(start.line + 1, end.line),
      (lines[end.line] ?? "").slice(0, end.character),
   ].join("\n");
}

let loaded: MalloyModule | undefined;
let loading: Promise<MalloyModule> | undefined;

/**
 * Malloy's parser is about 450 KB gzipped. The Publisher SDK already loads it
 * lazily, into a chunk of its own, so importing it the same way here shares
 * that chunk and only a page that needs a declaration from the text fetches it.
 */
function loadMalloy(): Promise<MalloyModule> {
   loading ??= import("@malloydata/malloy").then((ns) => {
      // A CommonJS module's exports can arrive under `default`, depending on the bundler.
      const mod = (
         "Malloy" in ns ? ns : (ns as { default: MalloyModule }).default
      ) as MalloyModule;
      loaded = mod;
      return mod;
   });
   loading.catch(() => {
      loading = undefined;
   });
   return loading;
}

/**
 * `text` parsed, once the parser has loaded. `pending` while it loads; when
 * `wanted` is false nothing is loaded or parsed. A parser that fails to load
 * leaves `parsed` undefined and `pending` false.
 */
export function useParsedModelText(
   text: string | undefined,
   wanted = true,
): { parsed: ParsedModelText | undefined; pending: boolean } {
   const [malloy, setMalloy] = useState<MalloyModule | undefined>(loaded);
   const [failed, setFailed] = useState(false);
   const need = !!text && wanted;
   useEffect(() => {
      if (!need || malloy) return;
      let live = true;
      loadMalloy().then(
         (mod) => live && setMalloy(mod),
         () => live && setFailed(true),
      );
      return () => {
         live = false;
      };
   }, [need, malloy]);
   const parsed = useMemo(
      () => (need && malloy && text ? parseModelText(malloy, text) : undefined),
      [need, malloy, text],
   );
   return { parsed, pending: need && !malloy && !failed };
}
