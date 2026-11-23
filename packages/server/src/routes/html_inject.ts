// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Splices a snippet of ASCII markup into an HTML document's bytes. The bytes
 * are never decoded, since the snippet is ASCII and any charset reads it the
 * same; a latin1 view keeps one character per byte for scanning the start.
 */

/** Past this size a page is not worth buffering to splice; it is sent as is. */
export const MAX_INJECTABLE_HTML_BYTES = 2 * 1024 * 1024;

/** How far into a document the doctype is looked for; only a BOM, blanks and comments may come before it. */
const SCAN_BYTES = 64 * 1024;

const DOCTYPE = /<!doctype[^>]{0,1024}>/iy;
const UTF8_BOM = Buffer.from([0xef, 0xbb, 0xbf]);

const isBlank = (code: number) =>
   code === 0x20 ||
   code === 0x09 ||
   code === 0x0a ||
   code === 0x0c ||
   code === 0x0d;

/** Where the doctype ends, or undefined when the document has none to open it. */
function doctypeEnd(text: string, from: number): number | undefined {
   let i = from;
   for (;;) {
      while (isBlank(text.charCodeAt(i))) i++;
      if (!text.startsWith("<!--", i)) break;
      const close = text.indexOf("-->", i + 2);
      if (close === -1) return undefined;
      i = close + 3;
   }
   DOCTYPE.lastIndex = i;
   const m = DOCTYPE.exec(text);
   return m ? i + m[0].length : undefined;
}

/**
 * `html` with `snippet` inserted, or undefined for a UTF-16 byte-order mark
 * (its bytes cannot be scanned as ASCII). The snippet goes right after the
 * doctype, so the page stays out of quirks mode; a document with none gets it
 * at the start, past a UTF-8 byte-order mark. Before `<html>` or `<head>` is
 * fine: the parser puts a script there into the head.
 */
export function injectAfterDoctype(
   html: Buffer,
   snippet: string,
): Buffer | undefined {
   if (isUtf16(html)) return undefined;
   const start = html.subarray(0, UTF8_BOM.length).equals(UTF8_BOM)
      ? UTF8_BOM.length
      : 0;
   const text = html.subarray(0, SCAN_BYTES).toString("latin1");
   const at = doctypeEnd(text, start) ?? start;
   return Buffer.concat([
      html.subarray(0, at),
      Buffer.from(snippet, "latin1"),
      html.subarray(at),
   ]);
}

function isUtf16(html: Buffer): boolean {
   return (
      html.length >= 2 &&
      ((html[0] === 0xff && html[1] === 0xfe) ||
         (html[0] === 0xfe && html[1] === 0xff))
   );
}
