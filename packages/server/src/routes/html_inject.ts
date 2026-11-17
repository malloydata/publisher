// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Splices a snippet of ASCII markup into an HTML document's bytes. Bytes, not
 * text: the page's charset is whatever its own meta says, so the file is never
 * decoded, and a latin1 view keeps one character per byte for scanning.
 */

/** Past this size a page is not worth buffering to splice; it is sent as is. */
export const MAX_INJECTABLE_HTML_BYTES = 2 * 1024 * 1024;

/** How far into a document a browser looks for its charset declaration. */
const CHARSET_PRESCAN_BYTES = 1024;

// Bounded attribute spans: a file of unclosed `<meta ` tags must not scan quadratically.
const CHARSET_META =
   /<meta\b[^>]{0,512}?(?:\bcharset\s*=|\bhttp-equiv\s*=\s*["']?content-type)[^>]{0,512}>/gi;
const HEAD_OPEN = /<head(?=[\s/>])[^>]{0,1024}>/gi;
const DOCTYPE = /<!doctype[^>]{0,1024}>/gi;
const COMMENT = /<!--[\s\S]*?(?:-->|$)/g;

/**
 * `html` with `snippet` inserted, or undefined when the document is left
 * alone (a UTF-16 byte-order mark: its bytes cannot be scanned as ASCII).
 *
 * The snippet goes after the charset meta when one sits in the first 1024
 * bytes (so the meta stays inside the prescan window), else after `<head>`,
 * else after the doctype, else at the start past any UTF-8 byte-order mark.
 * A tag inside an HTML comment is not a tag.
 */
export function injectAfterHeadStart(
   html: Buffer,
   snippet: string,
): Buffer | undefined {
   if (isUtf16(html)) return undefined;
   const text = html.toString("latin1");
   const comments = [...text.matchAll(COMMENT)].map(
      (m) => [m.index, m.index + m[0].length] as const,
   );
   const live = (index: number) =>
      !comments.some(([start, end]) => index >= start && index < end);
   const endOfFirst = (pattern: RegExp, scanned = text) => {
      for (const m of scanned.matchAll(pattern)) {
         if (live(m.index)) return m.index + m[0].length;
      }
      return undefined;
   };
   const at =
      endOfFirst(CHARSET_META, text.slice(0, CHARSET_PRESCAN_BYTES)) ??
      endOfFirst(HEAD_OPEN) ??
      endOfFirst(DOCTYPE) ??
      (html.subarray(0, 3).equals(UTF8_BOM) ? UTF8_BOM.length : 0);
   return Buffer.concat([
      html.subarray(0, at),
      Buffer.from(snippet, "latin1"),
      html.subarray(at),
   ]);
}

const UTF8_BOM = Buffer.from([0xef, 0xbb, 0xbf]);

function isUtf16(html: Buffer): boolean {
   return (
      html.length >= 2 &&
      ((html[0] === 0xff && html[1] === 0xfe) ||
         (html[0] === 0xfe && html[1] === 0xff))
   );
}
