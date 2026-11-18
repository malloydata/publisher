// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Splices a snippet of ASCII markup into an HTML document's bytes. Bytes, not
 * text: the page's charset is whatever its own meta says, so the file is never
 * decoded, and a latin1 view keeps one character per byte for scanning.
 */

/** Past this size a page is not worth buffering to splice; it is sent as is. */
export const MAX_INJECTABLE_HTML_BYTES = 2 * 1024 * 1024;

/** How much of a document is scanned for an insertion point; a page's head sits well inside it. */
const SCAN_BYTES = 64 * 1024;

/** How far into a document a browser looks for its charset declaration. */
const CHARSET_PRESCAN_BYTES = 1024;

// Bounded attribute spans, sticky so each is tried once at a `<`.
const CHARSET_META =
   /<meta\b[^>]{0,512}?(?:\bcharset\s*=|\bhttp-equiv\s*=\s*["']?content-type)[^>]{0,512}>/iy;
const HEAD_OPEN = /<head(?=[\s/>])[^>]{0,1024}>/iy;
const DOCTYPE = /<!doctype[^>]{0,1024}>/iy;

// Elements whose content is text, never markup.
const RAW_TEXT_ELEMENTS = [
   "script",
   "style",
   "title",
   "textarea",
   "noscript",
   "noembed",
   "noframes",
   "iframe",
   "xmp",
];
const RAW_TEXT_OPEN = new RegExp(
   `<(${RAW_TEXT_ELEMENTS.join("|")}|plaintext)(?=[\\s/>])`,
   "iy",
);
const RAW_TEXT_CLOSE = new Map(
   RAW_TEXT_ELEMENTS.map((name) => [
      name,
      new RegExp(`</${name}(?=[\\s/>])[^>]{0,1024}>`, "gi"),
   ]),
);

/** How far an open tag is followed to its closing `>`, quoted attribute values included. */
const MAX_TAG_BYTES = 2048;

/**
 * Where the text that is not markup, starting at the `<` at `at`, ends: an HTML
 * comment, or a script, style, title, textarea, noscript, noembed, noframes,
 * iframe or xmp element through its closing tag, or a plaintext element to the end. One that
 * is never closed runs to the end of `text`. Undefined when `at`
 * starts neither. A forward-only scan, so a caller that continues from the
 * returned index never reads a character twice.
 */
export function nonTagTextEnd(text: string, at: number): number | undefined {
   if (text.startsWith("<!--", at)) {
      const close = text.indexOf("-->", at + 2);
      return close === -1 ? text.length : close + 3;
   }
   RAW_TEXT_OPEN.lastIndex = at;
   const open = RAW_TEXT_OPEN.exec(text);
   if (!open) return undefined;
   const closer = RAW_TEXT_CLOSE.get(open[1].toLowerCase());
   if (!closer) return text.length;
   closer.lastIndex = at + open[0].length;
   const close = closer.exec(text);
   return close ? close.index + close[0].length : text.length;
}

/**
 * Where the open tag at the `<` at `at` ends, with quoted attribute values
 * honored (one opens only right after `=`), so a `>` or `<` inside one is not
 * taken for markup. Undefined when `at` starts no tag. `closed` is false when
 * the scan gave up (no `>` or an unpaired quote within the bound): `end` is
 * then where it stopped, which a caller resumes from, so a run of such tags is
 * scanned once, not once per `<`.
 */
export function openTagEnd(
   text: string,
   at: number,
): { end: number; closed: boolean } | undefined {
   if (!/[a-z]/i.test(text[at + 1] ?? "")) return undefined;
   const limit = Math.min(text.length, at + MAX_TAG_BYTES);
   const span = text.slice(at + 1, limit);
   // HTML whitespace only; `\s` would also take \v and \xA0.
   const stop = /=[\t\n\f\r ]*(["'])|>/g;
   // lastIndex only moves forward within the capped span, so this stays linear in it.
   for (let m = stop.exec(span); m; m = stop.exec(span)) {
      if (m[0] === ">") return { end: at + 1 + m.index + 1, closed: true };
      const close = span.indexOf(m[1], stop.lastIndex);
      if (close === -1) break;
      stop.lastIndex = close + 1;
   }
   return { end: limit, closed: false };
}

function stickyEnd(pattern: RegExp, text: string, at: number) {
   pattern.lastIndex = at;
   const m = pattern.exec(text);
   return m ? at + m[0].length : undefined;
}

/**
 * `html` with `snippet` inserted, or undefined when the document is left
 * alone: a UTF-16 byte-order mark (its bytes cannot be scanned as ASCII), or
 * no insertion point in its first 64 KiB.
 *
 * The snippet goes after the charset meta when one ends in the first 1024
 * bytes (so the meta stays inside the prescan window), else after `<head>`,
 * else after the doctype, else, for a document that fits the scan, at the
 * start past any UTF-8 byte-order mark. Text that is not markup (comments,
 * and the other raw-text elements) holds no tag, nor does a quoted attribute
 * value; a tag ends at its real `>`, not at one inside a quoted value.
 */
export function injectAfterHeadStart(
   html: Buffer,
   snippet: string,
): Buffer | undefined {
   if (isUtf16(html)) return undefined;
   const text = html.subarray(0, SCAN_BYTES).toString("latin1");
   let charset: number | undefined;
   let head: number | undefined;
   let doctype: number | undefined;
   let i = text.indexOf("<");
   while (
      i !== -1 &&
      charset === undefined &&
      !(i >= CHARSET_PRESCAN_BYTES && head !== undefined)
   ) {
      const skipped = nonTagTextEnd(text, i);
      if (skipped !== undefined) {
         i = text.indexOf("<", skipped);
         continue;
      }
      const tag = openTagEnd(text, i);
      // The regexes end at the first `>`; the tag's own end can be later, or never come.
      const realEnd = (matched: number | undefined) =>
         matched === undefined || !tag
            ? matched
            : tag.closed
              ? tag.end
              : undefined;
      if (i < CHARSET_PRESCAN_BYTES) {
         const metaEnd = realEnd(stickyEnd(CHARSET_META, text, i));
         if (metaEnd !== undefined && metaEnd <= CHARSET_PRESCAN_BYTES) {
            charset = metaEnd;
         }
      }
      head ??= realEnd(stickyEnd(HEAD_OPEN, text, i));
      doctype ??= realEnd(stickyEnd(DOCTYPE, text, i));
      i = text.indexOf("<", tag?.end ?? i + 1);
   }
   const at =
      charset ??
      head ??
      doctype ??
      (html.length <= SCAN_BYTES
         ? html.subarray(0, 3).equals(UTF8_BOM)
            ? UTF8_BOM.length
            : 0
         : undefined);
   if (at === undefined) return undefined;
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
