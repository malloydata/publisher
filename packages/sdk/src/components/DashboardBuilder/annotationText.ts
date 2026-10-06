// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** A copy of the server's `AUTHORIZE_TAG_LIKE`, kept in step by a parity spec; the server's caller guard reads it anywhere outside markdown and text prose, and a tag value or `#"` caption is not prose. */
export const AUTHORIZE_TAG_LIKE = String.raw`##?\|?[ \t]*(?:[([{<][ \t]*)?(?:(?:(?:row|source)[-_]?)?authorize|access[-_]?filter)(?=[)\]}>]|[ \t]|$)`;

/** Why `text` cannot be written into a tag value or a `#"` note (`what` names it), or undefined when it can. */
export function annotationTextProblem(
   what: string,
   text: string,
): string | undefined {
   if (/[\r\n]/.test(text))
      return `A ${what} is one line; it cannot hold a line break.`;
   if (new RegExp(AUTHORIZE_TAG_LIKE, "iu").test(text))
      return `A ${what} cannot contain what reads as an access-control tag (authorize, row_authorize, source_authorize or access_filter).`;
   return undefined;
}
