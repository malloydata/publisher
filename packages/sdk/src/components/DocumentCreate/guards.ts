// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { isIdentifier } from "../DashboardBuilder/malloyText";
import type { DocumentType } from "../DocumentStorage";

/** A copy of the server's `AUTHORIZE_TAG_LIKE`, kept in step by a parity spec; a title lands in a tag the server's caller guard reads anywhere. */
export const AUTHORIZE_TAG_LIKE = String.raw`##?\|?[ \t]*(?:[([{<][ \t]*)?(?:(?:(?:row|source)[-_]?)?authorize|access[-_]?filter)(?=[)\]}>]|[ \t]|$)`;

export const malloyName = (name: string) =>
   isIdentifier(name) ? name : `\`${name}\``;

/** What a new document is made from: a view of a source in a model. */
export interface NewDocument {
   title: string;
   /** The model that declares the source, relative to the package root. */
   modelPath: string;
   source: string;
   view: string;
}

/** Why `input` cannot be written into a new `kind`, or undefined when it can. */
export function newDocumentProblem(
   kind: DocumentType,
   input: NewDocument,
): string | undefined {
   const { title, modelPath, source, view } = input;
   if (title.trim() === "") return "A title cannot be empty.";
   if (/[\r\n]/.test(title))
      return "A title is one line; it cannot hold a line break.";
   if (new RegExp(AUTHORIZE_TAG_LIKE, "iu").test(title))
      return "A title cannot contain what reads as an access-control tag (authorize, row_authorize, source_authorize or access_filter).";
   if (kind === "notebook" && title.includes("|##"))
      return "A title cannot contain `|##`, which closes a notebook's text cell.";
   if (/["\\\r\n]/.test(modelPath) || modelPath.trim() === "")
      return `The model path ${JSON.stringify(modelPath)} cannot be written as an import.`;
   for (const [what, name] of [
      ["source", source],
      ["view", view],
   ] as const)
      if (name.trim() === "" || /[`\r\n\\]/.test(name))
         return `The ${what} name ${JSON.stringify(name)} cannot be written as a Malloy name.`;
   return undefined;
}
