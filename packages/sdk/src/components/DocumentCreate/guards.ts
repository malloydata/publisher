// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { annotationTextProblem } from "../DashboardBuilder/annotationText";
import { isBareName } from "../../utils/malloyText";
import type { DocumentType } from "../DocumentStorage";

export const malloyName = (name: string) =>
   isBareName(name) ? name : `\`${name}\``;

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
   const textProblem = annotationTextProblem("title", title);
   if (textProblem) return textProblem;
   if (kind === "notebook" && title.includes("|##"))
      return "A title cannot contain `|##`, which closes a notebook's text cell.";
   if (/["\\\r\n]/.test(modelPath) || modelPath.trim() === "")
      return `The model path ${JSON.stringify(modelPath)} cannot be written as an import.`;
   // A dashboard writes both names bare; a notebook back-quotes what is not an identifier.
   for (const [what, name] of [
      ["source", source],
      ["view", view],
   ] as const)
      if (
         name.trim() === "" ||
         /[`\r\n\\]/.test(name) ||
         (kind === "dashboard" && !isBareName(name))
      )
         return `The ${what} name ${JSON.stringify(name)} cannot be written as a Malloy name.`;
   return undefined;
}
