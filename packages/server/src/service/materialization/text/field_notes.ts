// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A compiled field's annotation lines, read from its definition and written
 * again on a field Publisher generates (a `type:` column, a timeframe
 * restatement, a rollup's merged measure), so the generated field's result
 * column carries what live's does. Neither plan nor serve owns it: both
 * generate fields from the author's.
 *
 * Pure.
 */

/**
 * The annotation lines on a compiled field, outermost first: what it inherits,
 * then its statement's (block) notes, then its own. Each is the author's text
 * (`#(doc) "…"`), so it can be written again on the field in a `type:`.
 */
export function fieldNotes(field: unknown): string[] {
   interface Annotation {
      inherits?: Annotation;
      blockNotes?: { text?: unknown }[];
      notes?: { text?: unknown }[];
   }
   const out: string[] = [];
   const walk = (a: Annotation | undefined, depth: number) => {
      if (!a || depth > 16) return;
      walk(a.inherits, depth + 1);
      for (const n of [...(a.blockNotes ?? []), ...(a.notes ?? [])]) {
         if (typeof n.text === "string" && !out.includes(n.text)) {
            out.push(n.text);
         }
      }
   };
   const f = field as
      | { annotations?: Annotation; annotation?: Annotation }
      | undefined;
   walk(f?.annotations ?? f?.annotation, 0);
   return out;
}

/**
 * One note as Malloy source: its lines, the first at `indent`, each ending in
 * a newline, so it is placed on its own lines before the field it annotates.
 *
 * A multi-line note (`#|`…`|#`) is held without its closer and with its body
 * dedented, so written back as held it would leave the annotation open and
 * swallow the rest of the file. Its body is written one level deeper than
 * the opener and its closer at the opener's column: the compiler strips the
 * body's common indent again, so the note reads back as the same text.
 */
export function noteSource(note: string, indent: string): string {
   const text = note.replace(/\r?\n$/, "");
   if (!text.startsWith("#|")) return `${indent}${text.trimEnd()}\n`;
   const [opener, ...body] = text.split("\n");
   const bodyIndent = `${indent}  `;
   const lines = body.map((line) =>
      line === "" ? "\n" : `${bodyIndent}${line}\n`,
   );
   return `${indent}${opener}\n${lines.join("")}${indent}|#\n`;
}
