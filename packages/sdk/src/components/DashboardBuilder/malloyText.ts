// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The textual reading of a dashboard file that the reader and the writer
 * share: where declarations are, and how a tile expression splits.
 *
 * Text, not the parser's symbol tree, because the tree is unreliable inside a
 * source — measured, a refinement spelled `+ { limit: 5, where: … }` makes it
 * report the refined view's base as a child and drop the next declaration —
 * and does not cover `given:` at all. A `<keyword>: <name> is` line under a
 * `source: <name> is` line is unambiguous, and Malloy has no nested sources
 * to confuse it. Both sides reading the same lines the same way is also what
 * lets the writer patch exactly what the reader read.
 */
const IDENT = "[A-Za-z_][A-Za-z0-9_]*";

export const isIdentifier = (text: string) =>
   new RegExp(`^${IDENT}$`).test(text);

/** A one-line `<keyword>: <name> is <rest>` declaration. */
export interface DeclarationAt {
   line: number;
   /** Everything after `is`, trimmed: a base view, an expression, or `{`. */
   rest: string;
}

/**
 * The `<keyword>:` declarations under `source: <owner>`, in file order. The
 * body of a source ends at the next top-level declaration or `##` line.
 */
export function declarationsUnder(
   lines: string[],
   owner: string,
   keyword: "view" | "dimension",
): Map<string, DeclarationAt> {
   const found = new Map<string, DeclarationAt>();
   const declaration = new RegExp(
      `^\\s*${keyword}:\\s*(${IDENT})\\s+is\\b\\s*(.*)$`,
   );
   let current: string | undefined;
   for (let line = 0; line < lines.length; line++) {
      const text = lines[line];
      const source = new RegExp(`^\\s*source:\\s*(${IDENT})\\s+is\\b`).exec(
         text,
      );
      if (source) {
         current = source[1];
         continue;
      }
      if (/^\s*(query|run|import|given)\b/.test(text) || text.startsWith("##"))
         current = undefined;
      if (current !== owner) continue;
      const m = declaration.exec(text);
      if (m) found.set(m[1], { line, rest: m[2].trim() });
   }
   return found;
}

/** The line declaring `<keyword>: <name> is`, or -1. */
export function declarationLine(
   lines: string[],
   keyword: "view" | "source",
   name: string,
): number {
   const re = new RegExp(`^\\s*${keyword}:\\s*${name}\\s+is\\b`);
   return lines.findIndex((l) => re.test(l));
}

/** The line carrying the model-level `## artifact` tag, or -1. */
export const artifactLine = (lines: string[]) =>
   lines.findIndex(
      (l) => l.trimStart().startsWith("##") && l.includes("artifact"),
   );

/** A `given:` declaration: its line, and the block header when it is in one. */
export interface GivenAt {
   line: number;
   blockHeader?: number;
   /** `NAME :: type is default`, as written. */
   declaration: string;
}

/**
 * Every `given:` declaration, by name, in both spellings Malloy accepts:
 *
 *     given: CATEGORY :: filter<string> is f''   // one per line
 *
 *     given:                                     // a block
 *       CATEGORY :: filter<string> is f''
 *       SINCE :: date is @2023-01-01
 */
export function givenDeclarations(lines: string[]): Map<string, GivenAt> {
   const found = new Map<string, GivenAt>();
   const nameOf = (declaration: string) =>
      /^([A-Z_][A-Z0-9_]*)\s*::/.exec(declaration)?.[1];
   const add = (line: number, declaration: string, blockHeader?: number) => {
      const text = declaration.trim();
      const name = nameOf(text);
      if (name)
         found.set(name, {
            line,
            declaration: text,
            ...(blockHeader === undefined ? {} : { blockHeader }),
         });
   };
   for (let i = 0; i < lines.length; i++) {
      const text = lines[i].trim();
      if (text === "given:") {
         for (let j = i + 1; j < lines.length; j++) {
            const inner = lines[j].trim();
            if (
               inner === "" ||
               /^(source|query|import|run|given|##)/.test(inner)
            )
               break;
            add(j, inner, i);
         }
      } else if (text.startsWith("given:")) {
         add(i, text.slice("given:".length));
      }
   }
   return found;
}

/** A tile expression's steps: `orders -> by_brand + { limit: 2 }`. */
export interface TileSteps {
   source: string;
   view: string;
   /** The refinement on the view, `+ { … }`, when the expression carries one. */
   refinement?: string;
}

/**
 * `source -> view`, optionally refined — the one form the builder lays out.
 * Undefined for anything else: an inline stage, or a longer pipeline.
 */
export function tileSteps(
   expression: string | undefined,
): TileSteps | undefined {
   const parts = (expression ?? "").split("->").map((p) => p.trim());
   if (parts.length !== 2) return undefined;
   const plus = parts[1].indexOf("+");
   const view = plus < 0 ? parts[1] : parts[1].slice(0, plus).trim();
   const refinement = plus < 0 ? undefined : parts[1].slice(plus).trim();
   return {
      source: parts[0],
      view,
      ...(refinement === undefined ? {} : { refinement }),
   };
}
