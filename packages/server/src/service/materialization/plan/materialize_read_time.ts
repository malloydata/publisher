// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` source's read-time terms: the given-reading `where:`
 * terms left out of its build and applied again at read, the declaration
 * they are written in and the givens they read, and where each term is
 * written in a file, so a build copy can take it out.
 *
 * Read by the planner (`materialize_plan.ts`) and the build copy
 * (`materialize_build_copy.ts`).
 *
 * Pure.
 */

import type {
   TermUse,
   MalloyRange,
   GivenReadTerm,
} from "../domain/read_time_terms";
import type { SourceDefLike } from "./build_file_generator";
import {
   withinDeclaration,
   type ClassifiedWhereTerm,
} from "./persist_dynamic_terms";
import type {
   MaterializeColumn,
   MaterializeGivenDeclaration,
} from "../domain/table_plans";
import { offsetOf } from "../text/malloy_lexer";

/**
 * Why a given-reading `where:` written anywhere but the materialized source's
 * own declaration refuses the source: only that declaration's trailing
 * `extend` is kept by the served copy, so only there can the build leave the
 * term out and the read re-apply it.
 */
export function givenWhereNotOwnReason(shown: string): string {
   return (
      `${shown} reads a given and is not written on this source, so it ` +
      "would not be re-applied over the stored table at read; the source " +
      "is answered live under it. To serve it from a table, move the " +
      "given-reading `where:` onto the materialized source itself"
   );
}

/** `{ declaration }` when there is one, else nothing. */
export function withDeclaration(declaration: TermUse | undefined): {
   declaration?: TermUse;
} {
   return declaration ? { declaration } : {};
}

/** A compiled declaration's location, or undefined when it has none. */
export function declarationOf(def: SourceDefLike): TermUse | undefined {
   const location = def.location as
      | { url?: unknown; range?: MalloyRange }
      | undefined;
   if (
      typeof location?.url !== "string" ||
      typeof location.range?.start?.line !== "number" ||
      typeof location.range?.end?.line !== "number"
   ) {
      return undefined;
   }
   return { url: location.url, range: location.range };
}

/**
 * The given-reading `where:` terms of a materialized source (the `given` kind
 * of its classified terms, {@link classifyWhereTerms}), each one its own
 * declaration writes and over stored columns only, or why one is not.
 * See the module comment for why each refusal is one. A term that reads both
 * a given and the current time is not this function's: rolling_window.ts
 * refuses it.
 */
export function readTimeTermsOf(
   def: SourceDefLike,
   classified: readonly ClassifiedWhereTerm[],
   columns: readonly MaterializeColumn[],
   givens: Record<string, unknown> = {},
   /**
    * Keep a term written outside the source's own declaration, marked
    * `inherited`, rather than refuse it here, so a `#(materialize)`
    * extension's planner refuses it with the reason that fits once its
    * parent is known (`chainExtensionTables`).
    */
   options: { deferInherited?: boolean } = {},
):
   | {
        ok: true;
        terms: GivenReadTerm[];
        declaration?: TermUse;
        givenDeclarations?: MaterializeGivenDeclaration[];
     }
   | {
        ok: false;
        reason: string;
        refusal:
           | "inherited_where_with_given"
           | "reads_given"
           | "given_where_over_unstored_field";
     } {
   const located = classified.flatMap(({ dependsOn }) =>
      dependsOn.kind === "given"
         ? [
              {
                 ...dependsOn.term,
                 at: dependsOn.at,
                 givenIds: dependsOn.givenIds,
              },
           ]
         : [],
   );
   if (located.length === 0) return { ok: true, terms: [] };
   const declaration = declarationOf(def);
   const stored = new Set(columns.map((c) => c.column));
   const terms: GivenReadTerm[] = [];
   const givenDeclarations = new Map<string, string>();
   for (const term of located) {
      // malloy-text: markdown code formatting in a refusal reason, not Malloy
      const shown = term.code ? `\`where: ${term.code}\`` : "a `where:`";
      if (term.givens.length === 0 || term.code === "") {
         return {
            ok: false,
            reason: `${shown} reads a given that could not be read back, so it cannot be left out of the build`,
            refusal: "reads_given",
         };
      }
      // Only its own declaration's: the served copy keeps no base's text, so
      // an inherited term (from a base in this file or another) could not be
      // re-applied at read. Refused by the caller instead, with
      // `deferInherited`, once it knows whether a materialized parent is
      // involved.
      const inherited = !withinDeclaration(term.at, declaration);
      if (inherited && !options.deferInherited) {
         return {
            ok: false,
            reason: givenWhereNotOwnReason(shown),
            refusal: "inherited_where_with_given",
         };
      }
      if (
         term.columns.length === 0 ||
         !term.columns.every((column) => stored.has(column))
      ) {
         return {
            ok: false,
            reason:
               `${shown} reads a given and a field the table does not store ` +
               `(only dimensions of this source are stored), so it ` +
               `could not be re-applied over the table at read`,
            refusal: "given_where_over_unstored_field",
         };
      }
      // The generated `where:` reads each given by the name the author wrote,
      // so each must be importable under that name from the file declaring it.
      const declared = term.givenIds.map((id) =>
         givenDeclarationOf(givens[id]),
      );
      if (
         declared.length === 0 ||
         declared.some((d) => d === undefined) ||
         [...new Set(declared.map((d) => d?.name))].sort().join() !==
            [...new Set(term.givens)].sort().join()
      ) {
         return {
            ok: false,
            reason: `${shown} reads a given whose declaration could not be read back, so the build cannot re-write the term`,
            refusal: "reads_given",
         };
      }
      for (const d of declared as { name: string; url: string }[]) {
         givenDeclarations.set(d.name, d.url);
      }
      terms.push({
         kind: "given",
         code: term.code,
         givens: term.givens,
         columns: term.columns,
         at: term.at,
         ...(inherited ? { inherited: true as const } : {}),
      });
   }
   return {
      ok: true,
      terms,
      declaration,
      givenDeclarations: [...givenDeclarations]
         .map(([name, url]) => ({ name, url }))
         .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)),
   };
}

/** A compiled given's name and declaring file, or undefined. */
function givenDeclarationOf(
   given: unknown,
): { name: string; url: string } | undefined {
   const g = given as
      | { name?: unknown; location?: { url?: unknown } }
      | undefined;
   return typeof g?.name === "string" && typeof g.location?.url === "string"
      ? { name: g.name, url: g.location.url }
      : undefined;
}

export {
   TERM_NOT_FOUND as NOT_FOUND,
   termSpans,
} from "../text/declaration_text";

export const WINDOW_NOT_FOUND =
   "a `where:` keeping rows older than a moving cut-off could not be found " +
   "as written in the source's file, so it cannot be left out of the build";

/**
 * Where a left-out current-time term is written in `text`, the file that
 * declares it: the one occurrence of its verbatim text that covers every
 * place the compiler says it reads a field, or undefined when there is none
 * or more than one. Unlike a given term it may sit anywhere in the file (a
 * base this source extends wrote it): the served copy writes it again from
 * its parts, never from this text. The caller checks the file is the one
 * the term was compiled from.
 */
export function writtenSpan(
   text: string,
   term: { code: string; at: readonly TermUse[] },
): { start: number; end: number } | undefined {
   if (term.code === "" || term.at.length === 0) return undefined;
   const uses = term.at.map((use) => ({
      start: offsetOf(text, use.range.start),
      end: offsetOf(text, use.range.end),
   }));
   if (uses.some((u) => u.start < 0 || u.end < u.start)) return undefined;
   const matches: number[] = [];
   for (
      let at = text.indexOf(term.code);
      at >= 0;
      at = text.indexOf(term.code, at + 1)
   ) {
      const end = at + term.code.length;
      if (uses.every((u) => u.start >= at && u.end <= end)) matches.push(at);
   }
   if (matches.length !== 1) return undefined;
   return { start: matches[0], end: matches[0] + term.code.length };
}
