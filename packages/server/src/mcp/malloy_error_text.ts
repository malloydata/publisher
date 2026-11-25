// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The only place MCP advice reads an error's MESSAGE. Each matcher recognizes
 * wording that @malloydata/malloy (its compiler, or a driver it wraps) produces
 * and that reaches us as text: a MalloyError, or a BadRequestError or
 * ModelCompilationError that re-wrapped one. We cannot type those, so they are
 * matched here.
 *
 * An error Publisher throws itself carries what it means as data instead (a
 * class, or a field such as AccessDeniedError.source), and must not be matched
 * here: a message is prose, and a reworded one silently loses its advice.
 */

/**
 * A restricted-mode refusal, recognized by the compiler's wording when the
 * error lost its structured `problems` (re-wrapped). Narrower than the word
 * "restricted": a match suppresses the other diagnostics, so a user identifier
 * containing the word must not trip it.
 */
export function isRestrictedByMessage(message: string): boolean {
   return /cannot be used in a restricted/i.test(message);
}

/** What a Malloy error message says went wrong, when it says it in a shape we recognize. */
export type MalloyTextMatch =
   | { match: "view_not_found"; view: string; source: string }
   | { match: "source_not_found"; source: string }
   | { match: "query_not_found"; query: string }
   | {
        match: "field_not_found";
        field: string;
        contextType: string;
        contextName: string;
     }
   | { match: "undefined_reference"; object: string | undefined }
   | { match: "syntax" }
   | { match: "cannot_connect" };

/**
 * The first shape `message` matches. Order matters: more specific patterns
 * are checked first.
 */
export function matchMalloyErrorText(
   message: string,
): MalloyTextMatch | undefined {
   const view = message.match(/View '([^']+)' not found in source '([^']+)'/i);
   if (view) return { match: "view_not_found", view: view[1], source: view[2] };
   const source = message.match(/Source '([^']+)' not found/i);
   if (source) return { match: "source_not_found", source: source[1] };
   const query = message.match(/Query '([^']+)' not found/i);
   if (query) return { match: "query_not_found", query: query[1] };
   const field = message.match(
      /Field '([^']+)' not found in (source|query|view) '([^']+)'/i,
   );
   if (field) {
      return {
         match: "field_not_found",
         field: field[1],
         contextType: field[2],
         contextName: field[3],
      };
   }
   const reference = message.match(
      /Reference to undefined object(?: '([^']+)')?/i,
   );
   if (reference) {
      return { match: "undefined_reference", object: reference[1] };
   }
   if (/no viable alternative at input/i.test(message)) {
      return { match: "syntax" };
   }
   if (/Cannot connect to database/i.test(message)) {
      return { match: "cannot_connect" };
   }
   return undefined;
}
