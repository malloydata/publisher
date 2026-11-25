// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";

/** The rows at a result's top level, from the result's JSON; undefined for a
 *  string that isn't a result. */
function rowsInResultJson(result: string): number | undefined {
   try {
      const { data } = JSON.parse(result) as Pick<Malloy.Result, "data">;
      if (data?.kind === "array_cell") return data.array_value.length;
      return data?.kind === "record_cell" ? 1 : 0;
   } catch {
      return undefined;
   }
}

/**
 * What a source's access declarations say, read from the compiled model's
 * `sources[]` (the publisher's `Source.authorize` and `Source.accessFilter`).
 *
 * Both lists are the RULE, never a verdict about the reader: the publisher is
 * the only authority on whether a caller passes, and its docs forbid
 * re-deriving that from these strings. So everything here answers "is this
 * source restricted / row filtered, and what does the rule say", and nothing
 * answers "can you query it".
 */
export interface SourceAccess {
   /** Effective `#(authorize)` expressions (own and inherited), AND-ed. */
   authorize: string[];
   /** Effective `#(access_filter)` expressions, AND-ed. */
   accessFilter: string[];
}

const EMPTY: SourceAccess = { authorize: [], accessFilter: [] };

function stringList(value: unknown): string[] {
   return Array.isArray(value)
      ? value.filter(
           (v): v is string => typeof v === "string" && v.trim() !== "",
        )
      : [];
}

/**
 * Source name → its access declarations, for every source the compiled model
 * lists. Shape-checked: through the router an older publisher sends neither
 * field (or `null`), which reads as no declaration.
 */
export function extractSourceAccess(body: unknown): Map<string, SourceAccess> {
   const out = new Map<string, SourceAccess>();
   const sources = (body as { sources?: unknown })?.sources;
   if (!Array.isArray(sources)) return out;
   for (const raw of sources) {
      if (!raw || typeof raw !== "object") continue;
      const s = raw as Record<string, unknown>;
      if (typeof s.name !== "string" || !s.name) continue;
      const authorize = stringList(s.authorize);
      const accessFilter = stringList(s.accessFilter);
      if (authorize.length === 0 && accessFilter.length === 0) continue;
      out.set(s.name, { authorize, accessFilter });
   }
   return out;
}

/** The declarations of one source, or none. */
export function accessOf(
   bySource: ReadonlyMap<string, SourceAccess> | undefined,
   sourceName: string | undefined,
): SourceAccess {
   return (sourceName && bySource?.get(sourceName)) || EMPTY;
}

/**
 * Whether an `#(authorize)` rule locks the source. Read element-wise, as the
 * publisher documents: the list is unlocked only when every element is the
 * `"true"` sentinel (gates from different declaring sources AND together, so
 * `["true", "'finance' in $GROUPS"]` is locked).
 */
export function isRestricted(access: SourceAccess | undefined): boolean {
   return !!access && access.authorize.some((e) => e.trim() !== "true");
}

/** Whether an `#(access_filter)` scopes the source's rows per reader. */
export function isRowFiltered(access: SourceAccess | undefined): boolean {
   return !!access && access.accessFilter.length > 0;
}

/** `'finance' in $GROUPS` (either quote) → `finance`. */
const GROUP_ATOM = /^\(?\s*(?:'([^']*)'|"([^"]*)")\s+in\s+\$GROUPS\s*\)?$/;

/**
 * One rule in words, or undefined where it isn't the shape this can say
 * faithfully. Only group membership is put into words: any other given a rule
 * reads (a role, a tenant) is the author's vocabulary, and guessing at it
 * would tell a reader something the rule does not say.
 */
function ruleWords(expr: string): string | undefined {
   const e = expr.trim();
   if (e === "false") return "no one";
   const single = GROUP_ATOM.exec(e);
   if (single) return `members of ${single[1] ?? single[2]}`;
   for (const joiner of ["or", "and"] as const) {
      const parts = e.split(new RegExp(`\\s+${joiner}\\s+`));
      if (parts.length < 2) continue;
      const groups = parts.map((p) => GROUP_ATOM.exec(p.trim()));
      if (groups.every((g) => g)) {
         const names = groups.map((g) => g![1] ?? g![2]);
         return joiner === "or"
            ? `members of ${names.slice(0, -1).join(", ")} or ${names[names.length - 1]}`
            : `members of all of ${names.join(", ")}`;
      }
   }
   return undefined;
}

/**
 * Who the `#(authorize)` rule admits, in plain words: "members of finance".
 * Several rules all apply, so they are joined with "and"; a rule this cannot
 * say becomes "readers its access rule admits" rather than a guess.
 */
export function restrictedToWords(access: SourceAccess): string {
   const words = access.authorize
      .filter((e) => e.trim() !== "true")
      .map((e) => ruleWords(e) ?? "readers its access rule admits");
   const unique = Array.from(new Set(words));
   return unique.join(", and ");
}

/** What a Restricted chip's tooltip and accessible name say. */
export const RESTRICTED_MEANING = "Only some readers can query this source";
/** What a Row filtered chip's tooltip and accessible name say. */
export const ROW_FILTERED_MEANING =
   "Each reader sees only the rows they're allowed to see";

/**
 * What a refused (403) run says: that the reader can't access the source, a
 * restricted source's rule in words, and the way out — never a status code, and
 * no Retry, since a second attempt meets the same rule. A 403 on a source with no
 * rule this page knows (a package-level refusal, a rule the compiled model did
 * not report) still says so in the same words.
 */
export function refusalMessage(
   sourceName: string | undefined,
   access: SourceAccess,
): string {
   if (!sourceName)
      return "You don't have access to run this. Ask an administrator of this package for access.";
   if (!isRestricted(access))
      return `You can't access ${sourceName}. Ask an administrator of this package for access.`;
   return `You can't access ${sourceName}. It is restricted to ${restrictedToWords(
      access,
   )}; ask an administrator of this package for access.`;
}

/**
 * The caption under an empty result on a row-filtered source: an access filter
 * that admits none of a reader's rows answers 200 with zero rows, which reads as
 * "there is no data" unless the page says why. Only for zero rows: an ungrouped
 * aggregate over nothing returns one row (`count()` = 0), which the caption
 * cannot tell from a real zero, so it says nothing there.
 */
export function filteredEmptyCaption(
   access: SourceAccess,
   result: string | undefined,
): string | undefined {
   if (!isRowFiltered(access) || result === undefined) return undefined;
   return rowsInResultJson(result) === 0
      ? "Filtered to your rows: this source shows each reader only the rows they're allowed to see."
      : undefined;
}

/** Whether the source declares either rule, so there is something to show. */
export function hasAccessRules(access: SourceAccess | undefined): boolean {
   return isRestricted(access) || isRowFiltered(access);
}

/**
 * What an editor is told on an open join to a gated source: a gate applies only
 * on the source a query enters through, so through this join the joined
 * source's rows are read under this source's rules, not its own.
 */
export function joinGateWarning(
   sourceName: string,
   access: SourceAccess,
): string {
   const rule =
      isRestricted(access) && isRowFiltered(access)
         ? "access rule and row filter"
         : isRestricted(access)
           ? "access rule"
           : "row filter";
   return `This join doesn't apply the ${rule} on ${sourceName}: its rows are read under this source's rules.`;
}
