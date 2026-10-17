// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Filter annotation parsing and query filter injection.
 *
 * Annotation format on a Malloy source:
 *   #(filter) [name=NAME] dimension=DIMENSION_NAME type=[equal|in|like|greater_than|less_than] [implicit] [required]
 *
 * At query time, Publisher injects `+ {where: <clause>}` into the Malloy query
 * based on the provided filter values and the declared filter definitions.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type FilterType = "equal" | "in" | "like" | "greater_than" | "less_than";

const VALID_FILTER_TYPES = new Set<FilterType>([
   "equal",
   "in",
   "like",
   "greater_than",
   "less_than",
]);

export interface FilterDefinition {
   /** Display name for the filter. Defaults to the dimension name. */
   name: string;
   dimension: string;
   type: FilterType;
   /** Hidden from user/agent summaries; set by infrastructure (e.g. row-level security). */
   implicit: boolean;
   /** Must be provided for every query (or error). */
   required: boolean;
}

/** Filter values provided at query time, keyed by filter name. */
export type FilterParams = Record<string, string | string[]>;

// ---------------------------------------------------------------------------
// Annotation Parsing
// ---------------------------------------------------------------------------

export const FILTER_ANNOTATION_PREFIX = "#(filter)";

/**
 * Parse a single `#(filter)` annotation string into a definition.
 * Returns `null` if the string is not a filter annotation.
 * Throws on malformed annotations (missing required fields, bad type).
 */
export function parseFilterAnnotation(
   annotation: string,
): FilterDefinition | null {
   const trimmed = annotation.trim();
   if (!trimmed.startsWith(FILTER_ANNOTATION_PREFIX)) {
      return null;
   }

   const body = trimmed.slice(FILTER_ANNOTATION_PREFIX.length).trim();
   const tokens = tokenize(body);

   let name: string | undefined;
   let dimension: string | undefined;
   let type: FilterType | undefined;
   let implicit = false;
   let required = false;

   for (const token of tokens) {
      if (token.includes("=")) {
         const eqIndex = token.indexOf("=");
         const key = token.slice(0, eqIndex).toLowerCase();
         const value = token.slice(eqIndex + 1);
         switch (key) {
            case "name":
               name = value;
               break;
            case "dimension":
               dimension = value;
               break;
            case "type":
               if (!VALID_FILTER_TYPES.has(value as FilterType)) {
                  throw new Error(
                     `Invalid filter type "${value}". Must be one of: ${[...VALID_FILTER_TYPES].join(", ")}`,
                  );
               }
               type = value as FilterType;
               break;
            default:
               throw new Error(`Unknown filter parameter "${key}"`);
         }
      } else {
         const flag = token.toLowerCase();
         if (flag === "implicit") {
            implicit = true;
         } else if (flag === "required") {
            required = true;
         } else {
            throw new Error(`Unknown filter flag "${token}"`);
         }
      }
   }

   if (!dimension) {
      throw new Error(
         "filter annotation missing required 'dimension' parameter",
      );
   }
   if (!type) {
      throw new Error("filter annotation missing required 'type' parameter");
   }

   return {
      name: name ?? dimension,
      dimension,
      type,
      implicit,
      required,
   };
}

/** Extract all `#(filter)` definitions from a source's `blockNotes`. */
export function parseFilters(annotations: string[]): FilterDefinition[] {
   // Keyed by name so an extending source's annotations, which come after
   // the inherited base's, override them.
   const byName = new Map<string, FilterDefinition>();
   for (const annotation of annotations) {
      const parsed = parseFilterAnnotation(annotation);
      if (parsed) {
         byName.set(parsed.name, parsed);
      }
   }
   return [...byName.values()];
}

// ---------------------------------------------------------------------------
// Filter Clause Generation
// ---------------------------------------------------------------------------

function escapeMalloyString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function isBooleanLiteral(v: string): boolean {
   const lower = v.toLowerCase();
   return lower === "true" || lower === "false";
}

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const ISO_TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/;

function isDateLiteral(v: string): boolean {
   return ISO_DATE_RE.test(v) || ISO_TIMESTAMP_RE.test(v);
}

/** Booleans unquoted, ISO dates/timestamps as `@YYYY-MM-DD`, else a quoted string. */
function malloyLiteral(v: string): string {
   if (isBooleanLiteral(v)) {
      return v.toLowerCase();
   }
   if (isDateLiteral(v)) {
      return `@${v.slice(0, 10)}`;
   }
   return `'${escapeMalloyString(v)}'`;
}

function buildPredicate(
   filter: FilterDefinition,
   value: string | string[],
): string {
   const dim = `\`${filter.dimension}\``;

   switch (filter.type) {
      case "equal": {
         const v = Array.isArray(value) ? value[0] : value;
         return `${dim} = ${malloyLiteral(v)}`;
      }
      case "in": {
         const values = Array.isArray(value) ? value : [value];
         if (values.length === 1) {
            return `${dim} = ${malloyLiteral(values[0])}`;
         }
         const conditions = values.map((v) => `${dim} = ${malloyLiteral(v)}`);
         return `(${conditions.join(" or ")})`;
      }
      case "like": {
         const v = Array.isArray(value) ? value[0] : value;
         const escaped = escapeMalloyString(v.toLowerCase());
         const pattern =
            escaped.startsWith("%") || escaped.endsWith("%")
               ? escaped
               : `%${escaped}%`;
         return `lower(${dim}) ~ '${pattern}'`;
      }
      case "greater_than": {
         const v = Array.isArray(value) ? value[0] : value;
         return `${dim} > ${malloyLiteral(v)}`;
      }
      case "less_than": {
         const v = Array.isArray(value) ? value[0] : value;
         return `${dim} < ${malloyLiteral(v)}`;
      }
   }
}

/**
 * Whether `params` supplies a value for `filter`: the single definition of
 * "was this filter injected", shared by {@link buildFilterClause} and the
 * binding guard's caller (a filter never injected cannot be misbound).
 */
export function filterHasValue(
   filter: FilterDefinition,
   params: FilterParams,
): boolean {
   const value = params[filter.name];
   return (
      value !== undefined &&
      value !== null &&
      (Array.isArray(value) ? value.length > 0 : value !== "")
   );
}

/**
 * Build a Malloy `where:` clause body from the filters that have values, or
 * "" when none apply. Throws if a required filter has no value.
 */
export function buildFilterClause(
   filters: FilterDefinition[],
   params: FilterParams,
): string {
   const predicates: string[] = [];

   for (const filter of filters) {
      if (!filterHasValue(filter, params)) {
         if (filter.required) {
            throw new FilterValidationError(
               `Required filter "${filter.name}" (dimension: ${filter.dimension}) was not provided`,
            );
         }
         continue;
      }

      predicates.push(buildPredicate(filter, params[filter.name]));
   }

   if (predicates.length === 0) {
      return "";
   }

   return predicates.join(" and ");
}

/**
 * Append `+ {where: ...}` to a query; an empty clause leaves it unchanged.
 * The refinement goes on its own line so a trailing `//` or `--` comment in
 * the caller's query cannot swallow it and neutralize the filter.
 */
export function injectFilterRefinement(
   query: string,
   filterClause: string,
): string {
   if (!filterClause) {
      return query;
   }
   return `${query.trimEnd()}\n+ {where: ${filterClause}}`;
}

// ---------------------------------------------------------------------------
// Validation Error
// ---------------------------------------------------------------------------

export class FilterValidationError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "FilterValidationError";
   }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Split on whitespace, respecting quotes: `name="Foo Bar" dimension=status`. */
function tokenize(input: string): string[] {
   const tokens: string[] = [];
   let current = "";
   let inQuote = false;
   let quoteChar = "";

   for (const ch of input) {
      if (inQuote) {
         if (ch === quoteChar) {
            inQuote = false;
         } else {
            current += ch;
         }
      } else if (ch === '"' || ch === "'") {
         inQuote = true;
         quoteChar = ch;
      } else if (ch === " " || ch === "\t") {
         if (current) {
            tokens.push(current);
            current = "";
         }
      } else {
         current += ch;
      }
   }

   if (current) {
      tokens.push(current);
   }

   return tokens;
}
