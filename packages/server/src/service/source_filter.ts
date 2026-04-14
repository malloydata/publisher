/**
 * Source filter annotation parsing and query filter injection.
 *
 * Annotation format on a Malloy source:
 *   #(source_filter) [name=NAME] dimension=DIMENSION_NAME type=[equal|in|like|greater_than|less_than] [implicit] [required]
 *
 * At query time, Publisher injects `+ {where: <clause>}` into the Malloy query
 * based on the provided filter values and the declared filter definitions.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type SourceFilterType =
   | "equal"
   | "in"
   | "like"
   | "greater_than"
   | "less_than";

const VALID_FILTER_TYPES = new Set<SourceFilterType>([
   "equal",
   "in",
   "like",
   "greater_than",
   "less_than",
]);

export interface SourceFilterDefinition {
   /** Display name for the filter. Defaults to the dimension name. */
   name: string;
   /** The source dimension this filter targets. */
   dimension: string;
   /** Comparator type. */
   type: SourceFilterType;
   /** Hidden from user/agent summaries; set by infrastructure (e.g. row-level security). */
   implicit: boolean;
   /** Must be provided for every query (or error). */
   required: boolean;
}

/**
 * Filter values provided at query time.
 * Keys are filter names, values are the filter input(s).
 */
export type SourceFilterParams = Record<string, string | string[]>;

// ---------------------------------------------------------------------------
// Annotation Parsing
// ---------------------------------------------------------------------------

const ANNOTATION_PREFIX = "#(source_filter)";

/**
 * Parse a single `#(source_filter)` annotation string into a definition.
 * Returns `null` if the string is not a source_filter annotation.
 * Throws on malformed annotations (missing required fields, bad type).
 */
export function parseSourceFilterAnnotation(
   annotation: string,
): SourceFilterDefinition | null {
   const trimmed = annotation.trim();
   if (!trimmed.startsWith(ANNOTATION_PREFIX)) {
      return null;
   }

   const body = trimmed.slice(ANNOTATION_PREFIX.length).trim();
   const tokens = tokenize(body);

   let name: string | undefined;
   let dimension: string | undefined;
   let type: SourceFilterType | undefined;
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
               if (!VALID_FILTER_TYPES.has(value as SourceFilterType)) {
                  throw new Error(
                     `Invalid source_filter type "${value}". Must be one of: ${[...VALID_FILTER_TYPES].join(", ")}`,
                  );
               }
               type = value as SourceFilterType;
               break;
            default:
               throw new Error(`Unknown source_filter parameter "${key}"`);
         }
      } else {
         const flag = token.toLowerCase();
         if (flag === "implicit") {
            implicit = true;
         } else if (flag === "required") {
            required = true;
         } else {
            throw new Error(`Unknown source_filter flag "${token}"`);
         }
      }
   }

   if (!dimension) {
      throw new Error(
         "source_filter annotation missing required 'dimension' parameter",
      );
   }
   if (!type) {
      throw new Error(
         "source_filter annotation missing required 'type' parameter",
      );
   }

   return {
      name: name ?? dimension,
      dimension,
      type,
      implicit,
      required,
   };
}

/**
 * Extract all `#(source_filter)` definitions from a list of annotation strings
 * (as found on a Malloy source's `blockNotes`).
 */
export function parseSourceFilters(
   annotations: string[],
): SourceFilterDefinition[] {
   const filters: SourceFilterDefinition[] = [];
   for (const annotation of annotations) {
      const parsed = parseSourceFilterAnnotation(annotation);
      if (parsed) {
         filters.push(parsed);
      }
   }
   return filters;
}

// ---------------------------------------------------------------------------
// Filter Clause Generation
// ---------------------------------------------------------------------------

/**
 * Escape a string value for embedding in a Malloy query literal.
 */
function escapeMalloyString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/**
 * Returns true if the string is a bare boolean literal.
 */
function isBooleanLiteral(v: string): boolean {
   const lower = v.toLowerCase();
   return lower === "true" || lower === "false";
}

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const ISO_TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/;

/**
 * Returns true if the string looks like an ISO date or timestamp.
 */
function isDateLiteral(v: string): boolean {
   return ISO_DATE_RE.test(v) || ISO_TIMESTAMP_RE.test(v);
}

/**
 * Format a scalar value for Malloy.
 * - Boolean literals → unquoted true/false
 * - Date/timestamp strings → Malloy temporal literal @YYYY-MM-DD
 * - Everything else → single-quoted string
 */
function malloyLiteral(v: string): string {
   if (isBooleanLiteral(v)) {
      return v.toLowerCase();
   }
   if (isDateLiteral(v)) {
      return `@${v.slice(0, 10)}`;
   }
   return `'${escapeMalloyString(v)}'`;
}

/**
 * Build a single Malloy predicate expression for one filter.
 */
function buildPredicate(
   filter: SourceFilterDefinition,
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
         return `${dim} ~ '${escapeMalloyString(v)}'`;
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
 * Build a complete Malloy `where:` clause fragment from filter definitions
 * and provided parameter values.
 *
 * Returns an empty string when no filters apply.
 * Throws if a required filter has no value.
 */
export function buildFilterClause(
   filters: SourceFilterDefinition[],
   params: SourceFilterParams,
): string {
   const predicates: string[] = [];

   for (const filter of filters) {
      const value = params[filter.name];
      const hasValue =
         value !== undefined &&
         value !== null &&
         (Array.isArray(value) ? value.length > 0 : value !== "");

      if (!hasValue) {
         if (filter.required) {
            throw new SourceFilterValidationError(
               `Required filter "${filter.name}" (dimension: ${filter.dimension}) was not provided`,
            );
         }
         continue;
      }

      predicates.push(buildPredicate(filter, value));
   }

   if (predicates.length === 0) {
      return "";
   }

   return predicates.join(" and ");
}

/**
 * Append a filter refinement to a Malloy query string.
 * Uses Malloy's `+ {where: ...}` refinement syntax.
 *
 * If `filterClause` is empty, returns the original query unchanged.
 */
export function injectFilterRefinement(
   query: string,
   filterClause: string,
): string {
   if (!filterClause) {
      return query;
   }
   return `${query.trimEnd()} + {where: ${filterClause}}`;
}

// ---------------------------------------------------------------------------
// Validation Error
// ---------------------------------------------------------------------------

export class SourceFilterValidationError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "SourceFilterValidationError";
   }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Simple tokenizer that splits on whitespace but respects quoted values.
 * Handles: `name="Foo Bar" dimension=status type=equal implicit`
 */
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
