// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Parses `#(filter) ...` annotations declared on a Malloy source.
 *
 *   #(filter) [name=NAME] dimension=DIMENSION_NAME type=[equal|in|like|greater_than|less_than] [implicit] [required]
 */

import type { Filter } from "./internal_types";

export type { Filter };
type FilterType = NonNullable<Filter["type"]>;

const VALID_FILTER_TYPES = new Set<FilterType>([
   "equal",
   "in",
   "like",
   "greater_than",
   "less_than",
]);

const ANNOTATION_PREFIX = "#(filter)";

export function parseFilterAnnotation(annotation: string): Filter | null {
   const trimmed = annotation.trim();
   if (!trimmed.startsWith(ANNOTATION_PREFIX)) {
      return null;
   }

   const body = trimmed.slice(ANNOTATION_PREFIX.length).trim();
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

/**
 * Splits the annotation body on whitespace, respecting double/single quoted values.
 * Handles: `name="Foo Bar" dimension=status type=equal implicit`
 *
 * Exported so sibling annotation parsers (e.g. `#(index ...)` policy parsing)
 * tokenize `key="value"` bodies identically rather than re-implementing quote
 * handling.
 */
export function tokenizeAnnotationBody(input: string): string[] {
   return tokenize(input);
}

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
