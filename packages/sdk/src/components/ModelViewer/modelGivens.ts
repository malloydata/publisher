// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { givensToRequest } from "../given/paramCodec";

/**
 * A given the model declares, as `CompiledModel.givens` carries it, with the
 * model file that declared it so a row can say where it comes from.
 */
export type GivenInfo = Given & {
   /** Always present once shape-checked (the wire type leaves both optional). */
   name: string;
   type: string;
   /** Package-relative path of the model file that declared this given. */
   modelFile: string;
};

/** One given's value as a query request carries it. */
export type GivenRequestValue = string | number | boolean;

/** The `givens` map a run sends: keys sorted, so the same values always
 *  serialize (and cache-key, `givensKeyFor`) the same way. */
export type GivenRequest = Readonly<Record<string, GivenRequestValue>>;

/**
 * The givens of a compiled model, less the ones its host fills on every query
 * (`hostGivenNames`): a model may declare those (a gate reads `$GROUPS` with no
 * default, as a gate must), but they are never a reader's input, so a control
 * for one would be ignored and a missing value is never missing.
 *
 * Shape-checked rather than trusted, so an older server's body that lacks a
 * field reads as no givens rather than a throw.
 */
export function extractCompiledModelGivens(
   modelFile: string,
   body: unknown,
   hostGivenNames: ReadonlySet<string> = new Set(),
): GivenInfo[] {
   const arr = (body as { givens?: unknown })?.givens;
   if (!Array.isArray(arr)) return [];
   const out: GivenInfo[] = [];
   for (const raw of arr) {
      if (!raw || typeof raw !== "object") continue;
      const r = raw as Record<string, unknown>;
      if (typeof r.name !== "string" || !r.name) continue;
      if (typeof r.type !== "string" || !r.type) continue;
      if (hostGivenNames.has(r.name)) continue;
      const annotations = Array.isArray(r.annotations)
         ? r.annotations.filter((a): a is string => typeof a === "string")
         : undefined;
      out.push({
         name: r.name,
         type: r.type,
         modelFile,
         ...(annotations && { annotations }),
         ...(typeof r.default === "string" && { default: r.default }),
         ...(r.required === true && { required: true }),
         ...(typeof r.label === "string" && r.label && { label: r.label }),
         ...(typeof r.description === "string" &&
            r.description && { description: r.description }),
         ...((r.control === "select" || r.control === "multiselect") && {
            control: r.control,
         }),
         ...(typeof r.rangeMin === "number" && { rangeMin: r.rangeMin }),
         ...(typeof r.rangeMax === "number" && { rangeMax: r.rangeMax }),
         ...(r.suggest &&
            typeof r.suggest === "object" && {
               suggest: r.suggest as Given["suggest"],
            }),
      } as GivenInfo);
   }
   return out;
}

/** Declared type per given name, which decides how a `Date` is sent. */
export function declaredTypesOf(
   givens: ReadonlyArray<GivenInfo>,
): Map<string, string> {
   return new Map(givens.map((g) => [g.name, g.type]));
}

/**
 * The control row as the `givens` a run sends: only givens the model declares (a
 * value left over for a since-removed given must not reach the server as an
 * undeclared parameter), unset ones dropped, keys sorted. A blank `string` given
 * is unset too: a cleared text box means "use the default". `undefined` when
 * nothing is set, so a run at the defaults sends no `givens` at all.
 */
export function givensRequest(
   values: ReadonlyMap<string, GivenValue>,
   declaredTypes: ReadonlyMap<string, string | undefined>,
): GivenRequest | undefined {
   const declared = new Map(
      Array.from(values).filter(
         ([name, value]) =>
            declaredTypes.has(name) &&
            !(value === "" && declaredTypes.get(name) === "string"),
      ),
   );
   const request = givensToRequest(declared, declaredTypes) as Record<
      string,
      GivenRequestValue
   >;
   const entries = Object.entries(request).sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
   );
   return entries.length > 0 ? Object.fromEntries(entries) : undefined;
}

/** The givens part of a Malloy query's cache key: '' when none are set. */
export function givensKeyFor(
   givens: Readonly<Record<string, string | number | boolean>> | undefined,
): string {
   return givens && Object.keys(givens).length > 0
      ? JSON.stringify(givens)
      : "";
}

/**
 * Filter metadata parsed from a source's `#(filter)` annotation:
 * `#(filter) [name=NAME] dimension=DIMENSION type=TYPE [implicit] [required]`,
 * where a value may be double-quoted to hold spaces.
 */
export interface SourceFilter {
   /** Display name / API parameter key; defaults to the dimension name. */
   name: string;
   /** Name of the dimension the filter targets. */
   dimension: string;
   type: "equal" | "in" | "like" | "greater_than" | "less_than";
   /** If true, don't show in the UI. */
   implicit?: boolean;
   /** If true, the server requires a value. */
   required?: boolean;
}

/** Parse the `#(filter) ...` annotations on a source. */
export function parseSourceFilterAnnotations(
   annotations: Malloy.Annotation[] | undefined,
): SourceFilter[] {
   if (!annotations) return [];
   const filters: SourceFilter[] = [];
   for (const annotation of annotations) {
      const text = annotation.value?.trim();
      if (!text || !text.startsWith("#(filter)")) continue;
      const parsed = parseFilterBody(text.slice("#(filter)".length).trim());
      if (parsed) filters.push(parsed);
   }
   return filters;
}

const FILTER_TYPES: ReadonlySet<string> = new Set([
   "equal",
   "in",
   "like",
   "greater_than",
   "less_than",
]);

function parseFilterBody(body: string): SourceFilter | null {
   let name: string | undefined;
   let dimension: string | undefined;
   let type: SourceFilter["type"] | undefined;
   let implicit = false;
   let required = false;
   for (const token of tokenizeFilterBody(body)) {
      if (token === "implicit") {
         implicit = true;
         continue;
      }
      if (token === "required") {
         required = true;
         continue;
      }
      const eq = token.indexOf("=");
      if (eq <= 0) continue;
      const key = token.slice(0, eq).trim();
      let value = token.slice(eq + 1).trim();
      if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
         value = value.slice(1, -1);
      }
      if (key === "name") name = value;
      else if (key === "dimension") dimension = value;
      else if (key === "type" && FILTER_TYPES.has(value)) {
         type = value as SourceFilter["type"];
      }
   }
   if (!dimension || !type) return null;
   return { name: name ?? dimension, dimension, type, implicit, required };
}

/** Splits on whitespace, keeping a double-quoted run (`key="a b"`) together. */
function tokenizeFilterBody(body: string): string[] {
   const tokens: string[] = [];
   let current = "";
   let inQuotes = false;
   for (const c of body) {
      if (c === '"') {
         inQuotes = !inQuotes;
         current += c;
         continue;
      }
      if (!inQuotes && /\s/.test(c)) {
         if (current) tokens.push(current);
         current = "";
         continue;
      }
      current += c;
   }
   if (current) tokens.push(current);
   return tokens;
}

/**
 * The `#(index)` / `#(index_values)` tag, with or without parameters
 * (`#(index freshness.window="24h")`). `\b` keeps `#(indexed)` from matching.
 */
const INDEX_ANNOTATION = /^#\(index(_values)?\b/;

/** True if the field carries an `#(index_values)` or `#(index)` annotation. */
export function isFieldIndexed(field: Malloy.FieldInfo): boolean {
   if (!("annotations" in field) || !field.annotations) return false;
   return field.annotations.some((annotation) =>
      INDEX_ANNOTATION.test(annotation.value?.trim() ?? ""),
   );
}
