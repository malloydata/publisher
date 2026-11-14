// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Surfacing Malloy `Given` declarations on compiled models. Shared by the
 * main-thread `Model` and the package-load worker, so it must stay pure
 * TypeScript with no native deps.
 */

import {
   type GivenValue,
   isSourceDef,
   type ModelDef,
   type NamedQueryDef,
} from "@malloydata/malloy";
import type { Annotations } from "@malloydata/malloy";
import {
   BooleanFilterExpression,
   NumberFilterExpression,
   StringFilterExpression,
   TemporalFilterExpression,
} from "@malloydata/malloy-filter";
import { BadRequestError } from "../errors";
import { isReservedRoute } from "./annotations";
import { referencedGivenNames } from "./authorize";
import { MARKDOWN_ROUTE } from "./notebook";
import type { Tag } from "@malloydata/malloy-tag";
import { motlyTag, tagNumeric, tagText } from "./motly";

/**
 * Duck-typed shape of a Malloy SDK `Given` (the value type of
 * `Model.givens`), which the package root does not re-export.
 */
export interface MalloyGiven {
   readonly name: string;
   readonly type: {
      type: string;
      filterType?: string;
      /** Present when `type` is `array`; carries the element's own type def. */
      elementTypeDef?: { type: string };
   };
   readonly annotations: Annotations;
}

/** How a control renders. Absent means "infer from the given's type". */
export type GivenControlKind = "select" | "multiselect";

/**
 * Where a `select`/`multiselect` control gets its options: `query=` alone
 * (first column of each row), `source=` + `dimension=`, or `query=` +
 * `dimension=`. All run through the ordinary query endpoint. Mirrors the
 * `GivenSuggest` schema in `api-doc.yaml`; the two must move together.
 */
export interface GivenSuggestSpec {
   query?: string;
   source?: string;
   dimension?: string;
   /**
    * The givens the suggest query needs to RUN: those its source is scoped
    * or gated by, plus, for `query=`, those the named query references. A
    * client sends exactly these, so a gated source's options load without
    * the option list depending on the page's other filters.
    */
   givenNames?: string[];
}

/**
 * How a given is presented as an input control. Declared on the `given:`
 * itself so every surface (dashboard, notebook, SDK host) renders the same
 * control without restating it and drifting.
 */
export interface GivenControlSpec {
   label?: string;
   /**
    * Helper text, from a `# description=` tag. The older app-route spelling
    * `#(description="…")` still works client-side, but warns
    * `malformed-route` on every compile (Malloy ends a route at the first
    * whitespace); the plain tag carries no route and compiles clean.
    */
   description?: string;
   control?: GivenControlKind;
   rangeMin?: number;
   rangeMax?: number;
   suggest?: GivenSuggestSpec;
}

/** Wire shape of a given; structurally identical to the OpenAPI `Given`. */
export interface MalloyGivenApi extends GivenControlSpec {
   name: string;
   type: string;
   annotations?: string[];
   /** True when the declaration carries `#(secure)`: the host's to set; the builder hides its control only on a document held as text. */
   secure?: boolean;
   /** True when the given declares no default: a query that reads it fails until a value is supplied. */
   required?: boolean;
   /**
    * The default as a Malloy source literal (`'WN'`, `2003`, `true`,
    * `@2024-01-01`, `f'WN'`); omitted when there is none.
    */
   default?: string;
}

/**
 * A tag value that is present and non-blank. An empty `query=""` would
 * otherwise pass the runnable check below and send a client to fetch options
 * from nothing. Not folded into {@link tagText}, which also reads given
 * *values*, where an empty string is legitimate.
 */
function presentText(
   tag: Tag | undefined,
   ...path: string[]
): string | undefined {
   const raw = tagText(tag, ...path);
   return raw !== undefined && raw.trim() !== "" ? raw : undefined;
}

/**
 * Read the control contract from a given's raw annotation texts. These are
 * plain `#` tags, which `MalloyGivenApi.annotations` excludes, so the fields
 * are derived server-side rather than re-parsed (and drifting) per client.
 */
export function readGivenControlSpec(
   annotationTexts: readonly string[],
): GivenControlSpec {
   const spec: GivenControlSpec = {};
   const tag = motlyTag(annotationTexts);
   if (!tag) return spec;

   // Only throw-safe helpers: `Tag.text()` raises on some date literals, and
   // callers map over every given without a try/catch, so one typo would
   // fail the whole package's load.
   const label = presentText(tag, "label");
   if (label !== undefined) spec.label = label;

   const description = presentText(tag, "description");
   if (description !== undefined) spec.description = description;

   const control = tagText(tag, "control");
   if (control === "select" || control === "multiselect") {
      spec.control = control;
   }

   const rangeMin = tagNumeric(tag, "range_min");
   if (rangeMin !== undefined) spec.rangeMin = rangeMin;
   const rangeMax = tagNumeric(tag, "range_max");
   if (rangeMax !== undefined) spec.rangeMax = rangeMax;

   const suggest = tag.tag("suggest");
   if (suggest) {
      const parsed: GivenSuggestSpec = {};
      const query = presentText(suggest, "query");
      if (query !== undefined) parsed.query = query;
      const source = presentText(suggest, "source");
      if (source !== undefined) parsed.source = source;
      const dimension = presentText(suggest, "dimension");
      if (dimension !== undefined) parsed.dimension = dimension;
      // Emit only a runnable block: `source` alone names no column and
      // `dimension` alone names nothing to read. A `control=select` without a
      // `suggest` is a state clients already handle; a `suggest` that looks
      // runnable and is not would be a new one.
      const runnable =
         parsed.query !== undefined ||
         (parsed.source !== undefined && parsed.dimension !== undefined);
      if (runnable) spec.suggest = parsed;
   }

   return spec;
}

/** Whether a given declares no default: its parsed default and its rendered text are both absent. */
function givenDeclaresNoDefault(given: MalloyGiven): boolean {
   const loose = given as unknown as {
      default?: unknown;
      _internal?: { default?: unknown; defaultText?: string };
   };
   const parsed = "default" in loose ? loose.default : loose._internal?.default;
   return parsed === undefined && loose._internal?.defaultText === undefined;
}

/**
 * Convert a Malloy SDK `Given` to the wire shape.
 *
 * - `location` is never surfaced: it is an absolute `file://` path that
 *   would leak the OS user and install layout.
 * - `annotations` keeps only caller-facing app routes (e.g. `#(doc)`), not
 *   Malloy's reserved routes.
 * - An array type renders as `<element>[]`, not bare `array`: set-valued
 *   givens are how `#(secure)` attributes are declared, and the storage
 *   tier's serve shape re-declares givens from this text, so it must be
 *   valid Malloy.
 */
export function malloyGivenToApi(given: MalloyGiven): MalloyGivenApi {
   const type = given.type;
   const renderedType =
      type.type === "filter expression"
         ? `filter<${type.filterType}>`
         : type.type === "array" && type.elementTypeDef?.type
           ? `${type.elementTypeDef.type}[]`
           : type.type;
   const allNotes = given.annotations.forRoute(undefined);
   return {
      name: given.name,
      type: renderedType,
      annotations: allNotes
         // `(markdown)` is a notebook cell's own prose, not part of the given.
         .filter(
            (note) =>
               !isReservedRoute(note.route) && note.route !== MARKDOWN_ROUTE,
         )
         .map((note) => note.text),
      // Reads the reserved plain-`#` notes the line above drops, which is where
      // the control tags live.
      ...readGivenControlSpec(allNotes.map((note) => note.text)),
      ...(allNotes.some((note) => note.route === "secure") && { secure: true }),
      // The rendered literal exists only on Malloy's private `_internal` (the
      // public `.default` is an AST). A local cast, because widening
      // `MalloyGiven` would collide with the SDK `Given`'s own `_internal` at
      // every `as MalloyGiven` site.
      default: (given as { _internal?: { defaultText?: string } })._internal
         ?.defaultText,
      // From the parsed default and the rendered text together, so neither
      // going missing alone makes a given with a default look required.
      ...(givenDeclaresNoDefault(given) && { required: true }),
   };
}

/**
 * Collect the given names referenced anywhere in a slice of Malloy IR. A
 * view's `TurtleDef` has no `givenUsage` summary, but its pipeline holds the
 * `{ node: 'given', refName }` nodes, so a structural walk is exact.
 */
export function collectGivenRefs(value: unknown, into: Set<string>): void {
   if (Array.isArray(value)) {
      for (const item of value) collectGivenRefs(item, into);
      return;
   }
   if (value === null || typeof value !== "object") return;
   const node = value as Record<string, unknown>;
   if (node.node === "given" && typeof node.refName === "string") {
      into.add(node.refName);
   }
   for (const child of Object.values(node)) collectGivenRefs(child, into);
}

/**
 * Which givens a query over a source, or a named query, needs in its request.
 * Both answer with names in first-seen order, or undefined for an unknown name.
 */
export interface SuggestGivenLookup {
   forSource(name: string): string[] | undefined;
   forQuery(name: string): string[] | undefined;
}

/**
 * The gate expressions a `suggest` over `name` must supply givens for: BOTH
 * routes, unioned. A dropdown's query is an ordinary query and meets both
 * gates: an omitted lock given denies (403), and an omitted filter given
 * cannot be grafted.
 */
export function gateGivenSource(
   sources: readonly {
      name?: string | undefined;
      authorize?: string[] | undefined;
      accessFilter?: string[] | undefined;
   }[],
   name: string,
): readonly string[] | undefined {
   const source = sources.find((candidate) => candidate.name === name);
   if (!source) return undefined;
   return [...(source.authorize ?? []), ...(source.accessFilter ?? [])];
}

/**
 * Build the lookup a `suggest` is resolved against. A source's names are the
 * givens its own `where:` reads plus those its EFFECTIVE gates read; the
 * caller supplies the gates (see {@link gateGivenSource}) because inheritance
 * is resolved by `extractSourcesFromModelDef`. A named query adds its own
 * `givenUsage`. `surfaced` narrows every answer to names the entry can bind,
 * since any other yields an "unknown given" error.
 */
export function suggestGivenLookup(
   modelDef: ModelDef,
   gatesBySource: (source: string) => readonly string[] | undefined,
   surfaced?: ReadonlySet<string>,
): SuggestGivenLookup {
   const registry = modelDef.givens ?? {};
   const bySource = new Map<string, string[]>();
   const byQuery = new Map<string, { own: string[]; source?: string }>();
   for (const obj of Object.values(modelDef.contents)) {
      if (isSourceDef(obj)) {
         const name = obj.as || obj.name;
         const refs = new Set<string>();
         collectGivenRefs(obj.filterList, refs);
         for (const expr of gatesBySource(name) ?? []) {
            for (const given of referencedGivenNames(expr)) refs.add(given);
         }
         bySource.set(name, Array.from(refs));
      } else if (obj.type === "query") {
         const query = obj as NamedQueryDef;
         byQuery.set(query.as || query.name, {
            own: (query.givenUsage ?? [])
               .map((usage) => registry[usage.id]?.name)
               .filter((n): n is string => n !== undefined),
            source:
               typeof query.structRef === "string"
                  ? query.structRef
                  : undefined,
         });
      }
   }
   const narrow = (names: string[]) =>
      Array.from(new Set(names)).filter(
         (name) => surfaced === undefined || surfaced.has(name),
      );
   return {
      forSource: (name) => {
         const found = bySource.get(name);
         return found && narrow(found);
      },
      forQuery: (name) => {
         const found = byQuery.get(name);
         if (!found) return undefined;
         return narrow([
            ...found.own,
            ...(found.source ? (bySource.get(found.source) ?? []) : []),
         ]);
      },
   };
}

/**
 * The `givenNames` for one suggest block, or undefined when it needs none or
 * names something the lookup does not know (a target the lint reports).
 */
export function suggestGivenNames(
   suggest: GivenSuggestSpec,
   lookup: SuggestGivenLookup,
): string[] | undefined {
   const names =
      suggest.query !== undefined
         ? lookup.forQuery(suggest.query)
         : suggest.source !== undefined
           ? lookup.forSource(suggest.source)
           : undefined;
   return names && names.length > 0 ? names : undefined;
}

/**
 * Fill in `suggest.givenNames` on every given that has a suggest block, in
 * place, so the same objects the sources carry see it too.
 */
export function attachSuggestGivenNames(
   givens: readonly MalloyGivenApi[] | undefined,
   lookup: SuggestGivenLookup,
): void {
   for (const given of givens ?? []) {
      if (!given.suggest) continue;
      const names = suggestGivenNames(given.suggest, lookup);
      if (names) given.suggest.givenNames = names;
      else delete given.suggest.givenNames;
   }
}

/** The parser for each `filter<T>` a given can declare, keyed by `T`. */
const FILTER_PARSERS: Record<
   string,
   { parse(text: string): { log: { message: string; severity: string }[] } }
> = {
   string: StringFilterExpression,
   number: NumberFilterExpression,
   boolean: BooleanFilterExpression,
   date: TemporalFilterExpression,
   timestamp: TemporalFilterExpression,
   timestamptz: TemporalFilterExpression,
};

/**
 * Refuse a `filter<T>` given value that does not parse as a `T` filter, with
 * the parser's own reason (Malloy's later refusal prints `[object Object]`).
 * Uses Malloy's parser, so anything accepted here compiles. Unknown names are
 * left to Malloy's "unknown given" error.
 */
export function assertFilterGivensParse(
   declared: readonly { name?: string; type?: string }[] | undefined,
   givens: Record<string, GivenValue> | undefined,
): void {
   if (!declared || !givens) return;
   for (const given of declared) {
      const inner = /^filter<(.+)>$/.exec(given.type ?? "")?.[1];
      if (!given.name || !inner) continue;
      const value = givens[given.name];
      if (typeof value !== "string") continue;
      // Malloy refuses a filter on any log entry, whatever its severity, and
      // reports the first; match it, so every value this passes compiles.
      const problem = FILTER_PARSERS[inner]?.parse(value).log[0];
      if (problem) {
         throw new BadRequestError(
            `Invalid value for given ${given.name} (${given.type}): ` +
               `${problem.message.replace(/\.$/, "")}. Fix: send a ` +
               `${given.type} expression, or leave ${given.name} unset to use ` +
               `its default.`,
         );
      }
   }
}
