// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Shared utilities for surfacing Malloy `Given` declarations on
 * compiled models.
 *
 * The Malloy SDK's `Given` class is declared in
 * `@malloydata/malloy/dist/api/foundation/core.d.ts` but is not
 * re-exported from the package root, so we duck-type against the
 * surface we actually use and don't pull in the private type.
 *
 * Lives here so both the main-thread `Model` constructor and the
 * package-load worker can use the same conversion. The worker
 * imports this file directly (it's pure TypeScript with no native
 * deps, so it's safe to bundle into the worker entry).
 */

import {
   isSourceDef,
   type ModelDef,
   type NamedQueryDef,
} from "@malloydata/malloy";
import type { Annotations } from "@malloydata/malloy";
import { isReservedRoute } from "./annotations";
import { referencedGivenNames } from "./authorize";
import type { Tag } from "@malloydata/malloy-tag";
import { motlyTag, tagNumeric, tagText } from "./motly";

/**
 * Duck-typed shape of a Malloy SDK `Given` instance (the value type
 * of `Model.givens`). `Given` itself isn't re-exported from the
 * package root, but the `Annotations` view it returns is.
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
 * Where a `select`/`multiselect` control gets its options: a named `query=`, a
 * `source=` + `dimension=` pair, or a `query=` with a `dimension=` naming which
 * of its columns to read. All three run through the ordinary query endpoint, so
 * none of them is a new capability.
 *
 * With `query=` alone the first column of each row is used, so a single-column
 * `group_by` needs no `dimension=`. Mirrors the `GivenSuggest` schema in
 * `api-doc.yaml`; the two are one contract and have to move together.
 */
export interface GivenSuggestSpec {
   query?: string;
   source?: string;
   dimension?: string;
   /**
    * The givens the suggest query needs in its request to RUN: those the
    * source it reads is scoped by (a source-level `where: … ~ $X`) or gated by
    * (an `#(authorize)` expression reading `$X`), plus, for the `query=` form,
    * the ones the named query itself references. A client sends the current
    * values of exactly these and nothing else, so a gated source's options load
    * while the option list still does not depend on the page's other filters.
    * Absent when the query needs none, or from a server too old to say.
    */
   givenNames?: string[];
}

/**
 * How a given should be presented as an input control.
 *
 * Declared on the `given:` itself, which is what makes it portable: a
 * dashboard, a notebook, and an SDK host all render the same control for the
 * same given without any of them restating it. Authoring one of these per
 * surface would let them drift, which is the whole reason it lives here.
 */
export interface GivenControlSpec {
   label?: string;
   /**
    * Helper text for the control, from a `# description=` tag.
    *
    * There is a second, older spelling: `#(description="…")`, an app-route
    * annotation that clients re-parse themselves. It still works and still has
    * live callers, so nothing here retires it. It is worth knowing why the
    * plain-tag form is only now available. `docs/givens.md` weighed exactly
    * this form and rejected it, correctly at the time, because plain `#` tags
    * are Malloy's reserved namespace and `malloyGivenToApi` drops them before
    * `annotations` reaches a client. Reading them here server-side is what
    * changes that, so the tag form becomes readable for the first time.
    *
    * It is also the form that compiles clean: Malloy takes an annotation's
    * route up to the first whitespace, so `#(description="Earliest report
    * date")` warns `malformed-route` on every compile, while `# description=`
    * carries no route and does not.
    */
   description?: string;
   control?: GivenControlKind;
   rangeMin?: number;
   rangeMax?: number;
   suggest?: GivenSuggestSpec;
}

/**
 * Wire/API shape of a given. Structurally identical to the
 * `components["schemas"]["Given"]` shape from the OpenAPI spec —
 * callers can cast freely.
 */
export interface MalloyGivenApi extends GivenControlSpec {
   name: string;
   type: string;
   annotations?: string[];
   /**
    * The given's default as a Malloy source literal — one literal per declared
    * `type`. Examples across the type range: `'WN'` or `"WN"` (string), `2003`
    * (number), `true` (boolean), `@2024-01-01` (date), `f'WN'` (filter). Omitted
    * when the given has no default. Consumers render/prefill it per `type` (e.g.
    * unquote a string).
    */
   default?: string;
}

/**
 * A tag value that is present *and* says something, for the fields where an
 * empty value cannot mean anything.
 *
 * `text()` returns `""` for `label=""`, which passes an `!== undefined` check
 * and ships an empty label, an empty helper text, or worst of all a `suggest`
 * block whose `query=""` satisfies the runnable check below and tells a client
 * to fetch options from nothing. Absent and empty are the same intent here, so
 * they get the same answer: omit the field. Note that is *not* a fallback to
 * something type-derived; only `control` has one of those, and `control` is the
 * one field this does not guard (an empty value fails its enum check anyway).
 * An absent `label` is simply an absent label.
 *
 * This is deliberately NOT folded into {@link tagText}, which is also how given
 * *values* are read, and there an empty string is a legitimate value (an empty
 * filter is not an absent filter).
 */
function presentText(
   tag: Tag | undefined,
   ...path: string[]
): string | undefined {
   const raw = tagText(tag, ...path);
   return raw !== undefined && raw.trim() !== "" ? raw : undefined;
}

/**
 * Read the control contract out of a given declaration's annotations.
 *
 * The input is the raw annotation texts, MOTLY route included: these are plain
 * `#` tags, which `MalloyGivenApi.annotations` deliberately excludes (it
 * carries only caller-facing app routes like `#(doc)`). So the control fields
 * are *derived* here and shipped alongside, rather than left for each client to
 * re-parse. A client re-parsing them would need the MOTLY parser and the
 * filter-literal quoting workaround, and would drift from the dashboard path.
 */
export function readGivenControlSpec(
   annotationTexts: readonly string[],
): GivenControlSpec {
   const spec: GivenControlSpec = {};
   const tag = motlyTag(annotationTexts);
   if (!tag) return spec;

   // Every read goes through the throw-safe helpers: `Tag.text()` raises on a
   // date literal MOTLY accepts and `Date` rejects, and both production callers
   // map over every given with no try/catch, so an unguarded read would turn one
   // typo into a failed load for the whole package.
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
      // Emit only a block a client can actually run, which is the three forms
      // `GivenSuggest` documents: `query` alone, `source` + `dimension`, or
      // `query` + `dimension`. `source` alone names no column and `dimension`
      // alone names nothing to read it from, so neither can fetch options.
      //
      // Dropping the block does NOT rescue the control: a declared
      // `control=select` still ships, so the client is told to render a list and
      // given no source for it. That is the same position as any `control=select`
      // carrying no `suggest` at all, which the schema permits and a client has
      // to handle anyway, so this does not invent a new state. What it avoids is
      // publishing a `suggest` that looks runnable and is not.
      const runnable =
         parsed.query !== undefined ||
         (parsed.source !== undefined && parsed.dimension !== undefined);
      if (runnable) spec.suggest = parsed;
   }

   return spec;
}

/**
 * Convert a Malloy SDK `Given` to the wire/API shape.
 *
 * Two fields are deliberately not surfaced:
 *
 * - `location` — Malloy's `DocumentLocation.url` is an absolute
 *   `file://` path on the publisher's filesystem. Surfacing it
 *   would leak the OS user, install directory, and internal
 *   layout. Existing `Filter` introspection does not expose
 *   location either; matching that floor. A future PR can add a
 *   sanitised package-relative path if a client needs it.
 *
 * - `default` is surfaced as the rendered source literal
 *   (`given._internal.defaultText` — e.g. a string `'WN'`, number
 *   `2003`, boolean `true`, date `@2024-01-01`, or filter `f'WN'`).
 *   Malloy's public surface still exposes only the parsed `.default`
 *   AST; `_internal.defaultText` is the already-rendered string, so we
 *   forward it verbatim rather than re-implement the printer. Omitted
 *   when the given has no default.
 *
 * `annotations` is restricted to app-route annotations (bracketed,
 * caller-facing, e.g. `#(doc)`), excluding Malloy's reserved routes
 * (plain `#` tags, `#"` doc strings, `##!` pragmas), which aren't part
 * of the given's surface contract.
 *
 * Type rendering: a scalar renders as its own name (`string`,
 * `number`, `boolean`, `date`, `timestamp`, `timestamptz`,
 * `error`), a filter as `filter<…>`, and an ARRAY as
 * `<element>[]` — `number[]`, `string[]`.
 *
 * The array case is not decoration. A set-valued given is how a
 * `#(secure)` attribute is declared (a scalar cannot be one: it
 * has no value that fails closed), so it is the shape every
 * row-level access boundary uses. Rendering the bare `array`
 * discriminator loses the element type and yields text that is
 * not valid Malloy, which breaks any consumer that re-declares a
 * given from this field rather than merely displaying it — the
 * storage tier's serve shape does exactly that. A `record`
 * given, if the grammar gains one, still renders bare.
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
         .filter((note) => !isReservedRoute(note.route))
         .map((note) => note.text),
      // Reads the reserved plain-`#` notes the line above drops, which is where
      // the control tags live.
      ...readGivenControlSpec(allNotes.map((note) => note.text)),
      // `_internal.defaultText` is the already-rendered source literal of the
      // given's default. It lives on Malloy's private `_internal` (the public
      // surface exposes only the parsed `.default` AST node, not a stringified
      // form), so we reach it through a localized cast rather than widening the
      // duck-typed `MalloyGiven` — which would collide with the SDK `Given`'s
      // own private `_internal` at every `as MalloyGiven` cast site.
      default: (given as { _internal?: { defaultText?: string } })._internal
         ?.defaultText,
   };
}

/**
 * Collect the given names referenced anywhere in a slice of Malloy IR.
 *
 * A view's `TurtleDef` carries no `givenUsage` summary the way a `Query` does,
 * but its pipeline holds the `{ node: 'given', refName }` reference nodes
 * themselves, so a structural walk answers the same question exactly.
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
 * Build the lookup a `suggest` is resolved against, from a compiled model.
 *
 * A source's names are the givens its own `where:` reads plus the ones its
 * EFFECTIVE `#(authorize)` gate reads; the caller supplies the gate expressions
 * per source because inheritance (`extend` of a gated base) is resolved by
 * `extractSourcesFromModelDef`, not here. A named query's names are its own
 * `givenUsage` plus its source's. `surfaced`, when given, narrows every answer
 * to names the entry can actually bind, since sending any other guarantees an
 * "unknown given" error.
 */
export function suggestGivenLookup(
   modelDef: ModelDef,
   authorizeBySource: (source: string) => readonly string[] | undefined,
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
         for (const expr of authorizeBySource(name) ?? []) {
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
