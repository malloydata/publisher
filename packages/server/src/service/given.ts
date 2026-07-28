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

import type { Annotations } from "@malloydata/malloy";
import { isReservedRoute } from "./annotations";
import { motlyTag } from "./motly";

/**
 * Duck-typed shape of a Malloy SDK `Given` instance (the value type
 * of `Model.givens`). `Given` itself isn't re-exported from the
 * package root, but the `Annotations` view it returns is.
 */
export interface MalloyGiven {
   readonly name: string;
   readonly type: { type: string; filterType?: string };
   readonly annotations: Annotations;
}

/** How a control renders. Absent means "infer from the given's type". */
export type GivenControlKind = "select" | "multiselect";

/**
 * Where a `select`/`multiselect` control gets its options. Either a named
 * `query=` or an ad-hoc `source=` + `dimension=` pair; both run through the
 * ordinary query endpoint, so neither is a new capability.
 */
export interface GivenSuggestSpec {
   query?: string;
   source?: string;
   dimension?: string;
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
 * Read the control contract out of a given declaration's annotations.
 *
 * The input is the raw annotation texts, MOTLY route included: these are plain
 * `#` tags, which `MalloyGivenApi.annotations` deliberately excludes (it
 * carries only caller-facing app routes like `#(doc)`). So the control fields
 * are *derived* here and shipped alongside, rather than left for each client to
 * re-parse — a client re-parsing them would need the MOTLY parser and the
 * filter-literal quoting workaround, and would drift from the dashboard path.
 */
export function readGivenControlSpec(
   annotationTexts: readonly string[],
): GivenControlSpec {
   const spec: GivenControlSpec = {};
   const tag = motlyTag(annotationTexts);
   if (!tag) return spec;

   const label = tag.text("label");
   if (label !== undefined) spec.label = label;

   const control = tag.text("control");
   if (control === "select" || control === "multiselect") {
      spec.control = control;
   }

   const rangeMin = tag.numeric("range_min");
   if (rangeMin !== undefined) spec.rangeMin = rangeMin;
   const rangeMax = tag.numeric("range_max");
   if (rangeMax !== undefined) spec.rangeMax = rangeMax;

   const suggest = tag.tag("suggest");
   if (suggest) {
      const parsed: GivenSuggestSpec = {
         query: suggest.text("query"),
         source: suggest.text("source"),
         dimension: suggest.text("dimension"),
      };
      if (Object.values(parsed).some((value) => value !== undefined)) {
         spec.suggest = parsed;
      }
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
 * Type rendering: `GivenTypeDef` is typed as `AtomicTypeDef |
 * FilterExpressionParamTypeDef`, but Malloy's grammar only emits
 * the scalar parameter types (`string` | `number` | `boolean` |
 * `date` | `timestamp` | `timestamptz` | `filter expression` |
 * `error`) for given declarations today. If the grammar expands
 * to allow array or record givens, the bare `type.type`
 * discriminator (`'array'`, `'record'`) will land in the wire
 * response with no element info — revisit when that happens.
 */
export function malloyGivenToApi(given: MalloyGiven): MalloyGivenApi {
   const type = given.type;
   const renderedType =
      type.type === "filter expression"
         ? `filter<${type.filterType}>`
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
