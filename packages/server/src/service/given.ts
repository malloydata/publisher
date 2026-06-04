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

/**
 * Wire/API shape of a given. Structurally identical to the
 * `components["schemas"]["Given"]` shape from the OpenAPI spec —
 * callers can cast freely.
 */
export interface MalloyGivenApi {
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
   return {
      name: given.name,
      type: renderedType,
      annotations: given.annotations
         .forRoute(undefined)
         .filter((note) => !isReservedRoute(note.route))
         .map((note) => note.text),
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
