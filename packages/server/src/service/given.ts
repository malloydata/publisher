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

/**
 * Duck-typed shape of a Malloy SDK `Given` instance (the value type
 * of `Model.givens`).
 */
export interface MalloyGiven {
   readonly name: string;
   readonly type: { type: string; filterType?: string };
   getTaglines(prefix?: RegExp): string[];
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
 * - `default` / `defaultText` — Malloy's API only exposes the
 *   parsed `ConstantExpr` AST, not a rendered source string.
 *   Rendering it here would duplicate the Malloy printer. Add
 *   when Malloy surfaces a stringified accessor.
 *
 * `annotations` is restricted to `#(...)` declaration annotations
 * (the caller-facing kind, e.g. `#(doc)`). `getTaglines()` with no
 * prefix would also return `##` doc-comment lines and the
 * model-level `##!` pragma, which aren't part of the given's
 * surface contract.
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
      annotations: given.getTaglines(/^#\(/),
   };
}
