// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";

/**
 * What a reader would see if the DOCUMENT were the file: the controls, and
 * each tile's query — so the builder's live view follows an edit the moment it
 * is made, rather than the file on the server's disk.
 *
 * The live view used to run from that file. The control row was the server's
 * manifest and each tile ran `overview -> revenue_trend`, the view as saved,
 * with whatever bindings it had when the package loaded. So unbinding a tile
 * changed the document and, after a save, the text — and the live tile kept
 * its disk bindings and kept filtering; removing a control edited the
 * document, and the live filter box stayed the server's list. The edit looked
 * like it did nothing, because on screen it did nothing.
 *
 * Two functions, one per half of what a reader sees. Both are pure, so the
 * host can hold the live document and derive its preview from it. A given the
 * document declares but the server's model has not compiled cannot be SENT to
 * it — the server refuses a given it does not know — so a new control's value
 * is written into the query as a literal instead, and the new filter works the
 * moment it is added rather than after a save.
 */

/**
 * The controls this document shows: every given some tile binds, this file's
 * own declarations first in declaration order, then the model's.
 *
 * That is the reader's rule — only a given some tile reads becomes a control
 * — applied to the document instead of the manifest. A local declaration is
 * built into a `Given` from its control contract; a model given is the
 * manifest's own spec, which carries what the file cannot say (`givenNames` a
 * suggest is gated by, for one).
 */
export function previewGivens(
   document: DashboardDocument,
   modelSpecs: readonly Given[],
): Given[] {
   const bound = new Set<string>();
   for (const tile of document.tiles)
      for (const filter of tile.filters ?? []) bound.add(filter.given);

   const out: Given[] = [];
   const seen = new Set<string>();
   for (const local of document.localGivens ?? []) {
      if (!bound.has(local.name)) continue;
      seen.add(local.name);
      out.push(givenFromLocal(local));
   }
   for (const spec of modelSpecs) {
      if (
         spec.name === undefined ||
         seen.has(spec.name) ||
         !bound.has(spec.name)
      )
         continue;
      seen.add(spec.name);
      out.push(spec);
   }
   return out;
}

/** A `given:` this file declares, as the `Given` the control row renders. */
function givenFromLocal(local: LocalGiven): Given {
   return {
      name: local.name,
      type: local.type,
      default: local.default,
      ...(local.label === undefined ? {} : { label: local.label }),
      ...(local.description === undefined
         ? {}
         : { description: local.description }),
      ...(local.control === undefined
         ? {}
         : { control: local.control as Given["control"] }),
      ...(local.suggest
         ? {
              suggest: {
                 ...(local.suggest.source === undefined
                    ? {}
                    : { source: local.suggest.source }),
                 ...(local.suggest.query === undefined
                    ? {}
                    : { query: local.suggest.query }),
                 dimension: local.suggest.dimension,
              },
           }
         : {}),
      ...(local.rangeMin === undefined ? {} : { rangeMin: local.rangeMin }),
      ...(local.rangeMax === undefined ? {} : { rangeMax: local.rangeMax }),
   };
}

/** A tile's query as the document has it, and the givens it sends. */
export interface PreviewTileQuery {
   /** A run expression, without `run:` — what `DashboardTile.tile` takes. */
   expression: string;
   /**
    * The givens this tile binds and the server can take, for narrowing the
    * request. Undefined for a tile whose bindings live in the model, where the
    * document cannot know them: send the whole row, as the reader does.
    */
   givenNames: string[] | undefined;
}

/**
 * A tile's query with the DOCUMENT's bindings, run against the model source
 * the dashboard's extension is built on.
 *
 * `overview -> revenue_trend` would run the view AS SAVED, bindings and all.
 * `order_items -> sales_by_month + { where: category ~ $CATEGORY }` is the same
 * view on the source it extends, refined by what the document binds now —
 * which is exactly the declaration the writer would produce, run before it is
 * written. Every reference tile has such a base: `view: x is base_view + …` can
 * only name a view of the source the extension extends.
 *
 * `runnable` is the set of givens the server's model declares; a binding to
 * one of them is written as `$NAME` and the given is sent with the request. A
 * binding to a given outside it — one this document just declared, which the
 * server would refuse by name — is written with its VALUE as a literal,
 * `where: brand ~ f'Nike'`, from `values` and the declaration's type; with no
 * value yet it is left out, which is what an empty filter means. A binding to
 * a given neither side knows is left out too.
 */
export function previewTileQuery(
   document: DashboardDocument,
   tile: DashboardTile,
   runnable: ReadonlySet<string>,
   values: ReadonlyMap<string, GivenValue> = new Map(),
): PreviewTileQuery {
   if (tile.declaration.kind !== "reference") {
      // Inherited: the model's view, bindings in the model. Inline: this file's
      // query body, which the document does not model and cannot rebind.
      return {
         expression: `${tile.source} -> ${tile.name}`,
         givenNames: tile.declaration.kind === "inherited" ? undefined : [],
      };
   }
   const base =
      document.sources.find((source) => source.name === tile.source)?.base ??
      tile.source;
   const localTypes = new Map(
      (document.localGivens ?? []).map((local) => [local.name, local.type]),
   );
   const sent: string[] = [];
   const clauses: string[] = [];
   for (const filter of tile.filters ?? []) {
      const comparison = `where: ${filter.field} ${filter.op ?? "~"}`;
      if (runnable.has(filter.given)) {
         sent.push(filter.given);
         clauses.push(`${comparison} $${filter.given}`);
         continue;
      }
      if (!localTypes.has(filter.given)) continue;
      const literal = malloyLiteral(
         localTypes.get(filter.given),
         values.get(filter.given),
      );
      if (literal !== undefined) clauses.push(`${comparison} ${literal}`);
   }
   const refinement = clauses.join(", ");
   return {
      expression:
         `${base} -> ${tile.declaration.from}` +
         (refinement ? ` + { ${refinement} }` : ""),
      givenNames: sent,
   };
}

/**
 * A control's value as the Malloy literal a given of `type` would hold, or
 * undefined when there is nothing to write: no value, or one the type cannot
 * spell. `filter<…>` is a filter expression, `f'…'`; a date is `@2024-01-31`;
 * a timestamp `@2024-01-31 09:30:00`; a string is quoted, a number and a
 * boolean are themselves.
 */
export function malloyLiteral(
   type: string | undefined,
   value: GivenValue | undefined,
): string | undefined {
   if (value === undefined || value === null || value === "") return undefined;
   const quote = (text: string) =>
      `'${text.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
   const scalar = type?.startsWith("filter<") ? "filter" : type;
   switch (scalar) {
      case "filter":
         return `f${quote(String(value))}`;
      case "string":
         return quote(String(value));
      case "number": {
         const n = typeof value === "number" ? value : Number(value);
         return Number.isFinite(n) ? String(n) : undefined;
      }
      case "boolean":
         return value === true || value === "true" ? "true" : "false";
      case "date":
         return datePart(value) && `@${datePart(value)}`;
      case "timestamp": {
         const day = datePart(value);
         if (!day) return undefined;
         const time =
            value instanceof Date
               ? value.toISOString().slice(11, 19)
               : (/[T ](\d{2}:\d{2}(?::\d{2})?)/.exec(String(value))?.[1] ??
                 "00:00:00");
         return `@${day} ${time}`;
      }
      default:
         return undefined;
   }
}

/** `2024-01-31` out of a Date (UTC) or an ISO-ish string, else undefined. */
function datePart(value: GivenValue): string | undefined {
   if (value instanceof Date)
      return Number.isNaN(value.getTime())
         ? undefined
         : value.toISOString().slice(0, 10);
   return /^(\d{4}-\d{2}-\d{2})/.exec(String(value))?.[1];
}
