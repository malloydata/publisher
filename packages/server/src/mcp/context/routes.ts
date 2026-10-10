// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The routes to a field: every other source that reaches the field's source
 * by joins, and the dotted name the field has there (`order_items` reaches
 * `products.category` as `products.category`). A field the catalog holds once
 * is queryable under each of these names, and each is a different population:
 * a Malloy join is a left join, so the copy reached from `order_items` counts
 * products that were ordered, while `products.category` counts every product.
 * The response lists the routes rather than picking one.
 */

import { entityId } from "../tools/get_context_tool";
import type { Catalog, CatalogNode } from "./catalog";

export interface Route {
   /** Catalog key of the source the field is reached from. */
   from: string;
   /** Join aliases from that source. */
   path: string[];
   /** The wire id of the field under that route. */
   id: string;
}

/** Deepest route listed, the depth the shipped index gives joined copies. */
export const ROUTE_MAX_DEPTH = 2;

const reverseCache = new WeakMap<
   Catalog,
   Map<string, Array<{ from: string; path: string[] }>>
>();

/** For each source, every (source, path) that reaches it, shortest first. */
function reverseJoins(catalog: Catalog) {
   let rev = reverseCache.get(catalog);
   if (rev) return rev;
   rev = new Map();
   for (const [from, hops] of catalog.joins) {
      for (const hop of hops) {
         if (hop.to === from) continue;
         const list = rev.get(hop.to) ?? [];
         list.push({ from, path: hop.path });
         rev.set(hop.to, list);
      }
   }
   for (const list of rev.values()) {
      list.sort(
         (a, b) =>
            a.path.length - b.path.length ||
            (a.from < b.from ? -1 : a.from > b.from ? 1 : 0) ||
            (a.path.join(".") < b.path.join(".") ? -1 : 1),
      );
   }
   reverseCache.set(catalog, rev);
   return rev;
}

/** Every route to a dimension or measure, up to `depth` joins; none for other kinds. */
export function routeList(
   catalog: Catalog,
   node: CatalogNode,
   depth = ROUTE_MAX_DEPTH,
): Route[] {
   if (node.kind !== "dimension" && node.kind !== "measure") return [];
   if (node.joinPath) return [];
   const out: Route[] = [];
   for (const r of reverseJoins(catalog).get(node.sourceKey) ?? []) {
      if (r.path.length > depth) continue;
      const source = catalog.sources.get(r.from);
      if (!source) continue;
      out.push({
         from: r.from,
         path: r.path,
         id: entityId(
            node.kind,
            source.name,
            `${r.path.join(".")}.${node.name}`,
         ),
      });
   }
   return out;
}

/**
 * The ids of every route, for a trace: the joins that reach the field, and,
 * with `collapseCopies`, the field's copies in extensions and their routes,
 * which are the same field under other names.
 */
export function routesOf(catalog: Catalog, node: CatalogNode): string[] {
   const out = routeList(catalog, node).map((r) => r.id);
   for (const key of node.copies ?? []) {
      const copy = catalog.nodes.get(key);
      if (!copy) continue;
      out.push(copy.id, ...routeList(catalog, copy).map((r) => r.id));
   }
   return out;
}
