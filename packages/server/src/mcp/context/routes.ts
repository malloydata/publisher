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
 *
 * How deep a route goes is the catalog's `routeDepth`: two joins with
 * `joinedCopies: "indexed"` (the depth the shipped index gives joined
 * copies), any depth the join walk recorded with `"routes"`. In routes mode a
 * field of a raw-table join on its own source (`date_dim.year` on `orders`)
 * has routes too: `orders.date_dim.year` from a source that reaches `orders`
 * through `orders`.
 */

import { entityId } from "../tools/get_context_tool";
import { ROUTE_MAX_DEPTH, type Catalog, type CatalogNode } from "./catalog";

export { ROUTE_MAX_DEPTH };

export interface Route {
   /** Catalog key of the source the field is reached from. */
   from: string;
   /** Join aliases from that source. */
   path: string[];
   /** The wire id of the field under that route. */
   id: string;
}

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

/**
 * Every route to a dimension or measure, up to `depth` joins (the catalog's
 * routeDepth by default); none for other kinds. In indexed mode a joined
 * copy has none (it is a route itself).
 */
export function routeList(
   catalog: Catalog,
   node: CatalogNode,
   depth = catalog.routeDepth ?? ROUTE_MAX_DEPTH,
): Route[] {
   if (node.kind !== "dimension" && node.kind !== "measure") return [];
   if (node.joinPath && catalog.joinedCopies !== "routes") return [];
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
 * The routes to a field from the given sources only, shortest first: what
 * routes mode shows in the judge's list and in the response, where `from`
 * is the other sources on the list or in the response.
 */
export function routesFrom(
   catalog: Catalog,
   node: CatalogNode,
   from: ReadonlySet<string>,
): Route[] {
   return routeList(catalog, node).filter(
      (r) => r.from !== node.sourceKey && from.has(r.from),
   );
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
