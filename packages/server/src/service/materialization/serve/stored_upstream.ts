// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which stored upstream a column a walk reached is read from, so a string
 * read is judged against that upstream's recorded contract alone and never
 * against another stored table that happens to hold a column of the same
 * name. Pure.
 */

/**
 * A set of source IDs as a walk may read it: membership only. A walk that
 * cannot take its size or iterate it gives the same answer over any set
 * holding none of the IDs it asked about, which `scope_facts.ts` relies on
 * to reuse one walk for many bound sets.
 */
export type IdMembership = { has(id: string): boolean };

const querySourcesMemo = new WeakMap<object, Map<string, string[]>>();

/**
 * The model's query-shaped sources by their query (as JSON), in model order.
 * Built once per model and never from a stored set, so a walk asks its
 * stored set only about the sources whose query it is looking up
 * (`scope_facts.ts` reuses a walk for every set holding none of them).
 */
export function querySourceIDs(modelDef: unknown): Map<string, string[]> {
   if (modelDef === null || typeof modelDef !== "object") return new Map();
   let out = querySourcesMemo.get(modelDef);
   if (out) return out;
   out = new Map();
   const contents =
      (modelDef as { contents?: Record<string, unknown> }).contents ?? {};
   for (const c of Object.values(contents)) {
      const d = c as { sourceID?: unknown; query?: unknown } | undefined;
      if (typeof d?.sourceID !== "string" || d.query === undefined) continue;
      const q = JSON.stringify(d.query);
      out.set(q, [...(out.get(q) ?? []), d.sourceID]);
   }
   querySourcesMemo.set(modelDef, out);
   return out;
}

/**
 * Resolves the owner of a column (the source whose fields hold it) to the
 * sourceID in `stored` whose table holds it: the owner's own ID, the stored
 * source it `extend`s (however many extends deep), or the stored
 * query-shaped source whose query it carries unchanged. Undefined when the
 * owner reaches none, which a judge of the read must refuse.
 */
export function storedUpstreamResolver(
   modelDef: unknown,
   stored: IdMembership,
): (owner: unknown) => string | undefined {
   let byID: Map<string, unknown> | undefined;
   const index = () => {
      byID = new Map();
      const contents =
         (modelDef as { contents?: Record<string, unknown> } | undefined)
            ?.contents ?? {};
      for (const c of Object.values(contents)) {
         const d = c as { sourceID?: unknown } | undefined;
         if (typeof d?.sourceID === "string") byID.set(d.sourceID, d);
      }
   };
   return (owner) => {
      let at = owner as
         | { sourceID?: unknown; extends?: unknown; query?: unknown }
         | undefined;
      for (let depth = 0; at !== undefined && depth < 32; depth++) {
         if (typeof at.sourceID === "string" && stored.has(at.sourceID)) {
            return at.sourceID;
         }
         if (byID === undefined) index();
         if (at.query !== undefined) {
            // The last stored one in model order, as the index kept before.
            const id = (
               querySourceIDs(modelDef).get(JSON.stringify(at.query)) ?? []
            ).findLast((x) => stored.has(x));
            if (id !== undefined) return id;
         }
         if (typeof at.extends !== "string") return undefined;
         if (stored.has(at.extends)) return at.extends;
         at = byID!.get(at.extends) as typeof at;
      }
      return undefined;
   };
}
