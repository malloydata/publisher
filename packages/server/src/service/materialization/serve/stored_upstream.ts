// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which stored upstream a column a walk reached is read from, so a string
 * read is judged against that upstream's recorded contract alone and never
 * against another stored table that happens to hold a column of the same
 * name. Pure.
 */

/**
 * Resolves the owner of a column (the source whose fields hold it) to the
 * sourceID in `stored` whose table holds it: the owner's own ID, the stored
 * source it `extend`s (however many extends deep), or the stored
 * query-shaped source whose query it carries unchanged. Undefined when the
 * owner reaches none, which a judge of the read must refuse.
 */
export function storedUpstreamResolver(
   modelDef: unknown,
   stored: ReadonlySet<string>,
): (owner: unknown) => string | undefined {
   let byID: Map<string, unknown> | undefined;
   let byQuery: Map<string, string> | undefined;
   const index = () => {
      byID = new Map();
      byQuery = new Map();
      const contents =
         (modelDef as { contents?: Record<string, unknown> } | undefined)
            ?.contents ?? {};
      for (const c of Object.values(contents)) {
         const d = c as { sourceID?: unknown; query?: unknown } | undefined;
         if (typeof d?.sourceID !== "string") continue;
         byID.set(d.sourceID, d);
         // `query` first: the stored set is asked only about the sources
         // that could answer (scope_facts.ts reads what a walk asked).
         if (d.query !== undefined && stored.has(d.sourceID)) {
            byQuery.set(JSON.stringify(d.query), d.sourceID);
         }
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
            const id = byQuery!.get(JSON.stringify(at.query));
            if (id !== undefined) return id;
         }
         if (typeof at.extends !== "string") return undefined;
         if (stored.has(at.extends)) return at.extends;
         at = byID!.get(at.extends) as typeof at;
      }
      return undefined;
   };
}
