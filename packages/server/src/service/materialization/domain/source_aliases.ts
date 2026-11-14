// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which names share one stored table, and which of a plan's sources a
 * manifest entry stands for, matched by name and content address.
 *
 * Read by the build's plan facts (run/materialization_run_support.ts) and by
 * the facade when it derives serve bindings. A name declared at more than one
 * address is never an alias; a source matches an entry only at the exact
 * address its plan computed.
 *
 * Pure.
 */

/**
 * The package's own sourceIDs for a source name at a content address, read from
 * the build plan's sources (keyed by sourceID).
 *
 * Both sides of the comparison come from this server's compile: the plan's
 * sourceIDs here, and each model's `contents[name].sourceID` in
 * {@link bindingServesModelSource}. A sourceID embeds an absolute model URL, so
 * one from another process would not compare; matching the manifest entry to
 * the plan by name and content address is what lets a binding the host supplied
 * carry this server's identity.
 *
 * Only an exact match counts: a plan source of that name AT that content
 * address. Every manifest entry carries the content address this server
 * computes (an instructed build keys its entry by it, whatever identity the
 * instruction used), so an entry at another address was built from another
 * definition: a source edited under its old name in an unversioned package, a
 * manifest restored from before the edit, or one from a release that addressed
 * the source differently. Its table holds that definition's rows, and its
 * columns may well be the same, so it serves nothing and the source answers
 * live until its own table is built. An empty list matches no model's source.
 */
export function planSourceIds(
   planSources: Record<string, { name?: string; sourceEntityId?: string }>,
): (sourceName: string, sourceEntityId: string) => string[] {
   const byName = new Map<string, { sourceID: string; address?: string }[]>();
   for (const [sourceID, source] of Object.entries(planSources)) {
      if (!source.name) continue;
      const named = byName.get(source.name) ?? [];
      named.push({ sourceID, address: source.sourceEntityId });
      byName.set(source.name, named);
   }
   return (sourceName, sourceEntityId) => {
      return (byName.get(sourceName) ?? [])
         .filter((s) => s.address === sourceEntityId)
         .map((s) => s.sourceID);
   };
}

/**
 * For each persist source in a build plan, the sources that materialize into its
 * table — the `aliasesBySourceName` argument {@link deriveServeBindings} takes.
 *
 * Grouped by content address (which decides who shares a table), then keyed by
 * name, the only identifier a manifest entry carries that means the same thing
 * whoever built it (an instructed build stamps the caller's `sourceEntityId`).
 *
 * A name declared at more than one address is dropped from aliasing: names are
 * not unique in a package, and binding one name to two tables puts two `source:`
 * declarations in the package-wide serve shape, which fails to compile and
 * silently takes the storage tier out for every model. Dropping costs only that
 * source its routing. The source that OWNS the name still binds it (see
 * {@link deriveServeBindings}).
 */
export function groupAliasesByName(
   planSources: { name?: string; sourceEntityId?: string }[],
): Record<string, string[]> {
   const namesByAddress = new Map<string, string[]>();
   for (const source of planSources) {
      if (!source.sourceEntityId || !source.name) continue;
      const group = namesByAddress.get(source.sourceEntityId);
      if (!group) namesByAddress.set(source.sourceEntityId, [source.name]);
      else if (!group.includes(source.name)) group.push(source.name);
   }

   const addressesPerName = new Map<string, number>();
   for (const group of namesByAddress.values()) {
      for (const name of group) {
         addressesPerName.set(name, (addressesPerName.get(name) ?? 0) + 1);
      }
   }

   const byName: Record<string, string[]> = {};
   for (const group of namesByAddress.values()) {
      const unambiguous = group.filter(
         (name) => addressesPerName.get(name) === 1,
      );
      for (const name of unambiguous) byName[name] = unambiguous;
   }
   return byName;
}
