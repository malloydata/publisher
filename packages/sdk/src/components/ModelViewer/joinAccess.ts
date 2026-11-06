// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a join's target source is a place you can go, or only something you
 * can read through this join.
 *
 * A package curates what it serves: `explores` in the manifest picks the model
 * files, `export {}` picks the sources within them, and the publisher omits
 * everything else from `listModels`/`sourceInfos`. Those omitted sources still
 * compile, import and join — a served source can legitimately join one — but
 * they are not addressable, and with the default `queryableSources: "declared"`
 * they are not directly queryable either.
 *
 * The join metadata comes from a host's entities, which may record every join
 * the model compiles, curated or not. So the target of a join is not evidence
 * that the target has a page. The browsable set — what the publisher actually
 * served us — is.
 *
 * Showing the joined source's fields inline is a separate question and stays
 * allowed: those fields are reachable through the join path at query time
 * anyway, so the curation boundary is about entry points, not field secrecy.
 */
export type JoinAccess =
   /** Served as its own source: the row gets its "open" arrow. */
   | { kind: "link" }
   /** Not served as its own source: reachable only through this join. */
   | { kind: "join-only" }
   /** Not established yet — say nothing rather than guess. */
   | { kind: "unknown" };

export function joinAccess(
   target: { modelPath: string; sourceName: string } | undefined,
   /**
    * modelPath → the source names that model served, for every model whose
    * source list has settled successfully. A model still in flight or whose
    * fetch failed is simply absent (`unknown`), so a timeout cannot look like
    * curation.
    */
   browsableByModel: ReadonlyMap<string, ReadonlySet<string>>,
): JoinAccess {
   if (!target?.modelPath || !target.sourceName) return { kind: "unknown" };
   const served = browsableByModel.get(target.modelPath);
   if (!served) return { kind: "unknown" };
   return served.has(target.sourceName)
      ? { kind: "link" }
      : { kind: "join-only" };
}

/** A resolved join target as the row renders it: the access plus, for a
 *  linkable one, where the arrow goes — an `href` to navigate to, or `onOpen`
 *  when the target is a selection on the page already showing (a join to
 *  another source of the same model). */
export type JoinLink = JoinAccess & { href?: string; onOpen?: () => void };
