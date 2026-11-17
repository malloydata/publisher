// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A composite source (`compose(a, b)`) resolves a query to the first member
 * that can answer it, and that member's own `where:` is part of the answer.
 * The served copy compiles the author's text over re-rooted tables, where a
 * stored column may be redeclared (a big integer read natively), and Malloy
 * may then resolve the same query to another member: a tenant filter on the
 * first member dropped, every tenant's rows returned. A served answer is kept
 * only when it resolves to the member live resolves to.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import { LegDecline } from "../domain/leg_decline";
import { readPreparedQuery } from "../../governance/ir/prepared_query";

/**
 * A member as both compiles write it: its source, what it extends, and its
 * filters' text, all from the author's own text, so the same member reads the
 * same in the author's model and in the served copy.
 */
function memberIdentity(member: unknown): string {
   const m = member as {
      sourceID?: unknown;
      extends?: unknown;
      filterList?: { code?: unknown }[];
   };
   return JSON.stringify([
      typeof m.sourceID === "string" ? m.sourceID : null,
      typeof m.extends === "string" ? m.extends : null,
      (m.filterList ?? []).map((f) =>
         typeof f?.code === "string" ? f.code : null,
      ),
   ]);
}

/** The member a compiled query resolved its composite to, `null` for none. */
async function resolvedMember(
   runnable: QueryMaterializer,
): Promise<string | null> {
   const view = await readPreparedQuery(runnable);
   const resolved = view.compositeResolvedSourceDef;
   return resolved === undefined || resolved === null
      ? null
      : memberIdentity(resolved);
}

/**
 * Throw a decline unless `served` resolves its composite to the member
 * `author` (the live compile) resolves it to. Read only when live resolves
 * one: the served copy is the author's text, so a run target that is not a
 * composite live is not one there. Fails closed: a compile whose resolution
 * cannot be read is declined.
 */
export async function assertSameCompositeMember(
   author: QueryMaterializer,
   served: QueryMaterializer,
): Promise<void> {
   let same: boolean;
   try {
      const live = await resolvedMember(author);
      if (live === null) return;
      same = live === (await resolvedMember(served));
   } catch {
      same = false;
   }
   if (!same) {
      throw new LegDecline(
         "served_copy_not_served",
         "the query's composite source resolves to another member over the stored tables than live",
      );
   }
}
