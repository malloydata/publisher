// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `PUBLISHER_REFUSAL_REASON_COMPAT`: a build plan as a response writes it.
 *
 * A client generated from the Publisher 0.12 API declares `RefusedSource.reason`
 * as a closed enum, and rejects the whole response that carries a reason it does
 * not know. During a rolling upgrade from 0.12 whose clients roll after the
 * server, `0.12` writes each reason 0.12 did not declare as
 * {@link REFUSAL_REASON_0_12_FALLBACK}, which 0.12 declared and no current plan
 * produces, so a mapped refusal is never mistaken for a real one. Its message
 * keeps the real reason, in brackets.
 *
 * Only responses change. The plan the server builds and serves from is the
 * object passed in, never modified; a client still sees every refused source,
 * which it needs: a refused `preaggregate` rollup stays in `sources`, and only
 * its `refusedSources` entry says not to build it.
 *
 * A transition setting: a later release removes it with this module.
 */

import type { components } from "../api";
import { getRefusalReasonCompat, type RefusalReasonCompat } from "../config";
import type { BuildPlan } from "./materialization/domain/types";

type RefusalReason = components["schemas"]["RefusedSource"]["reason"];

/** `RefusedSource.reason` as Publisher 0.12 declared it (`api-doc.yaml` at v0.12.6). */
export const REFUSAL_REASONS_0_12: ReadonlySet<RefusalReason> =
   new Set<RefusalReason>([
      "free_parameter",
      "given",
      "given_in_persisted_query",
      "dynamic_projection",
      "dynamic_join",
      "dynamic_joined_where",
      "partition_without_storage",
      "partition_column_unknown",
      "partition_column_not_public",
      "merge_key_scope_unresolved",
      "preaggregate_over_dynamic_source",
      "authorize",
      "not_duckdb_portable",
      "public_surface_unknown",
      "read_only_dialect",
   ]);

/**
 * What a reason 0.12 did not declare is written as under `0.12`: declared by 0.12,
 * retained in the current enum, and produced by no current plan.
 */
export const REFUSAL_REASON_0_12_FALLBACK: RefusalReason =
   "not_duckdb_portable";

/** Each plan's 0.12 form, computed once: `/status` writes every package's plan on every poll. */
const compat012 = new WeakMap<BuildPlan, BuildPlan>();

/**
 * `plan` as a response writes it under `mode` (the setting, by default). The same
 * object when nothing changes: under `off`, and under `0.12` for a plan whose
 * every reason 0.12 declared.
 */
export function compatBuildPlan<P extends BuildPlan | null | undefined>(
   plan: P,
   mode: RefusalReasonCompat = getRefusalReasonCompat(),
): P {
   if (mode === "off" || !plan) return plan;
   const cached = compat012.get(plan);
   if (cached) return cached as P;
   let refusedSources: NonNullable<BuildPlan["refusedSources"]> | undefined;
   for (const [sourceId, refused] of Object.entries(
      plan.refusedSources ?? {},
   )) {
      if (!refused || REFUSAL_REASONS_0_12.has(refused.reason)) continue;
      refusedSources ??= { ...plan.refusedSources };
      refusedSources[sourceId] = {
         ...refused,
         reason: REFUSAL_REASON_0_12_FALLBACK,
         message: `[${refused.reason}] ${refused.message}`,
      };
   }
   const written = refusedSources ? { ...plan, refusedSources } : plan;
   compat012.set(plan, written);
   return written as P;
}
