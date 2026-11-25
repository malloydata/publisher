// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Model } from "../service/model";
import { refreshStringCapabilities } from "../service/materialization/serve/source_capability";

/**
 * Read how `model`'s sources compare strings, as every bind commit does, for
 * a spec that sets a model's bindings directly (`setServeBindings`) with no
 * bind: read admission's lookups judge its answers by it.
 */
export async function recordModelCapabilities(model: Model): Promise<void> {
   model.setStringCapabilityReader(refreshStringCapabilities);
   await model.refreshStringCapabilities();
}

/** What a package's served plan refuses, by source name. */
type PlanRefusals = Record<string, { reason: string; message: string }>;

/**
 * What `pkg`'s served build plan refuses, by source name, once no refusal
 * waits on a string capability not read yet: the load's plan is derived
 * before its models' capabilities are read, and derived again when they
 * land. Throws past `timeoutMs`.
 */
export async function planRefusalsOnceRead(
   pkg: {
      getBuildPlan(): unknown;
      whenBuildPlanCurrent(timeoutMs?: number): Promise<void>;
   },
   timeoutMs = 30_000,
): Promise<PlanRefusals> {
   const deadline = Date.now() + timeoutMs;
   for (;;) {
      await pkg.whenBuildPlanCurrent(timeoutMs);
      const plan = pkg.getBuildPlan() as {
         refusedSources?: Record<
            string,
            { name: string; reason: string; message: string }
         >;
      } | null;
      const refused = Object.values(plan?.refusedSources ?? {});
      if (!refused.some((r) => r.message.includes("has not been read yet"))) {
         return Object.fromEntries(refused.map((r) => [r.name, r]));
      }
      if (Date.now() > deadline) {
         throw new Error("the plan still waits on a string capability");
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
   }
}
