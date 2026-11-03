// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `gate-insecure-given`: a published source whose `#(authorize)` or
 * `#(access_filter)` reads a given that is not declared `#(secure)`.
 *
 * A plain given is whatever the request supplies, so a gate keyed on one is a
 * filter the caller can set, with a policy-shaped name. A `#(secure)` given is
 * the host's to set: the trusted tier in front of Publisher resolves it from
 * the caller's verified identity and replaces anything the request carried
 * (docs/materialization.md, "Where the per-caller value comes from"). Only a
 * gate keyed on secure givens is a control, so the load says so, once per
 * published source, naming the givens to mark. A warning, not a refusal: the
 * model still serves, and a deployment with no trusted tier in front has no
 * secure givens to mark.
 */

import type { components } from "../api";
import { referencedGivenNames } from "./authorize";
import type { Model } from "./model";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a published source gated on a given a caller can set. */
export const GATE_INSECURE_GIVEN = "gate-insecure-given";

const isSentinel = (expr: string) =>
   ["false", "true"].includes(expr.trim().toLowerCase());

/** The warnings over a package's published models. */
export function gateGivenWarnings(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): ApiPackageWarning[] {
   const warnings: ApiPackageWarning[] = [];
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let apiSources: ReturnType<Model["getSources"]>;
      try {
         apiSources = model.getSources();
      } catch {
         continue;
      }
      for (const source of apiSources ?? []) {
         const secure = new Set(
            (source.givens ?? [])
               .filter((g) => g.secure === true && g.name)
               .map((g) => g.name as string),
         );
         const read = new Set<string>();
         for (const expr of [
            ...(source.authorize ?? []),
            ...(source.accessFilter ?? []),
         ]) {
            if (isSentinel(expr)) continue;
            for (const name of referencedGivenNames(expr)) read.add(name);
         }
         const insecure = Array.from(read)
            .filter((name) => !secure.has(name))
            .sort();
         if (insecure.length === 0) continue;
         warnings.push({
            model: modelPath,
            subject: source.name,
            code: GATE_INSECURE_GIVEN,
            severity: "warn",
            message:
               `Source ${source.name} is gated on $${insecure.join(", $")}, ` +
               `which ${insecure.length === 1 ? "is" : "are"} not declared ` +
               `#(secure), so a caller can set ${insecure.length === 1 ? "it" : "them"} ` +
               `and the gate is a filter, not a control. Write "#(secure)" on ` +
               `the given: line so the trusted tier in front of Publisher sets ` +
               `it from the caller's verified identity (docs/authorize.md, ` +
               `Security model).`,
         });
      }
   }
   return warnings;
}
