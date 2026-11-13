// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Markdown cannot read whole response bodies, so these two hooks scan every
// outward surface for the generated build source's name.

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

const PKG = "mzn";
const GENERATED = "__mat";

/**
 * A run record without the caller's own `sourceNames`, which the first hook
 * deliberately sets to the generated name and the record echoes back.
 */
function withoutRequest(value: unknown): string {
   return JSON.stringify(value, (k, v) =>
      k === "sourceNames" ? undefined : v,
   );
}

/** A scoped run naming the generated build source selects nothing. */
export async function scopedRunByGeneratedNameSelectsNothing(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const rest = await api.use("on");
   const run = await rest.createMaterialization(PKG, {
      sourceNames: ["mzn_orders__mat"],
   });
   const rec = await rest.pollMaterialization(PKG, run.id);
   const entries = await rest.latestManifestEntries(PKG).catch(() => ({}));
   assert.eq(
      "a run scoped by the generated name builds no table",
      Object.keys(entries).length,
      0,
   );
   assert.ok(
      "and its record never names the generated source",
      !withoutRequest(rec).includes(GENERATED),
      withoutRequest(rec),
   );
}

/** The wire plan, run records, manifest, warnings and path report. */
export async function noGeneratedNameOnAnySurface(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const rest = await api.use("on");
   const base = `${rest.baseUrl}/api/v0/environments/${rest.env}/packages/${PKG}`;
   const get = async (url: string) => (await fetch(url)).text();
   const surfaces: Record<string, string> = {
      "package (build plan and warnings)": await get(base),
      "run records": withoutRequest(await rest.listMaterializations(PKG)),
      manifest: JSON.stringify(await rest.latestManifestEntries(PKG)),
      status: await get(`${rest.baseUrl}/api/v0/status`),
   };
   const queried = await fetch(`${base}/models/mzn.malloy/query`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
         query: "run: mzn_orders -> { group_by: order_month; aggregate: total }",
      }),
   });
   const query = await queried.text();
   surfaces["query (path report)"] = query;
   assert.ok(
      "the path report names the stored table",
      query.includes('"materialized"'),
      query.slice(0, 2000),
   );
   for (const [name, body] of Object.entries(surfaces)) {
      assert.ok(
         `${name} never names the generated build source`,
         !body.includes(GENERATED),
         body.slice(
            Math.max(0, body.indexOf(GENERATED) - 300),
            body.indexOf(GENERATED) + 300,
         ),
      );
   }
   // The plan entry is the author's source, by its own declaration's sourceID.
   const plan = JSON.parse(surfaces["package (build plan and warnings)"]) as {
      buildPlan?: { sources?: Record<string, { name?: string }> };
   };
   const sources = Object.entries(plan.buildPlan?.sources ?? {});
   assert.eq(
      "one plan entry, the author's source",
      sources.map(([id, s]) => [id.split("@")[0], s.name]),
      [["mzn_orders", "mzn_orders"]],
   );
}
