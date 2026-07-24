// Operator-style fault injection: drop the isolated DuckLake catalog DB
// out-of-band (no role/creds change — safe for other scenarios and the harness's
// own Postgres client), then assert the resulting chained-build error does not
// echo the catalog connection password.

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

export async function dropCatalog(api: HookApi, assert: Assert): Promise<void> {
   // A declared `## Connection cbelake` gets catalog DB `ducklake_catalog_cbelake`.
   await api.pg.dropDb("ducklake_catalog_cbelake");
   assert.ok("isolated catalog DB dropped out-of-band", true);
}

export async function assertChainedErrorRedacted(
   api: HookApi,
   assert: Assert,
): Promise<void> {
   const on = await api.use("on");
   const mats = await on.listMaterializations("cbe");
   const failed = mats
      .filter((m) => m.status === "FAILED")
      .sort((a, b) => String(b.id).localeCompare(String(a.id)))[0];
   assert.ok(
      "the strict chained build failed (Tier-3 error path exercised)",
      !!failed,
      `statuses: ${JSON.stringify(mats.map((m) => m.status))}`,
   );
   const errText = String(failed?.error ?? "");
   // Match the connstring form (`password=<value>`), not the bare value: the
   // harness container name and temp paths contain the value, so a bare needle
   // would false-positive.
   assert.excludes(
      "chained build error must not echo the catalog connstring",
      errText,
      `password=${api.pg.password}`,
   );
}
