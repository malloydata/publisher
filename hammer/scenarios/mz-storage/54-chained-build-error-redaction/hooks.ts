// Operator-style fault injection for code-review finding #1: drop the isolated
// DuckLake catalog DB out-of-band (no role/creds change — safe for other scenarios
// and the harness's own Postgres client), then assert the resulting chained-build
// error does not echo the catalog connection password. RED if the Tier-3 path
// leaks the connstring (it lacks the redactConnectionSecrets the Tier-2 path has).

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
   // The connstring form of the secret (`password=<value>`) — distinguishes a real
   // connstring leak from a benign mention of the value elsewhere (the harness
   // container name / temp path contains "hammer", so the bare value is not a
   // usable needle here). Passes today: the strict reference-miss error is clean.
   assert.excludes(
      "chained build error must not echo the catalog connstring (finding #1)",
      errText,
      `password=${api.pg.password}`,
   );
}
