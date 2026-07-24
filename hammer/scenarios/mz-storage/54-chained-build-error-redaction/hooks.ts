// Operator-style fault injection: drop the isolated DuckLake catalog DB
// out-of-band (no role/creds change — safe for other scenarios and the harness's
// own Postgres client), so the next build fails. The redaction assertion itself is
// in the markdown (`excludes:` on the refusal step).

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

export async function dropCatalog(api: HookApi, assert: Assert): Promise<void> {
   // A declared `## Connection cbelake` gets catalog DB `ducklake_catalog_cbelake`.
   await api.pg.dropDb("ducklake_catalog_cbelake");
   assert.ok("isolated catalog DB dropped out-of-band", true);
}
