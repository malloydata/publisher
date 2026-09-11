// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Operator-style fault injection: drop the isolated DuckLake catalog DB
// out-of-band (no role/creds change — safe for other scenarios and the harness's
// own Postgres client), so the next build fails. The redaction assertion itself is
// in the markdown (`excludes:` on the refusal step).

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

export async function dropCatalog(api: HookApi, assert: Assert): Promise<void> {
   // Catalog databases are per-scenario, so ask for this one's rather than
   // hardcoding the layout.
   await api.pg.dropDb(api.catalogDbFor("cbelake"));
   assert.ok("isolated catalog DB dropped out-of-band", true);
}
