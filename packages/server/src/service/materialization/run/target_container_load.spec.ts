// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// F-03 through a real package load (the load worker reads its containers
// on the main thread): a database `#@ persist` whose `name=` names a schema
// its warehouse does not hold is refused at plan as `container_not_found`,
// with a `persist-not-built` warning naming the schema, rather than planned
// and failing at build; one in a schema that exists is planned.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "path";
import { __setPackageLoadPoolForTests } from "../../../package_load/package_load_pool";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../test_helpers/temp_storage";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";

const ENV = "e1";
const PKG = "pc";

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: {
      setupSQL:
         "CREATE SCHEMA IF NOT EXISTS present; CREATE TABLE IF NOT EXISTS ev AS SELECT * FROM (VALUES (1, 'east'), (2, 'west')) AS t(id, region)",
   },
} as const;

const MODEL = `##! experimental.persistence
source: ev is warehouse.table('ev')

#@ persist name="no_such_schema.by_region"
source: missing is ev -> { group_by: region }

#@ persist name="present.by_region"
source: placed is ev -> { group_by: region }
`;

describe("F-03: a package load refuses a table whose container does not exist", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let env: Environment | undefined;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      ({ root, envPath, db } = await openTempStorage("target-container-", {
         id: "env-1",
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restoreSettings();
      await __setPackageLoadPoolForTests(null);
   });

   it("refuses it as container_not_found, naming the schema, and plans its sibling", async () => {
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage(PKG);
      const loaded = await env.getPackage(parsePackageName(PKG), false);
      const plan = loaded.getBuildPlan();
      expect(Object.values(plan?.sources ?? {}).map((s) => s.name)).toEqual([
         "placed",
      ]);
      const refused = Object.values(plan?.refusedSources ?? {});
      expect(refused.map((r) => [r.name, r.reason, r.tier])).toEqual([
         ["missing", "container_not_found", "database"],
      ]);
      expect(refused[0].message).toContain('schema "no_such_schema"');
      const warning = (loaded.getPackageMetadata().warnings ?? []).find(
         (w) => w.code === "persist-not-built" && w.subject === "missing",
      );
      expect(warning?.message).toContain('schema "no_such_schema"');
      expect(warning?.message).toContain("or change name=");
   });
});
