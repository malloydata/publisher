// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { SkillController } from "../controller/skill.controller";
import {
   __setPackageLoadPoolForTests,
   PackageLoadPool,
} from "../package_load/package_load_pool";
import { Package } from "../service/package";
import {
   SET,
   writeWorkflow,
} from "../service/skills/__fixtures__/skill-directory/fixture";

/**
 * The full-view REST replies for the fixture package, as the server serves them: the skill set
 * `house` and the workflow `analyst` that lists it. The package is loaded through the real load
 * pool and read through the real controller, so a consumer's golden check runs against what the
 * server answers and not against a copy. `source.servedRevision` is different on every load.
 */
export async function skillDirectoryReplies() {
   const dir = fs.mkdtempSync(
      path.join(os.tmpdir(), "skill-directory-replies-"),
   );
   try {
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      fs.writeFileSync(
         path.join(dir, "model.malloy"),
         `source: t is duckdb.sql("select 1 as id")\n`,
      );
      const declared = writeWorkflow(dir);
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", version: "1", ...declared }),
      );
      const connection = new DuckDBConnection("duckdb", ":memory:");
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () =>
            new FixedConnectionMap(new Map([["duckdb", connection]]), "duckdb"),
      );
      try {
         const pkg = await Package.create("env", "pkg", dir, config);
         const controller = new SkillController({
            getEnvironment: async () => ({ getPackage: async () => pkg }),
         } as never);
         return {
            skillSet: await controller.getSkillSet("env", "pkg", SET, "full"),
            workflow: await controller.getWorkflow(
               "env",
               "pkg",
               "analyst",
               "full",
            ),
         };
      } finally {
         await connection.close();
      }
   } finally {
      await __setPackageLoadPoolForTests(null);
      fs.rmSync(dir, { recursive: true, force: true });
   }
}
