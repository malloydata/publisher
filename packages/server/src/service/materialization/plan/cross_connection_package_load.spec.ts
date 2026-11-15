// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * #6982 through a real package load (the package-load worker): a model whose
 * files join sources on two origin connections loads from its build view with
 * the materialize switch on, names every definition it left out, and fails to
 * load, as before, with the switch off. The cross-connection join sits in an
 * imported file, so the importer's first compile also fails for the source it
 * could not find, which must not be left out with it.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../../package_load/package_load_pool";
import { installSettings } from "../../../test_helpers/settings_env";
import { writeTempPackage } from "../../../test_helpers/temp_package";
import { unrecordedAudit } from "../../access_record";
import { Package } from "../../package";

const BASE = `source: accounts is crm.sql("select 1 as id, 'acme' as account_name") extend {
  primary_key: id
}
source: usage is product.sql("select 1 as account_id, 5 as events")

source: account_usage is accounts extend {
  join_many: u is usage on u.account_id = id
  measure: total_events is u.events.sum()
}
`;

const MODEL = `import "base.malloy"

source: usage_report is account_usage extend {
  measure: n is count()
  view: by_account is { group_by: account_name; aggregate: total_events }
  view: counts is { group_by: account_name; aggregate: n }
}
`;

describe("a package joining sources across origin connections (#6982)", () => {
   let dir: string;
   let restore: (() => void) | undefined;
   let pool: PackageLoadPool | undefined;

   beforeEach(() => {
      dir = writeTempPackage(
         { "base.malloy": BASE, "model.malloy": MODEL },
         {
            prefix: "publisher-cross-origin-",
            manifest: { name: "pkg", description: "test package" },
         },
      ).dir;
   });

   afterEach(async () => {
      await __setPackageLoadPoolForTests(null);
      restore?.();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   async function load(materialize: boolean) {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: materialize ? "true" : "false" },
      }).restore;
      // The worker reads its settings when it starts.
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
      const crm = new DuckDBConnection("crm", ":memory:");
      const product = new DuckDBConnection("product", ":memory:");
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () =>
            new FixedConnectionMap(
               new Map([
                  ["crm", crm],
                  ["product", product],
               ]),
               "crm",
            ),
      );
      return Package.create("env", "pkg", dir, malloyConfig);
   }

   it("loads from its build view with the switch on, and names what it left out", async () => {
      const pkg = await load(true);
      const model = pkg.getModel("model.malloy");
      expect(model).toBeDefined();
      const left = model
         ?.crossConnectionLeftOutDefinitions()
         .map((l) => [l.keyword, l.name, l.crossConnection]);
      expect(left).toEqual([
         ["join_many", "u", true],
         ["measure", "total_events", false],
         ["view", "by_account", false],
      ]);
      const warnings = (pkg.getPackageMetadata().warnings ?? [])
         .filter((w) => w.code === "served-copy-not-served")
         .map((w) => w.message ?? "");
      expect(warnings.some((m) => m.includes("the join `u`"))).toBe(true);
      expect(
         warnings.some(
            (m) =>
               m.includes("`account_usage.total_events`") &&
               m.includes("`usage_report.by_account`"),
         ),
      ).toBe(true);
      // The importer's own source is kept, less only what read the join.
      const result = await model?.runQuery(unrecordedAudit(), {
         query: "run: usage_report -> counts",
      });
      expect(result?.compactResult).toEqual([{ account_name: "acme", n: 1 }]);
   });

   it("fails to load, as before, with the switch off", async () => {
      await expect(load(false)).rejects.toThrow(/Cannot join 'u'/);
   });
});
