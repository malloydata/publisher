// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Packages that use only `#(authorize)` and `#(access_filter)`, shaped like
 * the ones deployed today, load with no refusal and their gates filter: a
 * caller who supplies the given reads only its rows, and one who omits a
 * required given is refused.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { expectAccessDenied } from "../test_helpers/authorize_fixture";
import {
   importedScalarGivenPackage,
   indexGivensPackage,
   stringGivenSiblingsPackage,
} from "../test_helpers/gate_shape_fixtures";
import {
   loadTempPackage,
   type LoadedTempPackage,
} from "../test_helpers/temp_package";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { unrecordedAudit } from "./access_record";
import { QueryPrivileges } from "./model_query_request";

beforeAll(async () => {
   await __setPackageLoadPoolForTests(new PackageLoadPool(1));
});
afterAll(async () => {
   await __setPackageLoadPoolForTests(null);
});

let temp: LoadedTempPackage | undefined;
afterEach(async () => {
   await temp?.cleanup();
   temp = undefined;
});

async function load(files: Record<string, string>) {
   temp = await loadTempPackage(files, { prefix: "publisher-shapes-" });
   const bad = (temp.pkg.getPackageMetadata().warnings ?? []).filter(
      (w) => w.code !== "join-bypass",
   );
   expect(bad).toEqual([]);
   return temp.pkg;
}

async function run(
   pkg: LoadedTempPackage["pkg"],
   modelPath: string,
   query: string,
   givens: Record<string, GivenValue>,
) {
   const model = pkg.getModel(modelPath);
   if (!model) throw new Error(`no model ${modelPath}`);
   const { compactResult } = await model.runQuery(
      unrecordedAudit(),
      { query, givens },
      QueryPrivileges.of({ bypassFilters: true }),
   );
   return compactResult as unknown as Array<Record<string, unknown>>;
}

describe("a plain scalar given shared through an imported metadata model", () => {
   it("loads, filters to the supplied report, and refuses a caller with none", async () => {
      const pkg = await load(importedScalarGivenPackage());
      expect(
         await run(
            pkg,
            "campaigns.malloy",
            "run: campaigns -> { group_by: label order_by: label }",
            { INTERNAL_REPORT_ID: 1 },
         ),
      ).toEqual([{ label: "a" }, { label: "c" }]);
      expect(
         await run(
            pkg,
            "placements.malloy",
            "run: placements -> { aggregate: n }",
            { INTERNAL_REPORT_ID: 2 },
         ),
      ).toEqual([{ n: 1 }]);
      await expectAccessDenied(
         run(pkg, "summary.malloy", "run: summary -> { aggregate: n }", {}),
      );
   });
});

describe("a plain string given gating eight sources", () => {
   it("loads, filters to the org, and refuses a caller with none", async () => {
      const pkg = await load(stringGivenSiblingsPackage(8));
      for (const i of [0, 7]) {
         expect(
            await run(pkg, "index.malloy", `run: s${i} -> { aggregate: n }`, {
               AFFINITY_ORG_ID: "o1",
            }),
         ).toEqual([{ n: 2 }]);
      }
      await expectAccessDenied(
         run(pkg, "index.malloy", "run: s3 -> { aggregate: n }", {}),
      );
   });
});

describe("scalar, list and role givens declared in the index model", () => {
   it("loads, filters each gate, and refuses a caller missing its given", async () => {
      const pkg = await load(indexGivensPackage());
      const n = async (source: string, givens: Record<string, GivenValue>) =>
         run(pkg, "index.malloy", `run: ${source} -> { aggregate: n }`, givens);
      expect(await n("one_camp", { CAMP_ID: 2 })).toEqual([{ n: 1 }]);
      expect(await n("many_camps", { CAMPS: [1, 3] })).toEqual([{ n: 2 }]);
      expect(await n("directors", { ROLES: ["director"] })).toEqual([{ n: 3 }]);
      expect(await n("audit", { CAMP_ID: 3 })).toEqual([{ n: 1 }]);
      await expectAccessDenied(n("one_camp", {}));
      await expectAccessDenied(n("many_camps", {}));
      await expectAccessDenied(n("directors", { ROLES: ["viewer"] }));
      await expectAccessDenied(n("audit", {}));
   });
});
