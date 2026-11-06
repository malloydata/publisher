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
   loadTempPackage,
   type LoadedTempPackage,
} from "../test_helpers/temp_package";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { unrecordedAudit } from "./access_record";

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

const GIVENS = "##! experimental.givens\n";

const rows = (sql: string) => `duckdb.sql("${sql}")`;

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
   const { compactResult } = await model.getQueryResults(
      unrecordedAudit(),
      undefined,
      undefined,
      query,
      undefined,
      true,
      givens,
   );
   return compactResult as unknown as Array<Record<string, unknown>>;
}

describe("a plain scalar given shared through an imported metadata model", () => {
   const REPORTS = rows(
      "select * from (values (1,'a'),(2,'b'),(1,'c')) as t(internal_report_id, label)",
   );
   const model = (name: string, extra = "") => `import "metadata.malloy"

#(access_filter) internal_report_id = $INTERNAL_REPORT_ID
source: ${name} is ${REPORTS} extend {
   measure: n is count()
${extra}}
`;

   const files = () => ({
      "metadata.malloy": `${GIVENS}given: INTERNAL_REPORT_ID :: number\n`,
      "campaigns.malloy": model("campaigns"),
      "creatives.malloy": model("creatives"),
      "placements.malloy": `import "metadata.malloy"
import "campaigns.malloy"

#(access_filter) internal_report_id = $INTERNAL_REPORT_ID
source: placements is ${REPORTS} extend {
   join_one: c is campaigns on c.internal_report_id = internal_report_id
   measure: n is count()
}
`,
      "summary.malloy": model("summary"),
   });

   it("loads, filters to the supplied report, and refuses a caller with none", async () => {
      const pkg = await load(files());
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
   const ORGS = rows(
      "select * from (values ('o1', 1), ('o2', 2), ('o1', 3)) as t(org_id, v)",
   );

   it("loads, filters to the org, and refuses a caller with none", async () => {
      const sources = Array.from({ length: 8 }, (_, i) => i)
         .map(
            (i) => `#(access_filter) org_id = $AFFINITY_ORG_ID
source: s${i} is ${ORGS} extend { measure: n is count() }
`,
         )
         .join("\n");
      const pkg = await load({
         "index.malloy": `${GIVENS}given: AFFINITY_ORG_ID :: string\n\n${sources}`,
      });
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
   const CAMPS = rows(
      "select * from (values (1, 'x'), (2, 'y'), (3, 'z')) as t(camp_id, label)",
   );
   const INDEX = `${GIVENS}given: CAMP_ID :: number
given: CAMPS :: number[]
given: ROLES :: string[]

#(access_filter) camp_id = $CAMP_ID
source: one_camp is ${CAMPS} extend { measure: n is count() }

#(access_filter) camp_id in $CAMPS
source: many_camps is ${CAMPS} extend { measure: n is count() }

#(authorize) 'director' in $ROLES
source: directors is ${CAMPS} extend { measure: n is count() }

#(access_filter) camp_id = $CAMP_ID
source: audit is ${CAMPS} extend {
   join_one: c is one_camp on c.camp_id = camp_id
   measure: n is count()
}
`;

   it("loads, filters each gate, and refuses a caller missing its given", async () => {
      const pkg = await load({ "index.malloy": INDEX });
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
