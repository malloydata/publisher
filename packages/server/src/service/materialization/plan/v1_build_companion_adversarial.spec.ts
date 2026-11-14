// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 build copy (6d) against adversarial files.
 * Each unflagged file planned under `on` must plan exactly what the same file
 * with the flag written by its author plans under `off` (main's path).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   Runtime,
   type URLReader,
} from "@malloydata/malloy";
import { afterAll, afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   collectModelBuildPlan,
   computeSourceEntityId,
   emptyBuildPlanParts,
   type BuildPlanParts,
} from "./build_plan";
import { computePackageBuildPlan } from "./build_plan_compile";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";

const duckdb = new DuckDBConnection("duckdb", ":memory:");
const connections = new FixedConnectionMap(
   new Map([["duckdb", duckdb]]),
   "duckdb",
);
afterAll(() => duckdb.close());

function reader(overlay: ReadonlyMap<string, string>): URLReader {
   return {
      readURL: async (url: URL) => {
         const hit = overlay.get(url.href);
         if (hit !== undefined) return hit;
         return fs.readFileSync(url, "utf8");
      },
   };
}

async function plan(
   files: Record<string, string>,
   modelPath: string,
   // A label only since the switch retired: an unflagged file is planned
   // from its build copy, a flagged one from itself.
   _mode: "off" | "on",
): Promise<BuildPlanParts> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "v1-build-companion-"));
   for (const [f, text] of Object.entries(files)) {
      fs.mkdirSync(path.dirname(path.join(dir, f)), { recursive: true });
      fs.writeFileSync(path.join(dir, f), text);
   }
   const { restore } = installSettings({ env: {} });
   try {
      const base = pathToFileURL(dir + "/");
      const runtime = new Runtime({
         urlReader: reader(new Map()),
         connections,
      });
      const materializer = runtime.loadModel(new URL(modelPath, base), {
         importBaseURL: base,
      });
      const parts = emptyBuildPlanParts();
      await collectModelBuildPlan(parts, {
         modelPath,
         packagePath: dir,
         materializer,
         malloyModel: await materializer.getModel(),
         runtime,
         getRuntime: async (overlay) => ({
            runtime: new Runtime({ urlReader: reader(overlay), connections }),
            importBaseURL: base,
         }),
      });
      return parts;
   } finally {
      restore();
   }
}

async function addresses(
   parts: BuildPlanParts,
   dir?: string,
): Promise<Record<string, string>> {
   const digests = { duckdb: await duckdb.getDigest() };
   const out: Record<string, string> = {};
   for (const [id, s] of Object.entries(parts.sources)) {
      // sourceIDs carry the temp dir; key by name@file.
      const key = dir ? id.replace(dir, "") : id.replace(/^.*\//, "");
      out[key] = computeSourceEntityId(s, digests);
   }
   return out;
}

const FLAG = "##! experimental.persistence\n";
const PERSIST = `
source: base is duckdb.sql("select 1 as a, 'x' as b")
#@ persist
source: p is base extend { measure: n is count() } -> { group_by: a, b; aggregate: n }
`;

describe("the v1 build copy on adversarial files", () => {
   for (const [label, authorText] of [
      ["plain", PERSIST],
      ["leading comment and blank lines", `// header\n\n-- more\n${PERSIST}`],
      ["a model ## annotation first", `## doc="a model"\n${PERSIST}`],
      ["another ##! flag first", `##! experimental { join_types }\n${PERSIST}`],
      ["a commented-out #@ persist too", `${PERSIST}\n// #@ persist\n`],
      // Malloy reads `persist` anywhere on the line; the copy must too, or
      // the source is planned nowhere and reported nowhere.
      [
         "persist after another key on its #@ line",
         PERSIST.replace("#@ persist", "#@ name=p_t persist"),
      ],
   ] as const) {
      it(`on: ${label} plans what the author-flagged file plans off`, async () => {
         const on = await plan({ "m.malloy": authorText }, "m.malloy", "on");
         const off = await plan(
            { "m.malloy": FLAG + authorText },
            "m.malloy",
            "off",
         );
         const a = await addresses(on);
         expect(Object.keys(a).length).toBeGreaterThan(0);
         expect(a).toEqual(await addresses(off));
         expect(on.droppedPersistSources).toEqual(off.droppedPersistSources);
         expect(on.annotationFindings).toEqual(off.annotationFindings);
         expect(Object.values(on.sourceGateOutcomes)).toEqual(
            Object.values(off.sourceGateOutcomes),
         );
         expect(on.builds.length).toBe(off.builds.length);
      });
   }

   it("on: an unflagged file whose base is imported (unflagged, relative subdir)", async () => {
      const files = {
         "lib/base.malloy": `source: base is duckdb.sql("select 1 as a, 'x' as b")\n`,
      };
      const m = `import "lib/base.malloy"\n#@ persist\nsource: p is base -> { group_by: a, b }\n`;
      const on = await plan({ ...files, "m.malloy": m }, "m.malloy", "on");
      const off = await plan(
         { ...files, "m.malloy": FLAG + m },
         "m.malloy",
         "off",
      );
      const a = await addresses(on);
      expect(Object.keys(a).length).toBe(1);
      expect(a).toEqual(await addresses(off));
   });

   it("on: a file importing an unflagged #@ persist file, itself declaring none, plans nothing of its own", async () => {
      const files = { "lib/p.malloy": PERSIST };
      const m = `import "lib/p.malloy"\nsource: q is p extend {}\n`;
      const on = await plan({ ...files, "m.malloy": m }, "m.malloy", "on");
      expect(Object.keys(on.sources)).toEqual([]);
      // the declaring file plans it
      const lib = await plan(files, "lib/p.malloy", "on");
      expect(Object.keys(lib.sources).length).toBe(1);
   });

   it("on: a broken #@ line's finding names the author's line, not the copy's", async () => {
      const text = `source: base is duckdb.sql("select 1 as a")\n#@ persist name="x\nsource: p is base -> { group_by: a }\n`;
      const on = await plan({ "m.malloy": text }, "m.malloy", "on");
      const off = await plan({ "m.malloy": FLAG + text }, "m.malloy", "off");
      const lineOf = (p: BuildPlanParts) =>
         JSON.stringify(p.annotationFindings ?? p).match(/line (\d+)/)?.[1];
      // Author file: the #@ line is line 2. Flagged off-file: line 3.
      expect(lineOf(off)).toBe("3");
      expect(lineOf(on)).toBe("2");
   });
});

describe("the v1 build copy of a file planned from its build view (#6982)", () => {
   const packages = tempBuildPlanPackages();
   afterEach(() => packages.cleanup());

   // A join across connections: the file plans from its build view, with
   // that join left out. Its build copy must be made from the same text.
   const CROSS = `source: accounts is crm.sql("select 1 as id, 'a' as account_name") extend { primary_key: id }
source: usage is product.sql("select 1 as account_id, 5 as events")
source: account_usage is accounts extend {
  join_many: u is usage on u.account_id = id
  measure: total_events is u.events.sum()
}
#@ persist
source: by_name is accounts -> { group_by: account_name }
`;

   async function planCross(text: string) {
      const { restore } = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      });
      const crm = new DuckDBConnection("crm", ":memory:");
      const product = new DuckDBConnection("product", ":memory:");
      try {
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
         const pkg = await packages.create(text, {
            getMalloyConfig: () => malloyConfig,
            getMalloyConnection: async (name: string) =>
               (name === "crm" ? crm : product) as never,
         });
         const { plan } = await computePackageBuildPlan(pkg);
         return {
            names: Object.values(plan?.sources ?? {}).map((s) => s.name),
            omitted: plan?.omittedSources ?? [],
         };
      } finally {
         restore();
         await crm.close();
         await product.close();
      }
   }

   it("an unflagged file plans what the author-flagged file plans", async () => {
      const flagged = await planCross(FLAG + CROSS);
      expect(flagged).toEqual({ names: ["by_name"], omitted: [] });
      expect(await planCross(CROSS)).toEqual(flagged);
   });
});
