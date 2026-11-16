// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Freshness declarations: a rollup's `freshness.window=` and
 * `freshness.fallback=` on `#(preaggregate)` and on `#@ preaggregate`, read
 * as `#@ persist` reads them, over what its base declares, then the model file
 * and the package; and one grammar on every mechanism, where a malformed
 * group fails the load with the route's own codes.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import type { Package } from "./package";
import { installSettings } from "../test_helpers/settings_env";
import { writePackageFiles } from "../test_helpers/temp_package";

let rootDir: string;
let envPath: string;
let restore: () => void;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "rollup-freshness-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});

afterEach(async () => {
   restore();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function load(
   model: string,
   materialization?: Record<string, unknown>,
): Promise<Package> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": model },
      {
         manifest: {
            name: "pkg",
            description: "fixture",
            ...(materialization ? { materialization } : {}),
         } as never,
      },
   );
   const env = await Environment.create("e", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

/** Each planned rollup's resolved freshness, by its base's name. */
function rollupFreshness(pkg: Package): Record<string, unknown> {
   const out: Record<string, unknown> = {};
   for (const s of Object.values(pkg.getBuildPlan()?.sources ?? {})) {
      if (s.origin === "preaggregate" && s.preaggregate) {
         out[s.preaggregate.baseSourceName] = s.freshness;
      }
   }
   return out;
}

const HEAD = `##! experimental { persistence composite_sources }`;
const ROWS = `duckdb.sql("SELECT 1 AS amount, 'A' AS category")`;

describe("a rollup's resolved freshness", () => {
   it("reads its own declaration on either spelling, field by field over its base's", async () => {
      const pkg = await load(`${HEAD}
#@ persist freshness.window="1h" freshness.fallback="stale_ok"
source: persisted is ${ROWS} -> { select: * } extend {
  #(preaggregate) grain=["category"] freshness.window="15m"
  measure: total is amount.sum()
}
source: plain is ${ROWS} extend {
  #@ preaggregate grain="category" freshness.fallback="live"
  measure: total is amount.sum()
}
source: inherits is ${ROWS} -> { select: * } extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`);
      expect(rollupFreshness(pkg)).toEqual({
         persisted: { window: "15m", fallback: "stale_ok" },
         plain: { fallback: "live" },
         inherits: null,
      });
   });

   it("falls back to the model file and the package under the rollup and its base", async () => {
      const pkg = await load(
         `${HEAD}
## freshness.window="48h"
source: plain is ${ROWS} extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
         { freshness: { fallback: "fail" } },
      );
      expect(rollupFreshness(pkg)).toEqual({
         plain: { window: "48h", fallback: "fail" },
      });
   });

   it("merges the measures of one rollup, where windows compare as durations", async () => {
      const pkg = await load(`${HEAD}
source: plain is ${ROWS} extend {
  #(preaggregate) grain=["category"] freshness.window="24h"
  measure: total is amount.sum()
  #(preaggregate) grain=["category"] freshness.window="1d" freshness.fallback="live"
  measure: n is count()
}
`);
      // The same duration, in either measure's spelling.
      expect(rollupFreshness(pkg)).toEqual({
         plain: {
            window: expect.stringMatching(/^(24h|1d)$/),
            fallback: "live",
         },
      });
   });
});

describe("a malformed rollup freshness fails the load", () => {
   const cases: [string, string, string][] = [
      [
         "an unknown key in the group on #(preaggregate)",
         `#(preaggregate) grain=["category"] freshness.windw="1h"`,
         "preaggregate-unknown-key",
      ],
      [
         "an unknown key in the group on #@ preaggregate",
         `#@ preaggregate grain="category" freshness.fallbak="live"`,
         "preaggregate-unknown-key",
      ],
      [
         "a window that is not a duration on #(preaggregate)",
         `#(preaggregate) grain=["category"] freshness.window="a day"`,
         "preaggregate-invalid",
      ],
      [
         "a fallback outside live, stale_ok, fail on #@ preaggregate",
         `#@ preaggregate grain="category" freshness.fallback="serve_stale"`,
         "preaggregate-invalid",
      ],
      [
         "a group written as a value",
         `#(preaggregate) grain=["category"] freshness="1h"`,
         "preaggregate-invalid",
      ],
   ];
   for (const [what, line, code] of cases) {
      it(what, async () => {
         await expect(
            load(`${HEAD}
source: plain is ${ROWS} extend {
  ${line}
  measure: total is amount.sum()
}
`),
         ).rejects.toThrow(`[${code}]`);
      });
   }

   it("two measures of one rollup with different windows in the two spellings", async () => {
      await expect(
         load(`${HEAD}
source: plain is ${ROWS} extend {
  #(preaggregate) grain=["category"] freshness.window="1h"
  measure: total is amount.sum()
  #@ preaggregate grain="category" freshness.window="2h"
  measure: n is count()
}
`),
      ).rejects.toThrow("[preaggregate-invalid]");
   });

   for (const spelling of [
      '#(preaggregate) grain=["category"]',
      '#@ preaggregate grain="category"',
   ]) {
      it(`two measures of one rollup with different windows (${spelling.split(" ")[0]} ${spelling.split(" ")[1]})`, async () => {
         await expect(
            load(`${HEAD}
source: plain is ${ROWS} extend {
  ${spelling} freshness.window="1h"
  measure: total is amount.sum()
  ${spelling} freshness.window="2h"
  measure: n is count()
}
`),
         ).rejects.toThrow("[preaggregate-invalid]");
      });
   }
});

describe("a malformed freshness value fails the load on every mechanism", () => {
   const persisted = (line: string) => `${HEAD}
${line}
source: daily is ${ROWS} -> { group_by: category; aggregate: total is amount.sum() }
`;
   const cases: [string, string, string][] = [
      [
         "a window that is not a duration on #@ persist",
         persisted('#@ persist freshness.window="a day"'),
         "persist-invalid",
      ],
      [
         "a fallback outside the three on #@ persist { … }",
         persisted('#@ persist { freshness.fallback="serve_stale" }'),
         "persist-invalid",
      ],
      [
         "a window that is not a duration on #(materialize)",
         `${HEAD}
#(materialize) freshness.window="1.5h"
source: m is ${ROWS} extend { dimension: big is amount > 0 }
`,
         "materialize-invalid",
      ],
      [
         "a fallback outside the three on #(materialize)",
         `${HEAD}
#(materialize) freshness.fallback="serve_stale"
source: m is ${ROWS} extend { dimension: big is amount > 0 }
`,
         "materialize-invalid",
      ],
      [
         "a window that is not a duration on #@ persist on a view",
         `${HEAD}
source: v is ${ROWS} extend {
  #@ persist freshness.window="soon"
  view: by_category is { group_by: category; aggregate: total is amount.sum() }
}
`,
         "persist-invalid",
      ],
      [
         "a fallback outside the three on #@ persist on a named query",
         `${HEAD}
source: q is ${ROWS}
#@ persist freshness.fallback="later"
query: by_category is q -> { group_by: category; aggregate: total is amount.sum() }
`,
         "persist-invalid",
      ],
   ];
   for (const [what, model, code] of cases) {
      it(what, async () => {
         await expect(load(model)).rejects.toThrow(`[${code}]`);
      });
   }

   it("loads a well-formed window and fallback on every mechanism", async () => {
      const pkg = await load(`${HEAD}
#@ persist freshness.window="90s" freshness.fallback="stale_ok"
source: daily is ${ROWS} -> { group_by: category; aggregate: total is amount.sum() }
#(materialize) freshness.window="2w" freshness.fallback="fail"
source: m is ${ROWS} extend { dimension: big is amount > 0 }
`);
      expect(
         Object.values(pkg.getBuildPlan()?.sources ?? {})
            .map((s) => s.freshness)
            .filter(Boolean),
      ).toHaveLength(2);
   });
});
