// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import { Model } from "./model";
import type { Package } from "./package";

/**
 * `Model.carriesReportTag` is asked on every query (to filter render logs),
 * and it reads the model's report facts. Both are fixed for a Model's life,
 * so both are derived once per Model; these pin that, and that the answers
 * are the ones deriving them afresh gives.
 */
type Derivers = {
   deriveReportModelFacts: () => unknown;
   deriveCarriesReportTag: () => boolean;
};

describe("report facts are derived once per Model", () => {
   let rootDir: string;

   async function loadPackage(): Promise<Package> {
      const envPath = path.join(rootDir, "env");
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      await fs.writeFile(
         path.join(dir, "m.malloy"),
         `source: nums is duckdb.sql("SELECT 1 as one, 2 as two") extend {
  measure: total is two.sum()
  view: by_one is { group_by: one; aggregate: total }
}
`,
      );
      await fs.writeFile(
         path.join(dir, "board.malloy"),
         `## artifact { title="Board" layout=grid tiles=[{ query="nums -> by_one" }] }
import "m.malloy"
`,
      );
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return env.getPackage("pkg", false);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-facts-"));
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it(
      "answers as a fresh derivation does, and derives once",
      async () => {
         const pkg = await loadPackage();
         const proto = Model.prototype as unknown as Derivers;
         for (const [modelPath, carries] of [
            ["board.malloy", true],
            ["m.malloy", false],
         ] as const) {
            const model = pkg.getModel(modelPath)!;
            const fresh = model as unknown as Derivers;
            // The memoized answers equal a derivation from scratch.
            expect(model.carriesReportTag()).toBe(carries);
            expect(fresh.deriveCarriesReportTag()).toBe(carries);
            // `suggestGivens` holds closures, so it compares by what it answers.
            const memo = model.getReportModelFacts()!;
            const again = fresh.deriveReportModelFacts() as typeof memo;
            const { suggestGivens: memoSuggest, ...memoRest } = memo;
            const { suggestGivens: againSuggest, ...againRest } = again;
            expect(memoRest).toEqual(againRest);
            expect(memoSuggest.forSource("nums")).toEqual(
               againSuggest.forSource("nums"),
            );
            expect(memoSuggest.forQuery("nums")).toEqual(
               againSuggest.forQuery("nums"),
            );

            const facts = spyOn(proto, "deriveReportModelFacts");
            const tag = spyOn(proto, "deriveCarriesReportTag");
            try {
               for (let i = 0; i < 5; i++) {
                  expect(model.carriesReportTag()).toBe(carries);
                  model.getReportModelFacts();
               }
               // Already derived above: nothing derives again.
               expect(facts).not.toHaveBeenCalled();
               expect(tag).not.toHaveBeenCalled();
            } finally {
               facts.mockRestore();
               tag.mockRestore();
            }
         }
      },
      { timeout: 60000 },
   );

   it(
      "derives each once on a fresh Model, however often it is asked",
      async () => {
         const proto = Model.prototype as unknown as Derivers;
         const facts = spyOn(proto, "deriveReportModelFacts");
         const tag = spyOn(proto, "deriveCarriesReportTag");
         try {
            const pkg = await loadPackage();
            const model = pkg.getModel("board.malloy")!;
            const factsBefore = facts.mock.calls.length;
            const tagBefore = tag.mock.calls.length;
            for (let i = 0; i < 10; i++) model.carriesReportTag();
            // At most one derivation each since load, whoever asked first.
            expect(facts.mock.calls.length - factsBefore).toBeLessThanOrEqual(
               1,
            );
            expect(tag.mock.calls.length - tagBefore).toBeLessThanOrEqual(1);
            const perModel = new Map<unknown, number>();
            for (const call of facts.mock.contexts) {
               perModel.set(call, (perModel.get(call) ?? 0) + 1);
            }
            for (const count of perModel.values()) expect(count).toBe(1);
         } finally {
            facts.mockRestore();
            tag.mockRestore();
         }
      },
      { timeout: 60000 },
   );
});
