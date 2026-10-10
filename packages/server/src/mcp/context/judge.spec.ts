// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { setRetrievalConfig } from "../../retrieval_config";
import { DEFAULT_FCJ_SETTINGS } from "../../service/package_retrieval";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import {
   embedText,
   scriptedChat,
} from "../../test_helpers/get_context_llm_harness";
import type { CatalogNode } from "./catalog";
import { connect } from "./connect";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import {
   judge,
   judgeOptions,
   renderJudgePrompt,
   validateVerdict,
} from "./judge";
import { toUnit } from "./vectors";

let f: FcjIndexFixture;
beforeAll(async () => {
   f = await buildFcjIndexFixture();
});
afterAll(async () => f.close());
afterEach(() => setRetrievalConfig(undefined));

const t = (
   index: number,
   type: FindTarget["type"],
   text: string,
): FindTarget => ({ index, type, text, kinds: KINDS_BY_FIND_TARGET[type] });

function clustersFor(
   targets: FindTarget[],
   allow: (n: CatalogNode) => boolean = () => true,
) {
   const queries = new Map(
      targets.map((x) => [x.index, toUnit(embedText(x.text))]),
   );
   const found = find({
      index: f.index,
      targets,
      queries,
      perTarget: 30,
      keyword: true,
      values: true,
      allow,
   });
   return connect({
      index: f.index,
      found,
      queries,
      settings: DEFAULT_FCJ_SETTINGS,
      allow,
   });
}

describe("judge prompt", () => {
   it("shows each target, its candidates with route and full doc, and the extras with their reason", () => {
      const targets = [t(0, "measure", "net sales")];
      const { clusters } = clustersFor(targets, (n) => n.source === "orders");
      const options = judgeOptions(clusters[0], targets);
      const prompt = renderJudgePrompt(f.index, clusters[0], targets, options);
      expect(prompt.split("\n").slice(0, 5)).toEqual([
         "Home source: orders - One row per order placed in the web shop.",
         "",
         "<targets>",
         'Target 1 (measure): "net sales"',
         "  [1] net_sales (measure, number; source orders; own field): (no doc; name in words: net sales)",
      ]);
      // The rule in sales' doc reaches the judge, which is the only place it goes.
      expect(prompt).toContain(
         "[2] sales (measure, number; source orders; own field): Order amount in dollars. Exclude test orders when summing.",
      );
      expect(prompt).toContain("Also in this group:");
      expect(prompt).toContain(
         "is_fulfilled (dimension, boolean; source orders; own field): The order has shipped. | added because: net_sales is computed from it",
      );
   });

   it("names the join a field is reached through", () => {
      const targets = [
         t(0, "dimension", "region name of the arrival airport"),
         t(1, "measure", "number of flights"),
      ];
      const { clusters } = clustersFor(targets);
      const prompt = renderJudgePrompt(
         f.index,
         clusters[0],
         targets,
         judgeOptions(clusters[0], targets),
      );
      expect(prompt).toContain(
         "region_name (dimension, string; source regions; through flights.arrival.region or flights.origin.region (a left join from flights))",
      );
   });

   it("never sends a stored value below the full egress preset", () => {
      const targets = [
         t(0, "measure", "how many flights"),
         t(1, "dimension", "flights to Denver"),
      ];
      const { clusters } = clustersFor(targets);
      const opts = judgeOptions(clusters[0], targets);
      const restricted = renderJudgePrompt(f.index, clusters[0], targets, opts);
      expect(restricted).not.toContain("Denver International (DIA)");
      expect(restricted).toContain(
         "a stored value matches the target's value (words)",
      );
      setRetrievalConfig({ egress: { preset: "full" } });
      const full = renderJudgePrompt(f.index, clusters[0], targets, opts);
      expect(full).toContain(
         'stored values matching the target: "Denver International (DIA)"',
      );
   });
});

describe("judge reply", () => {
   const targets = [
      t(0, "measure", "number of flights"),
      t(1, "dimension", "start date"),
   ];

   it("maps candidate numbers back to fields and refuses one that was not offered", () => {
      const { clusters } = clustersFor(targets);
      const options = judgeOptions(clusters[0], targets);
      const check = validateVerdict(targets, options);
      expect(
         check({
            picks: [
               { target: 1, candidates: [1], reason: "the flight count" },
               { target: 2, candidates: [2] },
            ],
            caveats: ["x"],
         }),
      ).toEqual({
         picks: [
            {
               target: 0,
               keys: ["measure:flights@travel.malloy:flight_count"],
               reason: "the flight count",
            },
            { target: 1, keys: ["dimension:flights@travel.malloy:start_date"] },
         ],
         caveats: ["x"],
      });
      expect(() => check({ picks: [{ target: 1, candidates: [99] }] })).toThrow(
         "candidate 99 was not offered",
      );
      expect(() => check({ picks: [{ target: 7, candidates: [] }] })).toThrow(
         "target 7 is not a target number (1 to 2)",
      );
   });

   it("makes one call per cluster and repairs a bad reply once", async () => {
      const { clusters } = clustersFor([
         t(0, "measure", "total sales"),
         t(1, "measure", "number of races"),
         t(2, "measure", "number of flights"),
      ]);
      let calls = 0;
      const chat = scriptedChat(() => {
         calls += 1;
         if (calls === 1)
            return JSON.stringify({ picks: [{ target: 1, candidates: [42] }] });
         return JSON.stringify({
            picks: [{ target: 1, candidates: [1] }],
            caveats: [],
         });
      });
      const verdicts = await judge({
         index: f.index,
         clusters: clusters,
         targets: [
            t(0, "measure", "total sales"),
            t(1, "measure", "number of races"),
            t(2, "measure", "number of flights"),
         ],
         chat: chat.model,
         concurrency: 1,
      });
      expect(verdicts.map((v) => v.cluster)).toEqual([
         "flights@travel.malloy",
         "orders@shop.malloy",
         "races@reports.malloy",
      ]);
      // Three clusters, three calls, plus the one repair of the first reply.
      expect(chat.prompts.length).toBe(4);
      expect(verdicts[0].picks).toEqual([
         { target: 0, keys: ["measure:flights@travel.malloy:flight_count"] },
      ]);
   });
});
