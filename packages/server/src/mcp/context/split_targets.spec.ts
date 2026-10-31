// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import type { JudgeVerdict } from "./judge";
import {
   capItemPicks,
   itemIndex,
   mergeVerdicts,
   planSplit,
   splitTrace,
} from "./split_targets";

const target: FindTarget = {
   index: 0,
   type: "dimension",
   text: "venue state, city, address",
   kinds: KINDS_BY_FIND_TARGET.dimension,
};

describe("capItemPicks", () => {
   const plan = planSplit([target], 15, () => false);
   const first = itemIndex(0, 0);
   const verdicts: JudgeVerdict[] = [
      {
         cluster: "c",
         picks: [
            { target: 0, keys: ["w1", "w2", "w3", "w4"] },
            { target: first, keys: ["a", "b", "c", "d", "e"] },
            { target: first + 1, keys: ["f", "g"] },
         ],
         caveats: [],
      },
   ];

   it("keeps each item's first picks, records the rest, and never caps the whole phrase", () => {
      const capped = capItemPicks(plan, verdicts, 3);
      expect(capped[0].picks.map((p) => [p.target, p.keys, p.capped])).toEqual([
         [0, ["w1", "w2", "w3", "w4"], undefined],
         [first, ["a", "b", "c"], ["d", "e"]],
         [first + 1, ["f", "g"], undefined],
      ]);
      // The capped keys do not come back under the caller's target.
      expect(mergeVerdicts(plan, capped)[0].picks[0].keys).toEqual([
         "w1",
         "w2",
         "w3",
         "w4",
         "a",
         "b",
         "c",
         "f",
         "g",
      ]);
      // The split trace names them.
      const trace = splitTrace(plan, [], capped, (k) => `id:${k}`);
      expect(trace[0].items.map((i) => [i.text, i.picks, i.capped])).toEqual([
         ["venue state", ["id:a", "id:b", "id:c"], ["id:d", "id:e"]],
         ["venue city", ["id:f", "id:g"], []],
         ["venue address", [], []],
      ]);
   });

   it("keeps every pick at 0", () => {
      expect(capItemPicks(plan, verdicts, 0)).toEqual(verdicts);
   });
});
