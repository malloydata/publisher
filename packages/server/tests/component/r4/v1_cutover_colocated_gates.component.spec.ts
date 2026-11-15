// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/* eslint-disable @typescript-eslint/no-explicit-any -- reads internals */

/**
 * Colocated v1 `#@ persist` sources behind gates,
 * under the cutover (contracts at read for colocated, G3), two callers per
 * gate kind, each answer against live (the switch off, before any bind).
 * Gates on the persisted source itself, on a same-file base it extends, and
 * a source gate (#(authorize)). The serve verdict (path report) is recorded.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "./conformance/package_harness";
import { rowDifference } from "./harness";

const MODEL = `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string

#(access_filter) region in $REGIONS
#@ persist name="own_gate"
source: own_gate is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#(access_filter) region in $REGIONS
source: gated_base is orders_raw extend {
  measure: n is count()
}
#@ persist name="over_gated_base"
source: over_gated_base is gated_base -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#(authorize) $ROLE = 'admin'
#@ persist name="authz"
source: authz is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

#@ persist name="plain"
source: plain is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}
#(access_filter) region in $REGIONS
source: gated_ext is plain extend {}
`;

const FILES = { ...DATA_FILES, "raw.malloy": RAW_MODEL, "m.malloy": MODEL };

const CASES: [string, string, Record<string, string | string[]>][] = [
   [
      "own gate, caller 1",
      "run: own_gate -> { group_by: region; aggregate: total }",
      { REGIONS: ["north", "east"] },
   ],
   [
      "own gate, caller 2",
      "run: own_gate -> { group_by: region; aggregate: total }",
      { REGIONS: ["west"] },
   ],
   [
      "own gate, no group",
      "run: own_gate -> { group_by: region; aggregate: total }",
      { REGIONS: [] },
   ],
   [
      "over gated base, caller 1",
      "run: over_gated_base -> { group_by: region; aggregate: total }",
      { REGIONS: ["north", "east"] },
   ],
   [
      "over gated base, caller 2",
      "run: over_gated_base -> { group_by: region; aggregate: total }",
      { REGIONS: ["west"] },
   ],
   [
      "authorize, admin",
      "run: authz -> { group_by: region; aggregate: total }",
      { ROLE: "admin" },
   ],
   [
      "authorize, viewer",
      "run: authz -> { group_by: region; aggregate: total }",
      { ROLE: "viewer" },
   ],
   [
      "authorize, default",
      "run: authz -> { group_by: region; aggregate: total }",
      {},
   ],
   [
      "gated extension, caller 1",
      "run: gated_ext -> { group_by: region; aggregate: total }",
      { REGIONS: ["south"] },
   ],
   [
      "gated extension, caller 2",
      "run: gated_ext -> { group_by: region; aggregate: total }",
      { REGIONS: ["north", "west"] },
   ],
   [
      "gated extension, caller join",
      "run: plain extend { join_one: g is gated_ext on g.id = id } -> { group_by: g.region; aggregate: total }",
      { REGIONS: ["south"] },
   ],
   [
      "gated extension, hidden region via nest",
      "run: gated_ext -> { nest: x is { group_by: region; aggregate: total } }",
      { REGIONS: ["south"] },
   ],
];

describe("colocated v1 gates under the cutover, two callers per kind", () => {
   let restore: () => void = () => {};
   let fixture: LoadedFixture;
   let dispose: () => Promise<void>;
   const live = new Map<string, Outcome>();

   beforeAll(async () => {
      await freshLoadPool();
      restore = withSwitch("cutover");
      const loader = await loadFixture(FILES);
      dispose = loader.dispose;
      restore();
      restore = withSwitch("off");
      for (const [name, q, g] of CASES)
         live.set(name, await loader.run("m.malloy", q, g));
      restore();
      restore = withSwitch("cutover");
      fixture = await loader.bind();
   });
   afterAll(async () => {
      restore();
      await dispose?.();
      await freshLoadPool();
   });

   for (const [name, q, g] of CASES) {
      // Interleave: run each case twice, around the others.
      it(`${name}: as live`, async () => {
         for (let i = 0; i < 2; i++) {
            const out = await fixture.run("m.malloy", q, g);
            const l = live.get(name)!;
            if ("error" in l) {
               expect("error" in out).toBe(true);
            } else {
               if ("error" in out) throw new Error(out.error);
               expect(
                  rowDifference(l.answer.rows, out.answer.rows),
               ).toBeUndefined();
            }
         }
      });
   }
});

describe("the colocated source over a gated base, today (switch off, tables bound)", () => {
   it("answers each caller with live's rows", async () => {
      await freshLoadPool();
      const restore = withSwitch("off");
      const loader = await loadFixture(FILES);
      try {
         const q =
            "run: over_gated_base -> { group_by: region; aggregate: total }";
         const live1 = await loader.run("m.malloy", q, { REGIONS: ["west"] });
         const fixture = await loader.bind();
         const out = await fixture.run("m.malloy", q, { REGIONS: ["west"] });
         if ("error" in out || "error" in live1) throw new Error("failed");
         expect(
            rowDifference(live1.answer.rows, out.answer.rows),
         ).toBeUndefined();
      } finally {
         restore();
         await loader.dispose();
         await freshLoadPool();
      }
   }, 120_000);
});
