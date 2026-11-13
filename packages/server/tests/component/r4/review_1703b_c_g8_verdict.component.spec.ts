// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Second review of #1703, G8 through the package path (PUBLISHER_MATERIALIZE
 * on, the v1 switch off): each #(materialize) source over a same-file base
 * with a measure, view or join; its serve verdict and warnings against what
 * actually answers, and every answer against live.
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

const MODEL = `##! experimental { givens access_modifiers }
import "raw.malloy"

source: b_ok is orders_raw extend {
  measure: n is count()
  view: by_region is { group_by: region; aggregate: n }
}
#(materialize) storage="lake"
source: m_ok is b_ok extend { measure: total is amount.sum() }

source: b_multi is orders_raw extend {
  measure: t is amount.sum()
} include { public: *; internal: status } extend {
  view: v is { group_by: region; aggregate: t }
}
#(materialize) storage="lake"
source: m_multi is b_multi extend { dimension: zz is upper(region) }

source: b_exc is orders_raw extend {
  except: discount
  measure: n is count()
}
#(materialize) storage="lake"
source: m_exc is b_exc extend { measure: total is amount.sum() }
`;

const CASES: [string, string][] = [
   ["m_ok", "run: m_ok -> by_region"],
   ["m_multi", "run: m_multi -> v"],
   ["m_multi", "run: m_multi -> { group_by: zz; aggregate: t }"],
   ["m_exc", "run: m_exc -> { group_by: region; aggregate: total, n }"],
];

describe("review 1703b G8: verdicts against what serves (package path)", () => {
   let restore: () => void = () => {};
   let fixture: LoadedFixture;
   let dispose: () => Promise<void>;
   const live = new Map<string, Outcome>();
   beforeAll(async () => {
      restore = withSwitch("on");
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": MODEL,
      });
      dispose = loader.dispose;
      for (const [, q] of CASES) live.set(q, await loader.run("m.malloy", q));
      fixture = await loader.bind();
   });
   afterAll(async () => {
      restore();
      await dispose?.();
      await freshLoadPool();
   });

   it("each verdict matches what serves", async () => {
      const mismatch: string[] = [];
      for (const [source, q] of CASES) {
         const out = await fixture.run("m.malloy", q);
         if ("error" in out) throw new Error(out.error);
         const l = live.get(q)!;
         if ("error" in l) throw new Error(l.error);
         expect(rowDifference(l.answer.rows, out.answer.rows)).toBeUndefined();
         const verdict = fixture.verdicts.find((v) =>
            v.startsWith(`${source}:`),
         );
         const notServedWarning = fixture.warnings.some(
            (w) =>
               w.code === "served-copy-not-served" &&
               (w.subject ?? "").includes(source),
         );
         const served = out.answer.via === "served_copy";
         if (!served && verdict?.endsWith(":serves") && !notServedWarning) {
            mismatch.push(
               `${q}: verdict ${verdict}, no warning, answered ${out.answer.servedBy}`,
            );
         }
      }
      expect(mismatch).toEqual([]);
   });
});
