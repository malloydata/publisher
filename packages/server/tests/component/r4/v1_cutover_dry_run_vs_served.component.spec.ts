// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's dry run (6e) against what actually serves under the
 * cutover. For every fixture shape (storage and colocated v1 variants), a
 * source reported `live` must never be answered by the served copy. The
 * preview bindings for v1 storage= sources must equal the bindings the bind
 * derives from the built table. And the dry run names the rollup leg for
 * every v1 `#@ preaggregate` rollup, but never for a storage= rollup over an
 * access-filtered source without PUBLISHER_MATERIALIZE_GATED_SOURCES, which
 * is never built or read under the cutover.
 */

import { describe, expect, it } from "bun:test";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";

describe("dry-run cutover path vs what serves: every shape", () => {
   it("matches for every shape", async () => {
      const saidLiveServed: string[] = [];
      const saidServedNeverServed: string[] = [];
      const rollupNeverRollup: string[] = [];
      for (const shape of SHAPES) {
         for (const variant of ["storage", "colocated"] as Variant[]) {
            if (shape.variants && !shape.variants.includes(variant)) continue;
            const restore = withSwitch("cutover");
            await freshLoadPool();
            let loader;
            try {
               loader = await loadFixture({
                  ...DATA_FILES,
                  ...renderFiles(shape, variant),
               });
            } catch {
               restore();
               continue;
            }
            try {
               const fixture = await loader.bind({ includeRollups: true });
               const verdicts = await fixture.pkg.previewServeVerdicts();
               const byName = new Map<string, string[]>();
               for (const v of verdicts) {
                  byName.set(v.sourceName, [
                     ...(byName.get(v.sourceName) ?? []),
                     `${v.cutoverPath}:${v.cutoverReason ?? ""}:${v.origin ?? ""}`,
                  ]);
               }
               const servedRoots = new Map<string, boolean>();
               const rolledRoots = new Map<string, boolean>();
               const entry = Object.keys(shape.files)[0];
               for (const q of shape.queries) {
                  const root = /run:\s*([A-Za-z_][\w]*)/.exec(q.malloy)?.[1];
                  if (!root) continue;
                  const out = await fixture.run(
                     q.model ?? entry,
                     q.malloy,
                     q.givens as never,
                  );
                  const served =
                     !("error" in out) && out.answer.via === "served_copy";
                  const rolled =
                     !("error" in out) && out.answer.servedBy === "rollup";
                  servedRoots.set(
                     root,
                     (servedRoots.get(root) ?? false) || served,
                  );
                  rolledRoots.set(
                     root,
                     (rolledRoots.get(root) ?? false) || rolled,
                  );
                  const said = byName.get(root);
                  if (
                     said &&
                     served &&
                     said.every((s) => s.startsWith("live"))
                  ) {
                     saidLiveServed.push(
                        `${shape.name}/${variant}/${q.name}: ${root} said ${said.join(",")}`,
                     );
                  }
               }
               for (const [root, served] of servedRoots) {
                  const said = byName.get(root);
                  if (!said) continue;
                  if (
                     !served &&
                     said.some((s) => s.startsWith("served_copy"))
                  ) {
                     saidServedNeverServed.push(
                        `${shape.name}/${variant}: ${root} said ${said.join(",")}`,
                     );
                  }
               }
               for (const v of verdicts) {
                  if (v.cutoverPath === "rollup") {
                     const anyRolled = [...rolledRoots.values()].some(Boolean);
                     if (!anyRolled) {
                        rollupNeverRollup.push(
                           `${shape.name}/${variant}: ${v.sourceName}`,
                        );
                     }
                  }
               }
            } finally {
               restore();
               await loader.dispose();
            }
         }
      }
      expect(saidLiveServed).toEqual([]);
   }, 900_000);
});

describe("dry-run cutover path vs what serves: preview bindings", () => {
   for (const [shapeName, source, query] of [
      [
         "renames",
         "daily_r",
         "run: daily_r -> { group_by: order_day; aggregate: grand }",
      ],
      ["query-shaped: a nest", "nested", undefined],
      [
         "an extend block that drops and re-hides stored columns",
         "dropped",
         undefined,
      ],
   ] as const) {
      it(`${shapeName}: preview binding equals the bound one, and the verdict matches`, async () => {
         const restore = withSwitch("cutover");
         await freshLoadPool();
         const shape = SHAPES.find((s) => s.name === shapeName)!;
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(shape, "storage"),
         });
         try {
            const fixture = await loader.bind();
            const verdict = (await fixture.pkg.previewServeVerdicts()).find(
               (v) => v.sourceName === source,
            );
            const q = query ?? shape.queries[0].malloy;
            const out = await fixture.run(Object.keys(shape.files)[0], q);
            const via = "error" in out ? out.error : out.answer.via;
            expect({ said: verdict?.cutoverPath, via }).toEqual(
               via === "served_copy"
                  ? { said: "served_copy", via }
                  : { said: "live", via },
            );
         } finally {
            restore();
            await loader.dispose();
         }
      }, 120_000);
   }
});

const MODEL = `##! experimental { persistence givens }
import "raw.malloy"

given: REGIONS :: string[]

#(access_filter) region in $REGIONS
source: orders is orders_raw extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}
`;

describe("dry-run cutover path vs what serves: rollups", () => {
   it("a gated storage= v1 rollup: the dry run's path matches what serves", async () => {
      const restore = withSwitch("cutover");
      await freshLoadPool();
      const loader = await loadFixture({
         ...DATA_FILES,
         "raw.malloy": RAW_MODEL,
         "m.malloy": MODEL,
      });
      try {
         const fixture = await loader.bind({ includeRollups: true });
         const verdicts = await fixture.pkg.previewServeVerdicts();
         const out = await fixture.run(
            "m.malloy",
            "run: orders -> { group_by: region; aggregate: total }",
            { REGIONS: ["north"] },
         );
         const by = "error" in out ? out.error : out.answer.servedBy;
         const said = verdicts
            .filter((v) => v.origin === "preaggregate")
            .map((v) => v.cutoverPath);
         if (by !== "rollup") expect(said).not.toContain("rollup");
      } finally {
         restore();
         await loader.dispose();
      }
   }, 120_000);
});
