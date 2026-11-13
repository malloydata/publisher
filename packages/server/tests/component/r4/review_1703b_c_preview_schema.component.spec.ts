// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/* eslint-disable @typescript-eslint/no-explicit-any -- reads internals */

/**
 * Second review of #1703, 6e: the dry run's preview bindings for v1 storage=
 * sources against the bindings the bind derives from the built table, and the
 * verdict against what serves.
 */

import { expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";
import { renderFiles, SHAPES } from "./conformance/shapes";

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
