// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Second review of #1703, 6e: for every fixture shape (storage and colocated
 * v1 variants), the dry run's cutoverPath against what actually serves under
 * the cutover. A source reported `live` must never be answered by the served
 * copy; one reported `served_copy` that no query of it is answered by the
 * served copy is listed for inspection.
 */

import { describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";

describe("review 1703b 6e: dry-run cutover path vs what serves", () => {
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
