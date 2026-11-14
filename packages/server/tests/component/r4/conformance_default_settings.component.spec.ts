// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every conformance shape under the shipped default, which no
 * other spec runs. The conformance spec's `cutover` switch also sets
 * `PUBLISHER_MATERIALIZE`; the default leaves it off, and 6f found one defect
 * only that combination reaches (the cutover read `#(preaggregate)` with it
 * off). Here only `PERSIST_STORAGE_MODE=on` is set: each v1 query is answered
 * live before any table is
 * bound, then after the bind, and the two must agree on rows and on each
 * column's type (number subtype aside) and annotations.
 */

import { afterAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES } from "./conformance/data";
import {
   type Answer,
   freshLoadPool,
   loadFixture,
   type Outcome,
} from "./conformance/package_harness";
import { renderFiles, SHAPES } from "./conformance/shapes";
import { rowDifference } from "./harness";

function metadataDifference(a: Answer, b: Answer): string | undefined {
   if (a.fields.length !== b.fields.length) return "field count";
   const strip = (t: unknown) => {
      if (t && typeof t === "object" && "subtype" in t) {
         const { subtype: _s, ...rest } = t as Record<string, unknown>;
         return rest;
      }
      return t;
   };
   const notes = (n: string[] | undefined) =>
      (n ?? []).filter((x) => !x.startsWith("#(malloy)"));
   for (let i = 0; i < a.fields.length; i++) {
      const [x, y] = [a.fields[i], b.fields[i]];
      if (JSON.stringify(strip(x.type)) !== JSON.stringify(strip(y.type))) {
         return `${x.name}: type ${JSON.stringify(x.type)} vs ${JSON.stringify(y.type)}`;
      }
      if (
         JSON.stringify(notes(x.annotations)) !==
         JSON.stringify(notes(y.annotations))
      ) {
         return `${x.name}: annotations ${JSON.stringify(notes(x.annotations))} vs ${JSON.stringify(notes(y.annotations))}`;
      }
   }
   return undefined;
}

let servedCount = 0;

afterAll(async () => {
   await freshLoadPool();
});

describe("the conformance shapes under the default settings", () => {
   for (const shape of SHAPES) {
      for (const variant of shape.variants ?? ["storage", "colocated"]) {
         // `#(materialize)` is ignored with PUBLISHER_MATERIALIZE off.
         if (variant === "materialize") continue;
         it(`${shape.name} [${variant}]: every answer equals live`, async () => {
            const restore = installSettings({
               env: { PERSIST_STORAGE_MODE: "on" },
            }).restore;
            await freshLoadPool();
            const entry = Object.keys(shape.files)[0];
            const loader = await loadFixture({
               ...DATA_FILES,
               ...renderFiles(shape, variant),
            });
            const problems: string[] = [];
            try {
               const run = (
                  t: { run: typeof loader.run },
                  q: (typeof shape.queries)[number],
               ) => t.run(q.model ?? entry, q.malloy, q.givens as never);
               const live = new Map<string, Outcome>();
               for (const q of shape.queries) {
                  live.set(q.name, await run(loader, q));
               }
               const fixture = await loader.bind({ includeRollups: true });
               for (const q of shape.queries) {
                  const l = live.get(q.name)!;
                  const s = await run(fixture, q);
                  if ("error" in l || "error" in s) {
                     // A refusal stays a refusal.
                     if (!("error" in l && "error" in s)) {
                        problems.push(
                           `${q.name}: live ${"error" in l ? l.error : "answered"}, default ${"error" in s ? s.error : "answered"}`,
                        );
                     }
                     continue;
                  }
                  const why =
                     rowDifference(l.answer.rows, s.answer.rows) ??
                     metadataDifference(l.answer, s.answer);
                  if (why) problems.push(`${q.name}: ${why}`);
                  if (s.answer.servedBy !== "live") servedCount++;
               }
            } finally {
               restore();
               await loader.dispose();
            }
            expect(problems).toEqual([]);
         }, 180_000);
      }
   }

   it("answers from tables, not only live", () => {
      expect(servedCount).toBeGreaterThan(80);
   });
});
