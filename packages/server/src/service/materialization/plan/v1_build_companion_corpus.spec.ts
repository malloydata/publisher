// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * D3's gate (materialization v2, phase 3 pull request 6d): the generated build
 * companion for v1 `#@ persist` must not move a single address. For every
 * model of the repo's persist corpus, each planned source's content address
 * (`sourceEntityId`), compiler key and wire SQL from the build copy (the
 * author's file with `##! experimental.persistence` as its first line, served
 * over the file's own URL) must equal those from the author's own file, and
 * its `sourceID` must be the author's. A moved address orphans a built table
 * and re-seeds an incremental source forever (#6145).
 */

import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   closeCorpus,
   type CorpusModel,
   fixtureModels,
   hammerModels,
   releasingEach,
} from "../../../test_helpers/persist_corpus";
import {
   addressSQL,
   compilerBuildId,
   computeSourceEntityId,
   plannedSources,
} from "./build_plan";
import { v1BuildCompanionText } from "./v1_build_companion";

afterAll(closeCorpus);

async function addresses(
   model: CorpusModel,
   files: Map<string, string>,
): Promise<Map<string, string>> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(files),
      connections: new FixedConnectionMap(
         new Map([...model.connections]),
         [...model.connections.keys()][0],
      ),
   });
   const compiled = await runtime.loadModel(new URL(model.entry)).getModel();
   const targets = await runtime.getBuildTargets(compiled);
   const digests: Record<string, string> = {};
   for (const [name, c] of model.connections)
      digests[name] = await c.getDigest();
   const unbuildable = new Set<PersistSource>(
      targets.unbuildable.map((u) => u.source),
   );
   const out = new Map<string, string>();
   for (const s of plannedSources(targets)) {
      out.set(
         s.sourceID,
         unbuildable.has(s)
            ? "unbuildable"
            : JSON.stringify([
                 computeSourceEntityId(s, digests),
                 compilerBuildId(s, digests),
                 addressSQL(s),
              ]),
      );
   }
   return out;
}

describe("the v1 build companion over the repo's persist corpus", () => {
   it(
      "moves no source's sourceID, address, compiler key or wire SQL",
      async () => {
         const hammer = await hammerModels();
         const corpus = [...fixtureModels(), ...hammer.models];
         const moved: string[] = [];
         const uncompiled: string[] = [];
         let checked = 0;
         for await (const model of releasingEach(corpus)) {
            let author: Map<string, string>;
            try {
               author = await addresses(model, model.files);
            } catch {
               uncompiled.push(model.label);
               continue;
            }
            const text = model.files.get(model.entry);
            if (text === undefined) continue;
            const files = new Map(model.files);
            files.set(model.entry, v1BuildCompanionText(text));
            const companion = await addresses(model, files);
            for (const [sourceID, address] of author) {
               checked++;
               if (companion.get(sourceID) !== address) {
                  moved.push(`${model.label} ${sourceID}`);
               }
            }
            for (const sourceID of companion.keys()) {
               if (!author.has(sourceID)) {
                  moved.push(`${model.label} ${sourceID} (only in the copy)`);
               }
            }
         }
         // The corpus must be real, or the check passes vacuously.
         expect(checked).toBeGreaterThan(100);
         console.log(
            `v1 build companion corpus: ${checked} sources compared; ${uncompiled.length} models did not compile on the stand-in`,
         );
         expect(moved).toEqual([]);
      },
      { timeout: 300000 },
   );
});
