// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The address-stability corpus: moving the plan off the deprecated
// `PersistSource.getSQL()` / `makeBuildId()` onto the build target's `sql` and
// `buildSQL()` must not move a single address.
//
// A source's address keys the incremental ledger, reuse, refresh streams and
// anchors (#6145 is what drift looks like): a moved address orphans the built
// table and re-seeds forever, silently.
// `build_targets_address_equality.spec.ts` pins the rule on one hand-written
// model; this runs it over every real model in the repo that declares a
// persisted source:
//
//  - the persist fixtures under `tests/fixtures/persist-*`, compiled as their
//    packages are, against their own data files;
//  - every hammer scenario's models, compiled against DuckDB with each
//    scenario's `## Data` tables created under the connection and schema the
//    model reads them by (hammer runs them on Postgres; the address rule does
//    not depend on the dialect, since both forms render through the same
//    compiler call, so DuckDB stands in for schema lookup only).
//
// For each planned source it compares the new address, compiler key, wire SQL
// and build SQL with the deprecated computation, and fails listing every source
// that moved. Kept permanently: a compiler bump is exactly what could move one.

import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   addressSQL,
   addressText,
   buildSQLOf,
   compilerBuildId,
   computeSourceEntityId,
   plannedSources,
} from "./materialization/plan/build_plan";
import {
   closeCorpus,
   type CorpusModel,
   fixtureModels,
   hammerModels,
} from "../test_helpers/persist_corpus";
import { declaredStorage } from "./materialization/plan/persist_source_fields";
import { installSettings } from "../test_helpers/settings_env";

afterAll(closeCorpus);

/** What moved for one source, or nothing. */
interface Moved {
   model: string;
   source: string;
   what: string[];
}

/** Compare every address and SQL form for each planned source of `model`. */
async function compare(
   model: CorpusModel,
): Promise<{ checked: string[]; moved: Moved[] } | undefined> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(model.files),
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

   const checked: string[] = [];
   const moved: Moved[] = [];
   for (const source of plannedSources(targets)) {
      if (targets.unbuildable.some((u) => u.source === source)) {
         // Unkeyable now. It must have been unaddressable before too: a source
         // the deprecated getSQL() could render, and the target walk cannot
         // key, has lost its address, and its built table is orphaned.
         checked.push(source.name);
         if (legacyRenders(source)) {
            moved.push({
               model: model.label,
               source: source.name,
               what: ["lostAddress"],
            });
         }
         continue;
      }
      const what = movedFields(source, digests);
      checked.push(source.name);
      if (what.length > 0) {
         moved.push({ model: model.label, source: source.name, what });
      }
   }
   return { checked, moved };
}

/** Whether the deprecated `getSQL()` renders `source` at all. */
function legacyRenders(source: PersistSource): boolean {
   try {
      source.getSQL();
      return true;
   } catch {
      return false;
   }
}

/** Which forms differ between the target-based and deprecated computations. */
function movedFields(
   source: PersistSource,
   digests: Record<string, string>,
): string[] {
   const digest = digests[source.connectionName];
   const legacySQL = source.getSQL();
   const what: string[] = [];
   if (addressSQL(source) !== legacySQL) what.push("sql");
   if (
      computeSourceEntityId(source, digests) !==
      source.makeBuildId(digest, addressText(source, legacySQL))
   ) {
      what.push("sourceEntityId");
   }
   if (
      compilerBuildId(source, digests) !== source.makeBuildId(digest, legacySQL)
   ) {
      what.push("compilerBuildId");
   }
   const empty = { entries: {} };
   if (
      buildSQLOf(source, empty) !==
      source.getSQL({ buildManifest: empty, connectionDigests: digests })
   ) {
      what.push("buildSQL");
   }
   return what;
}

describe("build-target addresses over the repo's persist corpus", () => {
   it(
      "no source's address, compiler key, wire SQL or build SQL moves",
      async () => {
         const hammer = await hammerModels();
         const corpus = [...fixtureModels(), ...hammer.models];
         const moved: Moved[] = [];
         const uncompiled: string[] = [...hammer.skipped];
         let checked = 0;
         for (const model of corpus) {
            try {
               const result = await compare(model);
               if (!result) continue;
               checked += result.checked.length;
               moved.push(...result.moved);
            } catch (err) {
               uncompiled.push(
                  `${model.label}: ${(err as Error).message.slice(0, 300).replace(/\n/g, " ")}`,
               );
            }
         }
         // The corpus must be real: a parser or loader regression that compiled
         // nothing would pass vacuously.
         expect(checked).toBeGreaterThan(100);
         if (uncompiled.length > 0) {
            console.log(
               `build-target corpus: ${uncompiled.length} model(s) did not compile ` +
                  `against the DuckDB stand-in and were not compared:\n  ${uncompiled.join("\n  ")}`,
            );
         }
         console.log(
            `build-target corpus: ${checked} sources compared across ${corpus.length - uncompiled.length} models`,
         );
         expect(moved).toEqual([]);
      },
      { timeout: 180000 },
   );

   // P12 behind PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: with the
   // switch on, the sources that move must be exactly the ones that declare
   // `storage=`, and each by the destination alone. Listed, so the rollout
   // note can say what a deployment rebuilds.
   it(
      "with the destination switch on, exactly the storage= sources move",
      async () => {
         const hammer = await hammerModels();
         const corpus = [...fixtureModels(), ...hammer.models];
         const unexpected: string[] = [];
         let storageSources = 0;
         let colocatedSources = 0;
         for (const model of corpus) {
            let sources: PersistSource[];
            let digests: Record<string, string>;
            try {
               const runtime = new Runtime({
                  urlReader: new InMemoryURLReader(model.files),
                  connections: new FixedConnectionMap(
                     new Map([...model.connections]),
                     [...model.connections.keys()][0],
                  ),
               });
               const compiled = await runtime
                  .loadModel(new URL(model.entry))
                  .getModel();
               const targets = await runtime.getBuildTargets(compiled);
               const unbuildable = new Set(
                  targets.unbuildable.map((u) => u.source),
               );
               sources = plannedSources(targets).filter(
                  (s) => !unbuildable.has(s),
               );
               digests = {};
               for (const [name, c] of model.connections)
                  digests[name] = await c.getDigest();
            } catch {
               continue; // reported by the no-move test above
            }
            const addressUnder = (env: Record<string, string>) => {
               const { restore } = installSettings({ env });
               try {
                  return sources.map((s) => computeSourceEntityId(s, digests));
               } finally {
                  restore();
               }
            };
            const off = addressUnder({ PERSIST_STORAGE_MODE: "on" });
            const on = addressUnder({
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
            });
            sources.forEach((source, i) => {
               const stored = declaredStorage(source) !== undefined;
               if (stored) storageSources++;
               else colocatedSources++;
               const moved = off[i] !== on[i];
               if (moved !== stored) {
                  unexpected.push(
                     `${model.label} ${source.name}: ${stored ? "storage= did not move" : "colocated moved"}`,
                  );
               }
            });
         }
         console.log(
            `destination switch: ${storageSources} storage= sources move, ` +
               `${colocatedSources} colocated sources stay`,
         );
         expect(storageSources).toBeGreaterThan(0);
         expect(colocatedSources).toBeGreaterThan(0);
         expect(unexpected).toEqual([]);
      },
      { timeout: 180000 },
   );
});
