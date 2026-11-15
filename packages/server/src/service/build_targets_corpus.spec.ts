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
   releasingEach,
} from "../test_helpers/persist_corpus";
import { declaredStorage } from "./materialization/plan/persist_source_fields";
import {
   admitGivenPositions,
   classifyWhereTerms,
   givenReadingFilterTerms,
   givenTerms,
   readTimeDynamicTerms,
} from "./materialization/plan/persist_dynamic_terms";
import { installSettings } from "../test_helpers/settings_env";

afterAll(closeCorpus);

// Loaded once for the file: every load opens a file-backed DuckDB instance per
// scenario connection (about 280) and none closes before `closeCorpus`, so a
// second load doubled the open instances. That ran Windows CI out of memory.
let loaded: Promise<{ models: CorpusModel[]; skipped: string[] }> | undefined;
function corpusOnce(): Promise<{ models: CorpusModel[]; skipped: string[] }> {
   loaded ??= hammerModels().then((hammer) => ({
      models: [...fixtureModels(), ...hammer.models],
      skipped: hammer.skipped,
   }));
   return loaded;
}

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
         const { models: corpus, skipped } = await corpusOnce();
         const moved: Moved[] = [];
         const uncompiled: string[] = [...skipped];
         let checked = 0;
         for await (const model of releasingEach(corpus)) {
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
         const { models: corpus } = await corpusOnce();
         const unexpected: string[] = [];
         let storageSources = 0;
         let colocatedSources = 0;
         for await (const model of releasingEach(corpus)) {
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

// v1 persistence reads its given-reading `where:` terms through the one
// classifier (`givenTerms(classifyWhereTerms(…))`). Before that, a separate
// walk returned every `filterList` entry that reads a given, in order. The
// oracle below is that walk, frozen; every planned source's terms (and each
// joined source's, which the position rule reads the same way) must agree with
// it. Kept until v1 persistence retires.
function legacyReadsAGiven(node: unknown, seen: WeakSet<object>): boolean {
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node)) return false;
   seen.add(node);
   if (Array.isArray(node)) {
      return node.some((item) => legacyReadsAGiven(item, seen));
   }
   const record = node as Record<string, unknown>;
   if (record.node === "given" || record.node === "givenReference") return true;
   if (record.givenRef !== undefined) return true;
   const summary = record.refSummary as { givenUsage?: unknown[] } | undefined;
   if (summary?.givenUsage && summary.givenUsage.length > 0) return true;
   if (Array.isArray(record.givenUsage) && record.givenUsage.length > 0) {
      return true;
   }
   return Object.values(record).some((v) => legacyReadsAGiven(v, seen));
}

function legacyGivenTermCodes(filterList: unknown): string[] {
   if (!Array.isArray(filterList)) return [];
   return filterList
      .filter(
         (entry): entry is { code?: unknown } =>
            entry !== null &&
            typeof entry === "object" &&
            legacyReadsAGiven(entry, new WeakSet()),
      )
      .map((entry) => (typeof entry.code === "string" ? entry.code : ""));
}

/** The source and every source it joins, each with its dotted join path. */
function defsOf(
   def: unknown,
   at = "",
   depth = 0,
): { at: string; def: { filterList?: unknown; fields?: unknown } }[] {
   if (def === null || typeof def !== "object" || depth > 8) return [];
   const record = def as { filterList?: unknown; fields?: unknown };
   const out = [{ at, def: record }];
   for (const field of Array.isArray(record.fields) ? record.fields : []) {
      const f = field as { join?: unknown; as?: unknown; name?: unknown };
      if (f?.join === undefined) continue;
      const name = String(f.as ?? f.name);
      out.push(...defsOf(f, at ? `${at}.${name}` : name, depth + 1));
   }
   return out;
}

describe("v1 given terms over the repo's persist corpus", () => {
   it(
      "the one classifier hands v1 exactly the terms the old walk did",
      async () => {
         const { models: corpus } = await corpusOnce();
         const differ: string[] = [];
         let sources = 0;
         let withTerms = 0;
         for await (const model of releasingEach(corpus)) {
            let planned: PersistSource[];
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
               planned = plannedSources(
                  await runtime.getBuildTargets(compiled),
               );
            } catch {
               continue; // reported by the address test above
            }
            for (const source of planned) {
               sources++;
               const label = `${model.label} ${source.name}`;
               const root = source._sourceDef as unknown as {
                  filterList?: unknown;
               };
               const legacy = legacyGivenTermCodes(root.filterList);
               if (legacy.length > 0) withTerms++;
               const codes = (terms: { code: string }[]) =>
                  terms.map((t) => t.code);
               for (const { at, def } of defsOf(root)) {
                  const now = codes(givenTerms(classifyWhereTerms(def)));
                  const was = legacyGivenTermCodes(def.filterList);
                  if (JSON.stringify(now) !== JSON.stringify(was)) {
                     differ.push(`${label}${at ? ` (join ${at})` : ""}`);
                  }
               }
               const readTime = codes(readTimeDynamicTerms(source));
               const filters = codes(givenReadingFilterTerms(root));
               const admitted = admitGivenPositions(source);
               for (const [what, now] of [
                  ["readTimeDynamicTerms", readTime],
                  ["givenReadingFilterTerms", filters],
                  ...(admitted.ok
                     ? [["strippedTerms", codes(admitted.terms)] as const]
                     : []),
               ] as const) {
                  if (JSON.stringify(now) !== JSON.stringify(legacy)) {
                     differ.push(`${label}: ${what}`);
                  }
               }
            }
         }
         console.log(
            `v1 given terms: ${sources} planned sources, ${withTerms} with given terms`,
         );
         expect(sources).toBeGreaterThan(100);
         expect(withTerms).toBeGreaterThan(0);
         expect(differ).toEqual([]);
      },
      { timeout: 180000 },
   );
});
