// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Materialization characterization goldens over the repo's persist corpus
 * (phase 5, step 1): every hammer scenario model that materializes, through
 * v1 `#@ persist` / `#@ preaggregate` or v2 `#(materialize)` /
 * `#(preaggregate)` (`test_helpers/persist_corpus.ts`), compiled against its
 * DuckDB stand-in and planned through `collectModelBuildPlan` with the
 * materialize switch on, as a package load plans each model. The persist fixtures are packages,
 * and run through the package goldens instead.
 *
 * Per model, under `corpus/hammer/<scenario>/`:
 *
 *  - `.plan.json`: the wire build plan this model alone derives (SQL as
 *    fingerprints), the internal `MaterializePlan`s and rollup plans, gate
 *    outcomes and findings; or the compile error, for a model the stand-in
 *    cannot compile;
 *  - `.build.sql`: each planned source's address, compiler key, address and
 *    build SQL, in build order, then every generated build file.
 *
 * No query runs: the hammer suite, on Postgres, owns the serving side.
 *
 * To regenerate after an intended change (from `packages/server`):
 *
 *    UPDATE_GOLDENS=1 bun test --timeout 200000 tests/component/materialization_goldens
 */

import {
   FixedConnectionMap,
   Runtime,
   type URLReader,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   collectModelBuildPlan,
   deriveBuildPlanOutcome,
   emptyBuildPlanParts,
   type CompiledBuildPlan,
} from "../../../src/service/materialization/plan/build_plan";
import { overlayKey } from "../../../src/service/overlay_key";
import {
   closeCorpus,
   type CorpusModel,
   hammerModels,
   materializes,
   releasingEach,
} from "../../../src/test_helpers/persist_corpus";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   addressesOf,
   GeneratedFiles,
   internalPlans,
   learnAddresses,
   renderBuild,
   sqlFingerprint,
} from "./capture";
import {
   expectGolden,
   removeGoldens,
   unclaimedGoldens,
   UPDATING,
} from "./golden_files";
import { Normalizer } from "./normalize";

/** The prefix the corpus gives a hammer model's in-memory URL. */
const HAMMER_URL = "file:///hammer/";

interface Planned {
   plan: string;
   build: string | undefined;
}

/** A reader over `files`, with `overlay` (keyed by `overlayKey`) in front. */
function readerOf(
   files: ReadonlyMap<string, string>,
   overlay: ReadonlyMap<string, string> = new Map(),
): URLReader {
   return {
      readURL: async (url: URL) => {
         const text = overlay.get(overlayKey(url)) ?? files.get(url.href);
         if (text === undefined) throw new Error(`not found: ${url.href}`);
         return text;
      },
   };
}

/**
 * Plan one corpus model as a package load plans a model, under `root`: its
 * files are rehomed from the corpus's `file:///hammer/` URLs to real paths,
 * so the companion URLs the planner derives from a package path agree on
 * every platform.
 */
async function planModel(model: CorpusModel, root: string): Promise<Planned> {
   const normalizer = new Normalizer().addRoot(root, "<hammer>");
   const rehome = (url: string) =>
      pathToFileURL(path.join(root, ...url.slice(HAMMER_URL.length).split("/")))
         .href;
   const files = new Map(
      [...model.files].map(([url, text]) => [rehome(url), text] as const),
   );
   const relative = model.entry.slice(HAMMER_URL.length).split("/");
   const packagePath = path.join(root, relative[0]);
   const modelPath = relative.slice(1).join("/");
   const entry = new URL(rehome(model.entry));
   const connections = new FixedConnectionMap(
      new Map([...model.connections]),
      [...model.connections.keys()][0],
   );
   const generated = new GeneratedFiles();
   const runtimeOver = (overlay?: ReadonlyMap<string, string>) =>
      new Runtime({ urlReader: readerOf(files, overlay), connections });
   let compiled: CompiledBuildPlan;
   try {
      const runtime = runtimeOver();
      const materializer = runtime.loadModel(entry);
      const malloyModel = await materializer.getModel();
      const parts = emptyBuildPlanParts();
      await collectModelBuildPlan(parts, {
         modelPath,
         packagePath,
         materializer,
         malloyModel,
         runtime,
         getRuntime: async (overlay) => {
            generated.addAll(overlay);
            return {
               runtime: runtimeOver(overlay),
               importBaseURL: new URL(".", entry),
            };
         },
      });
      const connectionDigests: Record<string, string> = {};
      for (const [name, c] of model.connections) {
         connectionDigests[name] = await c.getDigest();
      }
      compiled = {
         ...parts,
         connectionDigests,
         connections: new Map([...model.connections]),
      };
   } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
         plan: JSON.stringify(
            { compileError: normalizer.text(message.split("\n")[0]) },
            null,
            2,
         ),
         build: undefined,
      };
   }
   const addresses = addressesOf(compiled);
   learnAddresses(normalizer, addresses);
   const { connections: _connections, ...collected } = compiled;
   const outcome = deriveBuildPlanOutcome(collected, null);
   const wirePlan = outcome.plan && {
      ...outcome.plan,
      sources: Object.fromEntries(
         Object.entries(outcome.plan.sources).map(([id, s]) => [
            id,
            { ...s, sql: sqlFingerprint(normalizer, s.sql) },
         ]),
      ),
   };
   return {
      plan: normalizer.json({
         wirePlan,
         droppedPersistSources: outcome.droppedPersistSources,
         rollupsOverStoredBase: outcome.rollupsOverStoredBase,
         rollupsKeepingGates: outcome.rollupsKeepingGates,
         internal: internalPlans(compiled),
      }),
      build: renderBuild(
         `${model.label}: build plan`,
         normalizer,
         addresses,
         generated,
      ),
   };
}

/** The golden path stem for a corpus model. */
function stemOf(model: CorpusModel): string {
   // `hammer/<scenario>/<package>/<file>.malloy`
   const [, scenario, ...rest] = model.label.split("/");
   return `corpus/hammer/${scenario}/${rest.join("__").replace(/\.malloy$/, "")}`;
}

const claimed = new Set<string>();
const planned = new Map<string, Planned>();
let root: string;
let restore: () => void = () => {};

beforeAll(async () => {
   restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
   }).restore;
   root = fs.mkdtempSync(path.join(os.tmpdir(), "mat-goldens-corpus-"));
   const { models } = await hammerModels(materializes);
   for await (const model of releasingEach(models)) {
      const stem = stemOf(model);
      if (planned.has(stem)) throw new Error(`two corpus models at ${stem}`);
      planned.set(stem, await planModel(model, root));
   }
}, 180000);

afterAll(async () => {
   restore();
   await closeCorpus();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("materialization goldens: the hammer persist corpus", () => {
   it("plans every corpus model and matches its goldens", () => {
      // The corpus must be real: a loader regression that found nothing
      // would pass vacuously.
      expect(planned.size).toBeGreaterThan(100);
      const failures: string[] = [];
      for (const [stem, { plan, build }] of planned) {
         for (const [suffix, text] of [
            ["plan.json", plan],
            ["build.sql", build],
         ] as const) {
            const file = `${stem}.${suffix}`;
            claimed.add(file);
            try {
               expectGolden(file, text);
            } catch (error) {
               failures.push(
                  error instanceof Error ? error.message : String(error),
               );
            }
         }
      }
      if (failures.length > 0) {
         throw new Error(
            `${failures.length} corpus golden(s) differ:\n\n${failures.join("\n\n")}`,
         );
      }
   });

   it("leaves no golden that no corpus model produces", () => {
      const stale = unclaimedGoldens("corpus", claimed);
      if (UPDATING) {
         removeGoldens(stale);
         return;
      }
      expect(stale).toEqual([]);
   });
});
