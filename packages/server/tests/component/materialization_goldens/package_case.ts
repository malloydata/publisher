// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One package-level golden case: a package loaded through the real
 * production path (the conformance fixture's harness: Environment,
 * `Package.create` in the load worker, the package's own build plan and
 * bindings), its plan compiled again on this thread to read the internals,
 * every planned table built and bound, and a fixed query set run live, then
 * served with the materialize switch off and on.
 *
 * It writes up to four goldens under `<area>/<case>`:
 *
 *  - `.plan.json`: the wire build plan (what the plan endpoint returns, its
 *    SQL as fingerprints), the internal `MaterializePlan`s and rollup plans,
 *    the gate outcomes and findings, and the load warnings;
 *  - `.build.sql`: each planned source's address, compiler key, address and
 *    build SQL, in build order, then every generated build file (companions,
 *    build copies, level files, rollup and view companions);
 *  - `.serve.json`: per query and switch, what answered (path report, skip
 *    reasons), whether it equals live, and the access record's governance
 *    fields; plus the bind's verdicts and warnings;
 *  - `.served.txt`: the files generated while serving (the served copy,
 *    the rollup and view variants), when there are any.
 */

import type { GivenValue } from "@malloydata/malloy";
import { unrecordedAudit } from "../../../src/service/access_record";
import { compilePackageBuildPlan } from "../../../src/service/materialization/plan/build_plan_compile";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import { servedCopyAlias } from "../../../src/service/materialization/serve/served_copy_config";
import { rowDifference, type Row } from "../r4/harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   loadFixture,
   type Outcome,
   type Switch,
} from "../r4/conformance/package_harness";
import {
   addressesOf,
   GeneratedFiles,
   internalPlans,
   learnAddresses,
   recordingOverlays,
   renderBuild,
   sqlFingerprint,
} from "./capture";
import { Normalizer } from "./normalize";

/** One query a case runs, live and served. */
export interface GoldenQuery {
   name: string;
   malloy: string;
   /** The model file it runs in (default the case's first file). */
   model?: string;
   givens?: Record<string, unknown>;
}

export interface GoldenCase {
   /** The golden path stem, `<area>/<case>`. */
   stem: string;
   title: string;
   /** The package's files (models, data, `publisher.json`). */
   files: Record<string, string>;
   /** The model queries run in by default. */
   entry: string;
   queries: GoldenQuery[];
   /**
    * Settings beyond the harness's (`PERSIST_STORAGE_MODE=on` and the
    * materialize switch), such as `PUBLISHER_MATERIALIZE_GATED_SOURCES`.
    * A case with any loads in a load pool of its own.
    */
   env?: Record<string, string>;
}

/** The goldens a case produced, by file suffix; undefined when it has none. */
export type CaseGoldens = Record<
   "plan.json" | "build.sql" | "serve.json" | "served.txt",
   string | undefined
>;

const SWITCHES: Switch[] = ["off", "on"];

/** Install the case's settings with the materialize switch as given; returns the restore. */
function settingsFor(c: GoldenCase, materialize: Switch): () => void {
   return installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: materialize === "on" ? "true" : "false",
         ...c.env,
      },
   }).restore;
}

/** The first line of an error, normalised. */
function firstLine(normalizer: Normalizer, error: string): string {
   return normalizer.text(error.split("\n")[0]).slice(0, 400);
}

/** Load, plan, build, bind and query one case, and render its goldens. */
export async function runCase(c: GoldenCase): Promise<CaseGoldens> {
   const normalizer = new Normalizer();
   normalizer.addAlias(servedCopyAlias(LAKE), LAKE);
   let restore = settingsFor(c, "on");
   // A load worker reads its settings at spawn: a case with settings of its
   // own plans in a pool made under them, and the next case's first load
   // picks a pool made under its own.
   if (c.env) await freshLoadPool();
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   try {
      try {
         loader = await loadFixture(c.files);
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         return {
            "plan.json": JSON.stringify(
               { loadError: message.split("\n")[0] },
               null,
               2,
            ),
            "build.sql": undefined,
            "serve.json": undefined,
            "served.txt": undefined,
         };
      }
      normalizer.addRoot(loader.root);
      const pkg = loader.pkg;
      normalizer.addRoot(pkg.getPackagePath(), "<pkg>");
      const modelOf = (q: GoldenQuery) => q.model ?? c.entry;

      // Live first, before anything is bound.
      const live = new Map<string, Outcome>();
      for (const q of c.queries) {
         live.set(
            q.name,
            await loader.run(modelOf(q), q.malloy, q.givens as never),
         );
      }

      const loadWarnings = pkg.getPackageMetadata().warnings ?? [];
      const wirePlan = pkg.getBuildPlan();

      // The plan again, on this thread, for the internals and the generated text.
      const buildFiles = new GeneratedFiles();
      let compiled:
         | Awaited<ReturnType<typeof compilePackageBuildPlan>>
         | undefined;
      let planError: string | undefined;
      try {
         compiled = await recordingOverlays(buildFiles, () =>
            compilePackageBuildPlan(pkg),
         );
      } catch (error) {
         planError = error instanceof Error ? error.message : String(error);
      }
      const addresses = compiled ? addressesOf(compiled) : [];
      learnAddresses(normalizer, addresses);

      const plan = {
         wirePlan: wirePlan && {
            ...wirePlan,
            sources: Object.fromEntries(
               Object.entries(wirePlan.sources).map(([id, s]) => [
                  id,
                  { ...s, sql: sqlFingerprint(normalizer, s.sql) },
               ]),
            ),
         },
         buildPlanUnavailable: pkg.isBuildPlanUnavailable() || undefined,
         planError: planError && firstLine(normalizer, planError),
         internal: compiled ? internalPlans(compiled) : undefined,
         loadWarnings,
      };

      // Build and bind, recording what the served copy generates.
      const servedFiles = new GeneratedFiles();
      const fixture = await recordingOverlays(servedFiles, () =>
         loader!.bind({ includeRollups: true }),
      );
      const runs: unknown[] = [];
      for (const on of SWITCHES) {
         restore();
         restore = () => {};
         const restoreSwitch = settingsFor(c, on);
         try {
            for (const q of c.queries) {
               runs.push({
                  switch: on,
                  ...(await recordingOverlays(servedFiles, () =>
                     serveOne(pkg, modelOf(q), q, live.get(q.name)!),
                  )),
               });
            }
         } finally {
            restoreSwitch();
         }
      }
      const serve = {
         bind: {
            built: fixture.built,
            buildErrors: Object.fromEntries(
               Object.entries(fixture.buildErrors).map(([k, v]) => [
                  k,
                  firstLine(normalizer, v),
               ]),
            ),
            verdicts: fixture.verdicts,
            warnings: fixture.warnings,
         },
         queries: runs,
      };
      return {
         "plan.json": normalizer.json(plan),
         "build.sql": renderBuild(
            `${c.title}: build plan`,
            normalizer,
            addresses,
            buildFiles,
         ),
         "serve.json": normalizer.json(serve),
         "served.txt": servedFiles.render(
            normalizer,
            `${c.title}: files generated while serving`,
         ),
      };
   } finally {
      restore();
      await loader?.dispose();
      if (c.env) await freshLoadPool();
   }
}

/** One served query: what answered, against live, and the access record's governance fields. */
async function serveOne(
   pkg: Awaited<ReturnType<typeof loadFixture>>["pkg"],
   modelPath: string,
   q: GoldenQuery,
   live: Outcome,
): Promise<Record<string, unknown>> {
   const head = {
      query: q.name,
      malloy: q.malloy,
      ...(q.givens ? { givens: Object.keys(q.givens).sort() } : {}),
   };
   const model = pkg.getModel(modelPath);
   if (!model) return { ...head, error: `${modelPath} did not load` };
   const audit = unrecordedAudit();
   try {
      const result = await model.runQuery(
         audit,
         {
            query: q.malloy,
            ...(q.givens
               ? { givens: q.givens as Record<string, GivenValue> }
               : {}),
         } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      const rows = result.compactResult as unknown as Row[];
      const summary = audit.summary;
      return {
         ...head,
         servedBy: result.path?.servedBy?.kind ?? "live",
         servedFrom: result.servedFrom ?? null,
         ...("error" in live
            ? { liveError: live.error.split("\n")[0].slice(0, 400) }
            : {
                 equalToLive:
                    rowDifference(live.answer.rows, rows) === undefined,
              }),
         path: result.path,
         access: {
            source: summary?.source,
            decision: summary?.decision,
            gates: summary?.gates,
            givenNames: summary?.givenNames,
            gateGivenNames: summary?.gateGivenNames,
            fieldsRead: summary?.fieldsRead,
            fieldsReadComplete: summary?.fieldsReadComplete,
            shape: summary?.shape,
            // The record's `path` is the path report's compact copy, kept above.
            pathUnavailable: audit.pathUnavailable,
         },
      };
   } catch (error) {
      return {
         ...head,
         error: (error instanceof Error ? error.message : String(error))
            .split("\n")[0]
            .slice(0, 400),
         access: { decision: audit.summary?.decision },
      };
   }
}
