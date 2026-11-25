// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A load plans each model's entities in the package-load worker and only
 * resolves the plans against the query route on the main thread
 * (`plan.ts`, `resolveModelEntities`). These pin that the two paths agree:
 * for every package the entities golden covers that loads, the entities and
 * findings a load serves from the worker's plans are byte-for-byte what the
 * main thread extracts by itself from the same models and route. (`gates`
 * does not load: its gates read givens with defaults, which a load refuses.
 * The golden extracts it without loading it.) And a plan made without
 * the boundary's descents, which the route then needs, is planned again
 * rather than served.
 */

import { type Connection } from "@malloydata/malloy";
import { describe, expect, test } from "bun:test";
import path from "node:path";
import { Package } from "../package";
import {
   GOLDEN_PACKAGES,
   type GoldenPackage,
} from "./__fixtures__/golden_packages";
import {
   entitiesFindingCode,
   extractPackageEntities,
   type ModelEntitiesPlan,
} from "./extract";
import { planModelFileEntities } from "./plan";
import { ConnectionLookup, writeWorkspace } from "./test_support";
import { toApiEntities } from "./to_api";

/** Load `golden` as a package, through the worker pool, and run `check` on it. */
async function withLoadedPackage(
   golden: GoldenPackage,
   check: (pkg: Package, packageRoot: string) => Promise<void>,
): Promise<void> {
   const manifest = {
      name: "entities-spec",
      ...(golden.manifest?.explores && { explores: golden.manifest.explores }),
      ...(golden.manifest?.queryableSources && {
         queryableSources: golden.manifest.queryableSources,
      }),
   };
   const ws = await writeWorkspace(
      Object.entries({
         ...golden.files,
         "publisher.json": JSON.stringify(manifest),
      }).map(([relpath, contents]) => ({ relpath, contents })),
   );
   const lookup = await ConnectionLookup.getInstance([], ws.dir);
   try {
      const pkg = await Package.create(
         "entities-spec",
         "entities-spec",
         ws.dir,
         new Map([
            ["duckdb", (await lookup.lookupConnection("duckdb")) as Connection],
         ]),
      );
      await check(pkg, ws.dir);
   } finally {
      await lookup.dispose();
      await ws.cleanup();
   }
}

/** The package's warnings the entities extraction reported. */
function entitiesWarnings(pkg: Package) {
   return (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
      w.code?.startsWith("entities-"),
   );
}

describe("entities planned in the load worker", () => {
   for (const [name, golden] of Object.entries(GOLDEN_PACKAGES)) {
      if (name === "gates") continue;
      test(`resolve to what the main thread extracts, for the ${name} package`, async () => {
         await withLoadedPackage(golden, async (pkg, packageRoot) => {
            // Every compiled model was planned by the worker.
            expect(pkg.getEntitiesPlannedOnMainThread()).toEqual([]);

            const modelFiles = await pkg.entitiesModelFiles();
            const here = await extractPackageEntities({
               packageRoot,
               packageName: "entities-spec",
               modelFiles,
               queryRoute: pkg.entitiesQueryRoute(),
               connections: pkg.getMalloyConfig().connections,
            });
            expect(here.plannedHere).toEqual(
               modelFiles
                  .filter((f) => "model" in f)
                  .map((f) => f.relativePath),
            );
            expect(JSON.stringify(pkg.getEntities())).toBe(
               JSON.stringify(toApiEntities(here.results)),
            );
            expect(entitiesWarnings(pkg)).toEqual(
               here.findings.map((finding) => ({
                  model: finding.modelPath,
                  message: finding.message,
                  severity: "warn",
                  code: entitiesFindingCode(finding.reason),
               })),
            );
         });
      }, 120_000);
   }

   test("a plan that left a descent the route takes untaken is planned again", async () => {
      // `reroot` lists only analytics.malloy, so the sources it imports are
      // not queryable, and its queryable `analytics` re-roots the joined
      // `order_items`: a descent only a boundary takes.
      await withLoadedPackage(GOLDEN_PACKAGES.reroot!, async (pkg, root) => {
         const modelFiles = await pkg.entitiesModelFiles();
         const connections = pkg.getMalloyConfig().connections;
         const unprovisional = new Map<string, ModelEntitiesPlan>();
         for (const file of modelFiles) {
            if (!("model" in file)) continue;
            unprovisional.set(
               file.relativePath,
               await planModelFileEntities(file, {
                  packageRoot: root,
                  connections,
                  provisional: false,
               }),
            );
         }
         const resolved = await extractPackageEntities({
            packageRoot: root,
            modelFiles,
            queryRoute: pkg.entitiesQueryRoute(),
            connections,
            plans: unprovisional,
         });
         expect(resolved.plannedHere).toContain("analytics.malloy");
         expect(JSON.stringify(toApiEntities(resolved.results))).toBe(
            JSON.stringify(pkg.getEntities()),
         );
      });
   }, 120_000);

   test("a plan from another package root is not used", async () => {
      await withLoadedPackage(
         GOLDEN_PACKAGES["query-surface"]!,
         async (pkg, root) => {
            const modelFiles = await pkg.entitiesModelFiles();
            const connections = pkg.getMalloyConfig().connections;
            const elsewhere = new Map<string, ModelEntitiesPlan>();
            for (const file of modelFiles) {
               if (!("model" in file)) continue;
               elsewhere.set(
                  file.relativePath,
                  await planModelFileEntities(file, {
                     packageRoot: path.join(root, "elsewhere"),
                     connections,
                     provisional: false,
                  }),
               );
            }
            const resolved = await extractPackageEntities({
               packageRoot: root,
               modelFiles,
               queryRoute: pkg.entitiesQueryRoute(),
               connections,
               plans: elsewhere,
            });
            expect(resolved.plannedHere).toEqual([...elsewhere.keys()]);
            expect(JSON.stringify(toApiEntities(resolved.results))).toBe(
               JSON.stringify(pkg.getEntities()),
            );
         },
      );
   }, 120_000);
});
