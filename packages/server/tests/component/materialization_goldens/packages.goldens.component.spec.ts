// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Materialization characterization goldens, package level (phase 5, step 1):
 * a regression net that freezes what materialization v2 does today, so the
 * v1 cutover and the simplification passes can show they changed nothing, or
 * show exactly what they changed.
 *
 * Each case is a package served through the real load path (see
 * package_case.ts for the four goldens it writes). The corpus:
 *
 *  - `conformance/`: every conformance fixture shape (ms2data/service#7098),
 *    in each variant it renders (`storage`, `colocated`, `materialize`);
 *  - `features/`: one package per feature the fixture does not reach
 *    (feature_shapes.ts): pair tables, hidden fields, given terms, rolling
 *    windows, extension tables, rollups, persisted views, gates;
 *  - `fixtures/`: the persist fixture packages under `tests/fixtures/persist-*`.
 *
 * The goldens live under `packages/server/tests/goldens/materialization/`.
 * A golden changes only in a commit that says why. To regenerate after an
 * intended change (from `packages/server`):
 *
 *    UPDATE_GOLDENS=1 bun test --timeout 200000 tests/component/materialization_goldens
 *
 * then review the diff. A mismatch fails with the differing lines and this
 * command.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { DATA_FILES } from "../r4/conformance/data";
import { freshLoadPool } from "../r4/conformance/package_harness";
import { renderFiles, SHAPES, VARIANTS } from "../r4/conformance/shapes";
import { FEATURE_SHAPES } from "./feature_shapes";
import {
   expectGolden,
   removeGoldens,
   slug,
   unclaimedGoldens,
   UPDATING,
} from "./golden_files";
import { type CaseGoldens, type GoldenCase, runCase } from "./package_case";

const FIXTURES = path.resolve(__dirname, "../../fixtures");
const SUFFIXES: (keyof CaseGoldens)[] = [
   "plan.json",
   "build.sql",
   "serve.json",
   "served.txt",
];
const AREAS = ["conformance", "features", "fixtures"];

/** Every file under `dir`, by path relative to it with `/` separators. */
function readTree(dir: string): Record<string, string> {
   const out: Record<string, string> = {};
   const walk = (at: string) => {
      for (const entry of fs.readdirSync(at, { withFileTypes: true })) {
         const full = path.join(at, entry.name);
         if (entry.isDirectory()) walk(full);
         else {
            out[path.relative(dir, full).split(path.sep).join("/")] =
               fs.readFileSync(full, "utf8");
         }
      }
   };
   walk(dir);
   return out;
}

function conformanceCases(): GoldenCase[] {
   const out: GoldenCase[] = [];
   for (const shape of SHAPES) {
      for (const variant of shape.variants ?? VARIANTS) {
         const entry = Object.keys(shape.files)[0];
         out.push({
            stem: `conformance/${slug(shape.name)}.${variant}`,
            title: `conformance: ${shape.name} [${variant}]`,
            files: { ...DATA_FILES, ...renderFiles(shape, variant) },
            entry,
            queries: shape.queries.map((q) => ({
               name: q.name,
               malloy: q.malloy,
               model: q.model,
               givens: q.givens,
            })),
         });
      }
   }
   return out;
}

function featureCases(): GoldenCase[] {
   return FEATURE_SHAPES.map((shape) => ({
      stem: `features/${slug(shape.name)}`,
      title: `feature: ${shape.name}`,
      files: { ...DATA_FILES, ...shape.files },
      entry: Object.keys(shape.files)[0],
      queries: shape.queries,
      env: shape.env,
   }));
}

function fixtureCases(): GoldenCase[] {
   const out: GoldenCase[] = [];
   for (const dir of fs.readdirSync(FIXTURES).sort()) {
      if (!dir.startsWith("persist-")) continue;
      const files = readTree(path.join(FIXTURES, dir));
      const manifest = JSON.parse(files["publisher.json"] ?? "{}");
      files["publisher.json"] = JSON.stringify({ ...manifest, name: "pkg" });
      const entry = Object.keys(files)
         .filter((f) => f.endsWith(".malloy"))
         .sort()[0];
      out.push({
         stem: `fixtures/${dir}`,
         title: `fixture: ${dir}`,
         files,
         entry,
         queries: fixtureQueries(dir),
      });
   }
   return out;
}

/** The queries each persist fixture is served with: a count over each persisted source. */
function fixtureQueries(dir: string): GoldenCase["queries"] {
   const sources: Record<string, string[]> = {
      "persist-advisory-test": ["daily_orders"],
      "persist-incremental-test": ["daily_orders"],
      "persist-multi-level": ["orders_base", "orders_rollup"],
      "persist-schedule-test": ["order_summary"],
      "persist-schedule-warn-test": ["order_summary"],
      "persist-test": ["order_summary"],
   };
   return (sources[dir] ?? []).map((source) => ({
      name: `count ${source}`,
      malloy: `run: ${source} -> { aggregate: n is count() }`,
   }));
}

const CASES: GoldenCase[] = [
   ...conformanceCases(),
   ...featureCases(),
   ...fixtureCases(),
];

const claimed = new Set<string>();

// Every case loads with the materialize switch on, and a load worker reads its
// settings once, at spawn: start from a fresh pool, so another spec file's
// workers do not plan these packages with that file's settings.
beforeAll(freshLoadPool);
afterAll(freshLoadPool);

for (const c of CASES) {
   describe(`materialization goldens: ${c.title}`, () => {
      let goldens: Promise<CaseGoldens> | undefined;
      const once = () => (goldens ??= runCase(c));
      for (const suffix of SUFFIXES) {
         const file = `${c.stem}.${suffix}`;
         claimed.add(file);
         it(`${suffix} matches its golden`, async () => {
            expectGolden(file, (await once())[suffix]);
         });
      }
   });
}

describe("materialization goldens: housekeeping", () => {
   it("leaves no golden that no case produces", () => {
      const stale = AREAS.flatMap((area) => unclaimedGoldens(area, claimed));
      if (UPDATING) {
         removeGoldens(stale);
         return;
      }
      expect(stale).toEqual([]);
   });
});
