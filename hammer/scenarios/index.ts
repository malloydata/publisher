// Scenario registry. Each scenario is a folder under scenarios/ containing a
// `scenario.md` (and an optional `hooks.ts`). They are discovered + parsed at
// startup by the markdown interpreter.

import { readdirSync, existsSync, readFileSync } from "fs";
import path from "path";
import { parseScenarioFile } from "../lib/scenario_md";
import type { Scenario } from "./framework";

/** Recursively find every folder (at any depth, e.g. under a suite) with a scenario.md. */
function findScenarioDirs(root: string): string[] {
   const found: string[] = [];
   const walk = (dir: string): void => {
      if (existsSync(path.join(dir, "scenario.md"))) found.push(dir);
      for (const e of readdirSync(dir, { withFileTypes: true })) {
         if (e.isDirectory()) walk(path.join(dir, e.name));
      }
   };
   walk(root);
   return found.sort(); // path order — numeric prefixes (01-, 02-) keep suite order
}

/**
 * A scenario whose `scenario.md` could not be parsed, represented as one that FAILS
 * rather than one that is absent. A parse error is an authoring mistake in a single
 * file, so it should not stop the other 57 scenarios from running — but it must
 * never read as "skipped" or vanish, or a typo'd scenario would quietly contribute
 * nothing while the run stays green. It reports red, carries the parse error as its
 * failure detail, and the run exits non-zero.
 *
 * The id comes from the front matter when readable (so `--scenarios <id>` can still
 * select or exclude it) and falls back to the directory name. Tagged `malformed` so
 * `--tags malformed` lists exactly the broken ones.
 */
function malformedScenario(dir: string, err: unknown): Scenario {
   const name = path.basename(dir);
   let id = name;
   try {
      const text = readFileSync(path.join(dir, "scenario.md"), "utf8");
      const fm = text.match(/^---\n([\s\S]*?)\n---/);
      id = fm?.[1].match(/^id:\s*(.+)$/m)?.[1].trim() || name;
   } catch {
      // Unreadable file: the directory name is identification enough.
   }
   const message = err instanceof Error ? err.message : String(err);
   return {
      id,
      tags: ["malformed"],
      title: `MALFORMED scenario.md (${name})`,
      requires: [],
      packages: [],
      run: async (_ctx, assert) => {
         assert.fail(`${name}/scenario.md does not parse`, message);
      },
   };
}

export async function loadScenarios(
   ids?: string[],
   tags?: string[],
): Promise<Scenario[]> {
   const dirs = findScenarioDirs(import.meta.dir);
   let scenarios: Scenario[] = [];
   for (const dir of dirs) {
      try {
         scenarios.push(await parseScenarioFile(dir));
      } catch (err) {
         scenarios.push(malformedScenario(dir, err));
      }
   }

   // `--scenarios` matches the id by substring; `--tags` matches any tag exactly.
   // When both are given they narrow together (a scenario must satisfy each).
   if (ids && ids.length) {
      const want = ids.map((s) => s.toLowerCase());
      scenarios = scenarios.filter((s) =>
         want.some((w) => s.id.toLowerCase().includes(w)),
      );
   }
   if (tags && tags.length) {
      const want = new Set(tags.map((t) => t.toLowerCase()));
      scenarios = scenarios.filter((s) =>
         s.tags.some((t) => want.has(t.toLowerCase())),
      );
   }
   return scenarios;
}
