// Scenario registry. Each scenario is a folder under scenarios/ containing a
// `scenario.md` (and an optional `hooks.ts`). They are discovered + parsed at
// startup by the markdown interpreter.

import { readdirSync, existsSync } from "fs";
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

export async function loadScenarios(
   ids?: string[],
   tags?: string[],
): Promise<Scenario[]> {
   const dirs = findScenarioDirs(import.meta.dir);
   let scenarios: Scenario[] = [];
   for (const dir of dirs) scenarios.push(await parseScenarioFile(dir));

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
