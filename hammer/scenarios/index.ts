// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * A scenario that could not be LOADED — its markdown did not parse, or its hooks.ts
 * was rejected — represented as one that FAILS
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
      title: `MALFORMED scenario (${name})`,
      requires: [],
      packages: [],
      run: async (_ctx, assert) => {
         assert.fail(`${name} failed to load`, message);
      },
   };
}

/**
 * Cross-scenario collisions in state the harness does NOT isolate.
 *
 * Each scenario now gets its own environment, Postgres source database, and DuckLake
 * catalog (see run.ts), so package names and seeded table names are private and two
 * scenarios may reuse them freely — copying a scenario as a template is expected to
 * work. Those are therefore no longer checked; they were, until isolation landed.
 *
 * What remains shared is a source table that names its database EXPLICITLY (`db` on
 * a SourceTable), which is opt-in sharing available to a TypeScript scenario. Two
 * scenarios seeding the same table in the same explicit database do read and write
 * each other's rows, and the symptom is a mystery failure in an unrelated scenario
 * rather than an error where the mistake is — so that one is reported as a FAILURE on
 * each colliding scenario, naming the counterpart, while the rest of the suite runs.
 */
function collisionMessages(scenarios: Scenario[]): Map<string, string[]> {
   const owners = (
      pick: (s: Scenario) => string[],
      label: string,
   ): Map<string, string[]> => {
      const byKey = new Map<string, Set<string>>();
      for (const s of scenarios) {
         for (const key of pick(s)) {
            if (!byKey.has(key)) byKey.set(key, new Set());
            byKey.get(key)!.add(s.id);
         }
      }
      const out = new Map<string, string[]>();
      for (const [key, ids] of byKey) {
         if (ids.size < 2) continue;
         for (const id of ids) {
            const others = [...ids].filter((o) => o !== id).sort();
            if (!out.has(id)) out.set(id, []);
            out
               .get(id)!
               .push(`${label} "${key}" is also used by ${others.join(", ")}`);
         }
      }
      return out;
   };

   const merged = new Map<string, string[]>();
   const add = (m: Map<string, string[]>) => {
      for (const [id, msgs] of m) {
         merged.set(id, [...(merged.get(id) ?? []), ...msgs]);
      }
   };
   // Only EXPLICIT databases are shared; an omitted `db` means the scenario's own.
   // A seed's table name lives in its SQL (`CREATE TABLE <name> (...)`), not as a
   // field, so read it back out.
   add(
      owners(
         (s) =>
            (s.sourceTables ?? [])
               .filter((st) => st.db)
               .flatMap((st) =>
                  [...st.sql.matchAll(/CREATE TABLE\s+(\S+)/gi)].map(
                     (m) => `${st.db}.${m[1]}`,
                  ),
               ),
         "shared source table",
      ),
   );
   return merged;
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

   // Collisions are computed over EVERY scenario, before filtering, so a filtered
   // run still reports one rather than appearing to pass in isolation.
   const collisions = collisionMessages(scenarios);
   if (collisions.size > 0) {
      scenarios = scenarios.map((s) => {
         const msgs = collisions.get(s.id);
         if (!msgs) return s;
         return {
            ...s,
            tags: [...s.tags, "collision"],
            title: `COLLIDES with another scenario (${s.title})`,
            packages: [],
            sourceTables: [],
            connections: [],
            run: async (_ctx, assert) => {
               assert.fail(
                  `${s.id}: shares state with another scenario`,
                  `${msgs.join("; ")} — scenarios share one source database and one ` +
                     `destination, so these must be unique`,
               );
            },
         };
      });
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
