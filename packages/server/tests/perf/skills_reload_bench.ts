// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How long `Package.refreshServingIdentity` blocks the event loop for a
 * package that declares agents and ships skills, at the caps. It walks and
 * hashes synchronously on a package's creation and on every reload, so its
 * wall time is the time nothing else on the server runs.
 *
 *   bun tests/perf/skills_reload_bench.ts [--runs 5] [--agents 8]
 *
 * The cap package has `--agents` agents (8 is the control plane's limit for a
 * draft turn). Each holds one skill with 1,000 walked entries under `assets/`
 * and under `scripts/`, 907 Markdown files in `reference/` (about 198 KiB,
 * inside the 256 KiB inline budget) and about 1 MiB of listed files (the
 * package's 8 MiB split across the agents). A package skill beside them has
 * the same entries and its own 4 MiB in 100 files. Every walk budget is spent,
 * so the numbers are the most one reload does for these skills. The three
 * lines below it each keep one of those loads alone, and a package with one
 * agent and no listed file is the floor.
 *
 * The first call of each package is the creation; the rest are reloads of the
 * same files. The files were just written, so they are in the page cache: a
 * box reading from a cold disk pays more for the bytes.
 */

import fs from "fs";
import os from "os";
import path from "path";
import { Package, type ApiPackage } from "../../src/service/package";
import { computeSourceContentSha } from "../../src/service/package_revision";
import {
   readPackageAgents,
   readSkillsDir,
} from "../../src/service/skills/package_skills";

function arg(name: string, fallback: string): string {
   const argv = process.argv;
   for (let i = 0; i < argv.length; i++) {
      if (argv[i] === `--${name}`) return argv[i + 1] ?? fallback;
      if (argv[i]?.startsWith(`--${name}=`)) {
         return argv[i]!.slice(name.length + 3);
      }
   }
   return fallback;
}

const RUNS = Number(arg("runs", "5"));
const AGENTS = Number(arg("agents", "8"));
const KIB = 1024;
/** Entries one walk budget allows, per location. */
const WALK_ENTRIES = 1000;

interface Shape {
   /** Fill assets/ and scripts/ to the 1,000 entries each walk budget allows. */
   walked: boolean;
   /** Markdown files of 64 bytes in reference/, beside 7 of 20 KiB. */
   tinyMarkdown: number;
}

const SKILL_MD = (name: string) =>
   `---\nname: ${name}\ndescription: A skill of ${name} that the cap bench builds.\n---\n\n# ${name}\n\nSee reference/.\n`;

function write(file: string, bytes: Buffer | string): void {
   fs.mkdirSync(path.dirname(file), { recursive: true });
   fs.writeFileSync(file, bytes);
}

/** `total` files in `dir`: the first `big` of `bigSize` bytes, the rest of 64. */
function fill(
   dir: string,
   big: number,
   bigSize: number,
   total: number,
   ext: string,
): void {
   for (let i = 0; i < total; i++) {
      const size = i < big ? bigSize : 64;
      write(path.join(dir, `f${i}${ext}`), Buffer.alloc(size, 97 + (i % 20)));
   }
}

/** One skill: `bigAssets` listed files of 40 KiB, and the rest of `shape`. */
function capSkill(
   dir: string,
   name: string,
   bigAssets: number,
   shape: Shape,
): void {
   write(path.join(dir, "SKILL.md"), SKILL_MD(name));
   fill(
      path.join(dir, "assets"),
      bigAssets,
      40 * KIB,
      shape.walked ? WALK_ENTRIES : bigAssets,
      ".bin",
   );
   if (shape.walked) {
      fill(path.join(dir, "scripts"), 0, 0, WALK_ENTRIES, ".py");
   }
   fill(
      path.join(dir, "reference"),
      7,
      20 * KIB,
      7 + shape.tinyMarkdown,
      ".md",
   );
}

function writeCapPackage(root: string, agents: number, shape: Shape): unknown {
   const declared: Record<string, unknown> = {};
   for (let a = 0; a < agents; a++) {
      const name = `agent-${a}`;
      write(path.join(root, "agents", name, "instructions.md"), "Be useful.\n");
      // 25 files of 40 KiB each is 1,000 KiB, so 8 agents stay under 8 MiB.
      capSkill(path.join(root, "agents", name, "skills", "s"), "s", 25, shape);
      declared[name] = {
         description: `Agent ${a}`,
         instructions: `agents/${name}/instructions.md`,
         skills: [`agents/${name}/skills`],
      };
   }
   // 100 files of 40 KiB is 4,000 KiB of the package skills' 4 MiB.
   capSkill(path.join(root, "skills", "p"), "p", 100, shape);
   return declared;
}

function writeFloorPackage(root: string): unknown {
   write(path.join(root, "agents", "a", "instructions.md"), "Be useful.\n");
   write(
      path.join(root, "agents", "a", "skills", "s", "SKILL.md"),
      SKILL_MD("s"),
   );
   return {
      a: {
         description: "The floor agent",
         instructions: "agents/a/instructions.md",
         skills: ["agents/a/skills"],
      },
   };
}

function packageAt(root: string, declared: unknown): Package {
   const pkg = new Package(
      "bench",
      "pkg",
      root,
      { name: "pkg" } as ApiPackage,
      [],
      new Map(),
   );
   // What a load sets from publisher.json before it refreshes the identity.
   (pkg as unknown as { rawAgents: unknown }).rawAgents = declared;
   return pkg;
}

const ms = (f: () => void): number => {
   const t = performance.now();
   f();
   return performance.now() - t;
};

const percentile = (values: number[], p: number): number => {
   const sorted = [...values].sort((a, b) => a - b);
   return sorted[
      Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)
   ]!;
};

function summarize(label: string, values: number[]): void {
   const p50 = percentile(values, 0.5).toFixed(1).padStart(7);
   const max = Math.max(...values)
      .toFixed(1)
      .padStart(7);
   const each = values.map((v) => v.toFixed(0)).join(", ");
   console.log(`${label.padEnd(26)} p50 ${p50} ms  max ${max} ms  (${each})`);
}

function measure(title: string, root: string, declared: unknown): void {
   const pkg = packageAt(root, declared);
   const creation = ms(() => pkg.refreshServingIdentity());
   const reloads: number[] = [];
   const skills: number[] = [];
   const agents: number[] = [];
   const hashes: number[] = [];
   for (let i = 0; i < RUNS; i++) {
      reloads.push(ms(() => pkg.refreshServingIdentity()));
      // The same three steps, one at a time.
      let s!: ReturnType<typeof readSkillsDir>;
      let a!: ReturnType<typeof readPackageAgents>;
      skills.push(ms(() => (s = readSkillsDir(root))));
      agents.push(ms(() => (a = readPackageAgents(root, declared))));
      hashes.push(
         ms(() =>
            computeSourceContentSha(
               root,
               [...s.paths, ...a.paths],
               undefined,
               new Map([...s.contentShas, ...a.contentShas]),
            ),
         ),
      );
   }
   console.log(`\n${title}: ${pkg.listAgents().length} agents served`);
   console.log(
      `${"creation (first call)".padEnd(26)} ${creation.toFixed(1)} ms`,
   );
   summarize(`reload, ${RUNS} runs`, reloads);
   summarize("  readSkillsDir", skills);
   summarize("  readPackageAgents", agents);
   summarize("  computeSourceContentSha", hashes);
}

const SCENARIOS: Array<[string, Shape]> = [
   [
      `caps: ${AGENTS} agents, 12 MiB listed, every walk spent`,
      { walked: true, tinyMarkdown: 900 },
   ],
   [
      "listed bytes only (no filler, 7 Markdown)",
      { walked: false, tinyMarkdown: 0 },
   ],
   [
      "walk entries only (1,000 per place, 7 Markdown)",
      { walked: true, tinyMarkdown: 0 },
   ],
   [
      "Markdown only (907 per skill, no filler)",
      { walked: false, tinyMarkdown: 900 },
   ],
];

const work = fs.mkdtempSync(path.join(os.tmpdir(), "skills-reload-bench-"));
try {
   const floor = path.join(work, "floor");
   measure("floor: one agent, SKILL.md only", floor, writeFloorPackage(floor));
   for (const [title, shape] of SCENARIOS) {
      const root = path.join(work, "scenario");
      measure(title, root, writeCapPackage(root, AGENTS, shape));
      fs.rmSync(root, { recursive: true, force: true });
   }
} finally {
   fs.rmSync(work, { recursive: true, force: true });
}
