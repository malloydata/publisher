// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How long `Package.refreshServingIdentity` blocks the event loop for a
 * package that declares workflows and ships skill sets, at the caps. It walks
 * and hashes synchronously on a package's creation and on every reload, so its
 * wall time is the time nothing else on the server runs.
 *
 *   bun tests/perf/skills_reload_bench.ts [--runs 5] [--sets 8]
 *
 * The cap package has `--sets` skill sets, listed by one `default` workflow
 * and, for the first, by a second workflow too, which reads it once. Each set
 * holds one skill with 1,000 walked entries under `assets/` (25 files of
 * 40 KiB, listed first, then 64-byte files up to the 200-file cap), 999
 * Markdown files in `references/` (the rest of the 1,000 "other" entries;
 * inlined, so read and hashed), 907 Markdown files in `reference/` and about
 * 1 MiB listed, so the sets list 7.8 MiB of the package's 8 MiB. A ninth set
 * has the same entries and 100 files of 40 KiB (3.9 MiB of its own 4 MiB),
 * which takes the rest of the package's budget in sorted path order. The
 * inline Markdown of a set stays under 256 KiB. This is one skill per set: the
 * walk has no limit on the number of skills, so it is not a ceiling. The lines
 * below it each keep one of those loads alone, and a package with one set and
 * no listed file is the floor.
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
import { readPackageWorkflows } from "../../src/service/skills/package_skills";

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
const SETS = Number(arg("sets", "8"));
const KIB = 1024;
/** Entries one walk budget allows, per location. */
const WALK_ENTRIES = 1000;

interface Shape {
   /** Fill assets/ to the 1,000 entries its walk budget allows, after the large files. */
   walked: boolean;
   /** Markdown files of 32 bytes in references/, which the "other" budget counts. */
   references: number;
   /** Markdown files of 64 bytes in reference/, beside 7 of 20 KiB. */
   tinyMarkdown: number;
}

const SKILL_MD = (name: string) =>
   `---\nname: ${name}\ndescription: A skill of ${name} that the cap bench builds.\n---\n\n# ${name}\n\nSee reference/.\n`;

function write(file: string, bytes: Buffer | string): void {
   fs.mkdirSync(path.dirname(file), { recursive: true });
   fs.writeFileSync(file, bytes);
}

/** `total` files in `dir`: the first `big` of `bigSize` bytes, the rest of `smallSize`. Names are zero-padded so they sort in that order. */
function fill(
   dir: string,
   big: number,
   bigSize: number,
   total: number,
   ext: string,
   smallSize = 64,
): void {
   for (let i = 0; i < total; i++) {
      const size = i < big ? bigSize : smallSize;
      write(
         path.join(dir, `f${String(i).padStart(4, "0")}${ext}`),
         Buffer.alloc(size, 97 + (i % 20)),
      );
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
   if (shape.references > 0) {
      fill(path.join(dir, "references"), 0, 0, shape.references, ".md", 32);
   }
   fill(
      path.join(dir, "reference"),
      7,
      20 * KIB,
      7 + shape.tinyMarkdown,
      ".md",
   );
}

function writeCapPackage(root: string, sets: number, shape: Shape): unknown {
   const paths: string[] = [];
   for (let s = 0; s < sets; s++) {
      const setPath = `sets/s${s}`;
      // 25 files of 40 KiB each is 1,000 KiB, so 8 sets stay under 8 MiB.
      capSkill(path.join(root, setPath, "s"), "s", 25, shape);
      paths.push(setPath);
   }
   // 100 files of 40 KiB is 4,000 KiB of the one set's 4 MiB.
   capSkill(path.join(root, "sets", "p", "p"), "p", 100, shape);
   write(
      path.join(root, "workflows", "analyst", "instructions.md"),
      "Be useful.\n",
   );
   return {
      default: { description: "The advertised skills", skill_sets: paths },
      analyst: {
         description: "Shares a set with default",
         instructions: "workflows/analyst/instructions.md",
         skill_sets: [paths[0], "sets/p"],
      },
   };
}

function writeFloorPackage(root: string): unknown {
   write(path.join(root, "sets", "a", "s", "SKILL.md"), SKILL_MD("s"));
   return {
      default: { description: "The floor", skill_sets: ["sets/a"] },
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
   (pkg as unknown as { rawWorkflows: unknown }).rawWorkflows = declared;
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
   console.log(`${label.padEnd(28)} p50 ${p50} ms  max ${max} ms  (${each})`);
}

function measure(title: string, root: string, declared: unknown): void {
   const pkg = packageAt(root, declared);
   const creation = ms(() => pkg.refreshServingIdentity());
   const reloads: number[] = [];
   const reads: number[] = [];
   const hashes: number[] = [];
   for (let i = 0; i < RUNS; i++) {
      reloads.push(ms(() => pkg.refreshServingIdentity()));
      // The same two steps, one at a time.
      let w!: ReturnType<typeof readPackageWorkflows>;
      reads.push(ms(() => (w = readPackageWorkflows(root, declared))));
      hashes.push(
         ms(() =>
            computeSourceContentSha(root, w.paths, undefined, w.contentShas),
         ),
      );
   }
   let listedFiles = 0;
   let listedBytes = 0;
   const read = readPackageWorkflows(root, declared);
   for (const set of read.sets.values()) {
      for (const skill of set.skills) {
         for (const a of skill.listed) {
            listedFiles++;
            listedBytes += a.bytes;
         }
      }
   }
   console.log(
      `\n${title}: ${read.workflows.size} workflows served over ${read.sets.size} sets, sets list ${listedFiles} files, ${(listedBytes / 1048576).toFixed(1)} MiB`,
   );
   console.log(
      `${"creation (first call)".padEnd(28)} ${creation.toFixed(1)} ms`,
   );
   summarize(`reload, ${RUNS} runs`, reloads);
   summarize("  readPackageWorkflows", reads);
   summarize("  computeSourceContentSha", hashes);
}

const SCENARIOS: Array<[string, Shape]> = [
   [
      `caps: ${SETS} sets and a ninth, every budget spent`,
      { walked: true, references: 999, tinyMarkdown: 900 },
   ],
   [
      "listed bytes only (no filler, 7 Markdown)",
      { walked: false, references: 0, tinyMarkdown: 0 },
   ],
   [
      "assets/ entries only (1,000 per skill)",
      { walked: true, references: 0, tinyMarkdown: 0 },
   ],
   [
      "references/ Markdown only (999 per skill)",
      { walked: false, references: 999, tinyMarkdown: 0 },
   ],
   [
      "reference/ Markdown only (907 per skill)",
      { walked: false, references: 0, tinyMarkdown: 900 },
   ],
];

const work = fs.mkdtempSync(path.join(os.tmpdir(), "skills-reload-bench-"));
try {
   const floor = path.join(work, "floor");
   measure("floor: one set, SKILL.md only", floor, writeFloorPackage(floor));
   for (const [title, shape] of SCENARIOS) {
      const root = path.join(work, "scenario");
      measure(title, root, writeCapPackage(root, SETS, shape));
      fs.rmSync(root, { recursive: true, force: true });
   }
} finally {
   fs.rmSync(work, { recursive: true, force: true });
}
