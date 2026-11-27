// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package the skills benches and the main-thread block spec load, which
 * declares workflows and ships skill sets at their caps.
 *
 * The cap package has `sets` skill sets, listed by one `default` workflow and,
 * for the first, by a second workflow too, which reads it once. Each set holds
 * one skill with 1,000 walked entries under `assets/` (25 files of 40 KiB,
 * listed first, then 64-byte files up to the 200-file cap), 999 Markdown files
 * in `references/` (the rest of the 1,000 "other" entries; inlined, so read and
 * hashed), 907 Markdown files in `reference/` and about 1 MiB listed, so the
 * sets list 7.8 MiB of the package's 8 MiB. A ninth set has the same entries
 * and 100 files of 40 KiB (3.9 MiB of its own 4 MiB), which takes the rest of
 * the package's budget in sorted path order. The inline Markdown of a set stays
 * under 256 KiB. This is one skill per set, far under the per-set skill cap.
 */

import fs from "fs";
import path from "path";

const KIB = 1024;
/** Entries one walk budget allows, per location. */
const WALK_ENTRIES = 1000;

export interface Shape {
   /** Fill assets/ to the 1,000 entries its walk budget allows, after the large files. */
   walked: boolean;
   /** Markdown files of 32 bytes in references/, which the "other" budget counts. */
   references: number;
   /** Markdown files of 64 bytes in reference/, beside 7 of 20 KiB. */
   tinyMarkdown: number;
}

/** Every budget spent. */
export const CAP_SHAPE: Shape = {
   walked: true,
   references: 999,
   tinyMarkdown: 900,
};

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

/** Writes the cap package's files under `root` and returns its `workflows` declaration. */
export function writeCapPackage(
   root: string,
   sets: number,
   shape: Shape,
): Record<string, unknown> {
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

/** A package with one set and no listed file: the floor. */
export function writeFloorPackage(root: string): Record<string, unknown> {
   write(path.join(root, "sets", "a", "s", "SKILL.md"), SKILL_MD("s"));
   return {
      default: { description: "The floor", skill_sets: ["sets/a"] },
   };
}
