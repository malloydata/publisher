// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The package the skills benches and the main-thread block spec load, which
 * declares skill sets at their ceilings.
 *
 * The cap package declares `sets` skill sets (32 is the most a manifest may),
 * one of them `default`, and one workflow per eight of them, the most a workflow
 * lists. Each set holds the inline Markdown files its cap allows, SKILL.md
 * files included, spread over as many skills as the per-folder cap of 1,000
 * needs, and the 200 listed files its asset cap allows, in the first skill's
 * `assets/`. The inline files are sized to fill the set's 256 KiB to the byte
 * it allows, and the listed bytes of all the sets stay under the package's
 * 8 MiB, so every set is served: a set past a cap is refused whole, which is
 * less work than serving it.
 */

import fs from "fs";
import path from "path";
import {
   DECLARED_SKILL_SETS_MAX,
   SKILL_SET_ASSETS_MAX_FILES,
   SKILL_SET_INLINE_MAX_FILES,
   SKILL_SET_MARKDOWN_MAX_BYTES,
   WORKFLOW_SKILL_SETS_MAX,
} from "../../src/service/skills/package_workflows";

const KIB = 1024;
/** Markdown files one `references/` folder holds before its walk stops. */
const REFERENCES_PER_FOLDER = 1000;
/** A set's inline Markdown ceiling, which the files of a shape fill. */
const SET_MARKDOWN_BYTES = SKILL_SET_MARKDOWN_MAX_BYTES;

export interface Shape {
   /** Inline Markdown files per set, each skill's SKILL.md included. */
   inline: number;
   /** Listed files per set, of 1 KiB, in the first skill's `assets/`. */
   listed: number;
}

/** Every per-set cap spent. */
export const CAP_SHAPE: Shape = {
   inline: SKILL_SET_INLINE_MAX_FILES,
   listed: SKILL_SET_ASSETS_MAX_FILES,
};

/** The sets a cap package of the most sets declares. */
export const CAP_SETS = DECLARED_SKILL_SETS_MAX;

const SKILL_MD = (name: string) =>
   `---\nname: ${name}\ndescription: A skill of ${name} that the cap bench builds.\n---\n\n# ${name}\n\nSee references/.\n`;

function write(file: string, bytes: Buffer | string): void {
   fs.mkdirSync(path.dirname(file), { recursive: true });
   fs.writeFileSync(file, bytes);
}

/** One set's skills, named for the set so no two of a workflow's sets share a name: `shape.inline` Markdown files in all, and `shape.listed` assets in the first skill. */
function capSet(dir: string, setIndex: number, shape: Shape): void {
   const skills = Math.ceil(shape.inline / REFERENCES_PER_FOLDER);
   const skillMdBytes = Array.from({ length: skills }, (_, skill) =>
      Buffer.byteLength(SKILL_MD(`s${setIndex}-${skill}`)),
   ).reduce((a, b) => a + b, 0);
   // Each reference takes an equal share of what the SKILL.md files leave of the set's cap.
   const references = shape.inline - skills;
   const bytes =
      references > 0
         ? Math.max(
              16,
              Math.floor((SET_MARKDOWN_BYTES - skillMdBytes) / references),
           )
         : 0;
   let left = shape.inline;
   for (let skill = 0; left > 0; skill++) {
      const name = `s${setIndex}-${skill}`;
      const here = Math.min(left, REFERENCES_PER_FOLDER);
      write(path.join(dir, name, "SKILL.md"), SKILL_MD(name));
      for (let i = 1; i < here; i++) {
         const file = `r${String(i).padStart(4, "0")}.md`;
         write(
            path.join(dir, name, "references", file),
            `#${"x".repeat(bytes - 2)}\n`,
         );
      }
      if (skill === 0) {
         for (let i = 0; i < shape.listed; i++) {
            const file = `f${String(i).padStart(4, "0")}.bin`;
            write(
               path.join(dir, name, "assets", file),
               Buffer.alloc(KIB, 97 + (i % 20)),
            );
         }
      }
      left -= here;
   }
}

/** The declaration of a package: what its manifest's `skillSets` and `workflows` hold. */
export interface Declaration {
   skillSets: Record<string, unknown>;
   workflows: Record<string, unknown>;
}

/** Writes the cap package's files under `root` and returns its declaration. */
export function writeCapPackage(
   root: string,
   sets: number,
   shape: Shape,
): Declaration {
   const names: string[] = [];
   const skillSets: Record<string, unknown> = {};
   for (let s = 0; s < sets; s++) {
      // The first is the default set; the others are named unlike their folders.
      const name = s === 0 ? "default" : `set-${String(s).padStart(2, "0")}`;
      capSet(path.join(root, "sets", `s${s}`), s, shape);
      skillSets[name] = { path: `sets/s${s}` };
      names.push(name);
   }
   write(
      path.join(root, "workflows", "analyst", "instructions.md"),
      "Be useful.\n",
   );
   const workflows: Record<string, unknown> = {};
   for (let w = 0; w * WORKFLOW_SKILL_SETS_MAX < names.length; w++) {
      workflows[`w${w}`] = {
         description: `Lists sets ${w * WORKFLOW_SKILL_SETS_MAX} on`,
         instructions: "workflows/analyst/instructions.md",
         skillSets: names.slice(
            w * WORKFLOW_SKILL_SETS_MAX,
            (w + 1) * WORKFLOW_SKILL_SETS_MAX,
         ),
      };
   }
   return { skillSets, workflows };
}

/** A package with one set and no listed file: the floor. */
export function writeFloorPackage(root: string): Declaration {
   write(path.join(root, "sets", "a", "s", "SKILL.md"), SKILL_MD("s"));
   return { skillSets: { default: { path: "sets/a" } }, workflows: {} };
}
