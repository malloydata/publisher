// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import * as path from "path";

/** A 1x1 PNG: real binary bytes, with a NUL and a byte that is not UTF-8. */
export const PNG = Buffer.from(
   "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
   "base64",
);

export const SKILLS_ROOT = "workflows/analyst/skill_sets/main";
export const SKILL = `${SKILLS_ROOT}/pricing`;

/** The skill's `.skillignore`: a test pattern, a folder with one file kept back, a rule that cannot drop SKILL.md, and a dotfile it cannot bring back. */
export const SKILLIGNORE =
   "*_test.py\nfixtures/*\n!fixtures/keep.csv\nSKILL.md\n!.env\n";

/**
 * What the fixture's skill holds, by path relative to the skill. Inline Markdown
 * (SKILL.md, NOTES.md, references/a.md, reference/rule-of-50.md), listed files (a
 * script, a template, a binary, a `.MD` file, a file kept back by a negation),
 * files that are skipped with a warning (a link, a nested CLAUDE.md), and files
 * the `.skillignore` or the built-in excludes leave out.
 */
export const FILES: Record<string, string | Buffer> = {
   "SKILL.md": [
      "---",
      "name: pricing",
      "description: >-",
      "  How we price deals:",
      "  start from list, then apply the discount rules.",
      "license: MIT",
      "metadata:",
      "  owner: revenue",
      "  reviewed: 2026-11-01",
      "---",
      "",
      "# Pricing",
      "",
      "Run `python3 scripts/run.py`. See references/a.md, reference/rule-of-50.md and NOTES.md.",
      "",
   ].join("\n"),
   "NOTES.md": "# Notes\n\nKept at the root.\n",
   "references/a.md": "# A\n\nDetail.\n",
   "reference/rule-of-50.md": "# The 50% rule\n\nHalf.\n",
   "scripts/run.py": "print('pricing')\n",
   "scripts/run_test.py": "assert True\n",
   "templates/t.html": "<h1>{{ title }}</h1>\n",
   "templates/CLAUDE.md": "Not for the agent computer to read.\n",
   "assets/logo.png": PNG,
   "docs/Guide.MD": "# Guide\n\nUpper-case extension.\n",
   "fixtures/data.csv": "a,b\n1,2\n",
   "fixtures/keep.csv": "a,b\n3,4\n",
   ".skillignore": SKILLIGNORE,
   ".env": "SECRET=1\n",
   ".DS_Store": "junk",
};

/** The skill's link, which is skipped. */
export const LINK = "scripts/run-link.sh";

/** The skill's directory, written under `skillSet` (a package-relative directory). */
export function writeSkill(pkg: string, skillSet: string = SKILLS_ROOT) {
   const dir = path.join(pkg, skillSet, "pricing");
   for (const [relative, contents] of Object.entries(FILES)) {
      const file = path.join(dir, relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, contents);
   }
   fs.symlinkSync("run.py", path.join(dir, LINK));
}

/** The fixture's workflow and the files it names, written under `pkg`; returns the manifest's `workflows`. */
export function writeWorkflow(pkg: string) {
   fs.mkdirSync(path.join(pkg, "workflows/analyst"), { recursive: true });
   fs.writeFileSync(
      path.join(pkg, "workflows/analyst/instructions.md"),
      "Price carefully.\n",
   );
   writeSkill(pkg);
   return {
      analyst: {
         description: "Prices deals",
         instructions: "workflows/analyst/instructions.md",
         skill_sets: [SKILLS_ROOT],
      },
   };
}
