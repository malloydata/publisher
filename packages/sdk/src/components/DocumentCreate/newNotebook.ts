// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { malloyName, newDocumentProblem, type NewDocument } from "./guards";

/**
 * A new notebook file in the layout format a dashboard uses: the givens flag,
 * the artifact tag listing an intro text tile and one query tile, a named
 * import of the one source it runs (reachable without a whole-file import),
 * the intro block, and one extension of the source (`<source>_tiles`, the
 * builder's naming) holding the picked view as the query tile.
 */
export function newNotebookSource(input: NewDocument): string {
   const problem = newDocumentProblem("notebook", input);
   if (problem) throw new Error(problem);
   const { modelPath, source, view } = input;
   const title = input.title.trim();
   const quoted = `"${title.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
   const extension = malloyName(`${source}_tiles`);
   const tile = "cell_1";
   // The entry is a quoted string, so a back-quoted extension name is written inside it as is.
   const entry = `"${extension} -> ${tile}"`;
   return [
      "##! experimental.givens",
      `## artifact { kind=notebook title=${quoted} tiles=[intro { kind=text }, ${entry}] }`,
      `import { ${malloyName(source)} } from "../${modelPath}"`,
      "",
      "##|(markdown) intro",
      `# ${title}`,
      "Say what this notebook is for, then let the queries below answer it.",
      "|##",
      "",
      `source: ${extension} is ${malloyName(source)} extend {`,
      `  view: ${tile} is ${malloyName(view)}`,
      "}",
      "",
   ].join("\n");
}
