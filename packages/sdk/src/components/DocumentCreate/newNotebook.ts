// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { malloyName, newDocumentProblem, type NewDocument } from "./guards";

/**
 * A new notebook file: the givens flag, the artifact tag, a named import of
 * the one source it runs (reachable without a whole-file import), a text cell
 * and one query cell of the picked view.
 */
export function newNotebookSource(input: NewDocument): string {
   const problem = newDocumentProblem("notebook", input);
   if (problem) throw new Error(problem);
   const { modelPath, source, view } = input;
   const title = input.title.trim();
   const quoted = `"${title.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
   return [
      "##! experimental.givens",
      `## artifact { kind=notebook title=${quoted} }`,
      `import { ${malloyName(source)} } from "../${modelPath}"`,
      "",
      "##|(markdown)",
      `# ${title}`,
      "Say what this notebook is for, then let the queries below answer it.",
      "|##",
      "",
      `run: ${malloyName(source)} -> ${malloyName(view)}`,
      "",
   ].join("\n");
}
