// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LogMessage } from "@malloydata/malloy";
import { DASHBOARDS_DIR, isDashboardModelPath } from "./dashboard";
import {
   callAccessor,
   codePointMap,
   isArtifactNoteText,
   isNotebookModelPath,
   isRuleNode,
   NOTEBOOKS_DIR,
   translateToParse,
   type ParseNode,
   type ParseToken,
   type TokenStreamShape,
} from "./notebook";
import { motlyTag, tagText } from "./motly";

/** One finding on a file under `notebooks/` or `dashboards/`. */
export interface NotebookLintFinding {
   /** 1-based. */
   line: number;
   code: string;
   /** Starts with `Line N:` and ends with the fix. */
   message: string;
}

/** `text` is reserved for the dashboard text tile; it is not an unknown kind. */
const KNOWN_KINDS = ["notebook", "text"];

const ARTIFACT_KIND_FIX = "Fix: write `## artifact { kind=notebook }`.";

/**
 * Findings that explain why a notebook or dashboard file will not show what its
 * author wrote, read off Malloy's parse tree and token stream. Reads the parse
 * step only, so a file that does not compile still gets the fix-it for the
 * mistake behind its compile error.
 */
export function lintNotebookText(
   modelPath: string,
   text: string,
): NotebookLintFinding[] {
   const inNotebooks = isNotebookModelPath(modelPath);
   if (!inNotebooks && !isDashboardModelPath(modelPath)) return [];
   let parse;
   try {
      parse = translateToParse(text).parse;
   } catch {
      return [];
   }
   const root = parse?.root as ParseNode | undefined;
   const stream = parse?.tokenStream as TokenStreamShape | undefined;
   const vocabulary = stream?.tokenSource?.vocabulary;
   const tokens =
      typeof stream?.getTokens === "function" ? stream.getTokens() : undefined;
   if (!root || typeof root.getChild !== "function" || !tokens || !vocabulary) {
      return [];
   }
   const symbolOf = (token: ParseToken) =>
      vocabulary.getSymbolicName(token.type);
   const map = codePointMap(text);
   const tokenText = (token: ParseToken) =>
      text.slice(map[token.startIndex], map[token.stopIndex + 1]);
   const nodeText = (node: ParseNode) =>
      node.start && node.stop
         ? text.slice(map[node.start.startIndex], map[node.stop.stopIndex + 1])
         : "";
   const lineOfNode = (node: ParseNode) =>
      (node.start as { line?: number } | undefined)?.line ?? 1;
   const findings: NotebookLintFinding[] = [];
   const add = (line: number, code: string, message: string) =>
      findings.push({ line, code, message: `Line ${line}: ${message}` });

   lintBlocks();

   // The statements, in file order.
   const children: ParseNode[] = [];
   for (let i = 0; i < (root.childCount ?? 0); i++)
      children.push(root.getChild(i));

   let artifact: { text: string; line: number; startIndex: number } | undefined;
   let givensEnabled = false;
   let firstGiven: number | undefined;
   const spans: [number, number][] = [];
   let closerLine: number | undefined;
   const runsAbove: number[] = [];
   children.forEach((child, index) => {
      if (!isRuleNode(child)) {
         const token = child?.symbol;
         const name = token ? symbolOf(token) : undefined;
         if (!token || name === "EOF" || name === "SEMI") return;
         if (closerLine !== undefined) {
            add(
               closerLine,
               "notebook-block-closed-early",
               `this \`|##\` closes the block, so the text after it on line ${token.line} is not prose and does not compile. Fix: a body line cannot start with \`|##\`, so reword it if the block should go on, or delete the stray text if the block is over.`,
            );
            closerLine = undefined;
         }
         return;
      }
      closerLine = undefined;
      if (child.start && child.stop)
         spans.push([child.start.startIndex, child.stop.stopIndex]);
      const group = callAccessor(child, "docAnnotations") as
         | ParseNode
         | undefined;
      if (group) {
         const notes = (callAccessor(group, "docAnnotation") ??
            []) as ParseNode[];
         for (const note of notes) {
            const noteText = nodeText(note);
            if (/^##!\s*experimental\b[\s\S]*\bgivens\b/.test(noteText))
               givensEnabled = true;
            if (!artifact && note.start && isArtifactNoteText(noteText)) {
               artifact = {
                  text: noteText,
                  line: lineOfNode(note),
                  startIndex: note.start.startIndex,
               };
            }
         }
         const last = notes[notes.length - 1];
         if (last && /^##\|/.test(nodeText(last))) {
            closerLine = (last.stop as { line?: number } | undefined)?.line;
         }
         return;
      }
      if (callAccessor(child, "defineGivenStatement")) {
         firstGiven ??= keywordLine(child, "GIVEN");
      }
      if (inNotebooks && callAccessor(child, "runStatement") && !artifact) {
         runsAbove.push(keywordLine(child, "RUN"));
      }
      if (inNotebooks && callAccessor(child, "ignoredObjectAnnotations")) {
         const next = children[index + 1];
         const nextRun = children
            .slice(index + 1)
            .find((sibling) => callAccessor(sibling, "runStatement"));
         add(
            lineOfNode(child),
            "notebook-orphaned-tag",
            `this # tag is followed by ${describeNext(next)}, not by a run:, so it annotates nothing. Render tags sit directly above their run:. Fix: move the tag, and any #" caption, directly above ${nextRun ? `the run: on line ${keywordLine(nextRun, "RUN")}` : "its run:"}.`,
         );
      }
   });

   // Without an artifact note the file is a helper model, not a served notebook.
   if (inNotebooks && !artifact) return [];
   for (const line of runsAbove) {
      add(
         line,
         "notebook-run-above-artifact",
         "this run: sits above the `## artifact` tag, so it is a definition cell, not a query cell. Fix: move the run: below the artifact tag; the header above it is not cells.",
      );
   }

   if (firstGiven !== undefined && !givensEnabled) {
      add(
         firstGiven,
         "notebook-givens-not-enabled",
         "`given:` needs the givens experiment, which this file does not enable, so it does not compile. Fix: add the line `##! experimental.givens` at the top of the file.",
      );
   }

   if (artifact) lintArtifact(artifact);

   if (inNotebooks && artifact) lintComments(artifact.startIndex);

   return findings.sort((a, b) => a.line - b.line);

   /** Comments no cell holds, when they sit directly above the cell they read as describing. */
   function lintComments(from: number): void {
      const all = tokens as ParseToken[];
      const isComment = (t: ParseToken) =>
         t.channel !== 0 && /COMMENT/.test(symbolOf(t) ?? "");
      const endLine = (t: ParseToken) =>
         t.line +
         (tokenText(t)
            .replace(/[\r\n]+$/, "")
            .match(/\n/g)?.length ?? 0);
      // A prose note is a cell of its own, so a comment above one is not describing a statement or tag.
      const isNoteToken = (t: ParseToken) =>
         children.some(
            (c) =>
               c.start !== undefined &&
               c.stop !== undefined &&
               t.startIndex >= c.start.startIndex &&
               t.stopIndex <= c.stop.stopIndex &&
               callAccessor(c, "docAnnotations") !== undefined,
         );
      const attached: boolean[] = [];
      for (let i = all.length - 1; i >= 0; i--) {
         const next = all[i + 1];
         attached[i] =
            next !== undefined &&
            symbolOf(next) !== "EOF" &&
            next.line === endLine(all[i]) + 1 &&
            (isComment(next) ? attached[i + 1] : !isNoteToken(next));
      }
      all.forEach((token, i) => {
         if (!isComment(token) || token.startIndex < from) return;
         if (!attached[i]) return;
         const before = all[i - 1];
         if (before && endLine(before) === token.line) return;
         if (
            spans.some(
               ([lo, hi]) => token.startIndex >= lo && token.stopIndex <= hi,
            )
         )
            return;
         add(
            token.line,
            "notebook-comment-not-shown",
            'this comment sits directly above a cell but is not part of it, so the notebook does not show it. Fix: write it as a `##"` prose note, or move it inside the statement it describes.',
         );
      });
   }

   function describeNext(next: ParseNode | undefined): string {
      if (!next) return "the end of the file";
      if (!isRuleNode(next)) {
         const token = next.symbol;
         return !token || symbolOf(token) === "EOF"
            ? "the end of the file"
            : `\`${tokenText(token)}\``;
      }
      if (callAccessor(next, "docAnnotations")) return "a note";
      const first = nodeText(next).trim().split("\n")[0].slice(0, 40);
      return `\`${first}\``;
   }

   /** The line of a statement's keyword, which is below its tag lines. */
   function keywordLine(node: ParseNode, keyword: string): number {
      const from = node.start?.startIndex ?? 0;
      const to = node.stop?.stopIndex ?? -1;
      const token = tokens?.find(
         (t) =>
            t.channel === 0 &&
            t.startIndex >= from &&
            t.stopIndex <= to &&
            symbolOf(t) === keyword,
      );
      return token?.line ?? lineOfNode(node);
   }

   function lintBlocks(): void {
      const list = tokens as ParseToken[];
      for (let i = 0; i < list.length; i++) {
         if (symbolOf(list[i]) !== "DOC_BLOCK_ANNOTATION_BEGIN") continue;
         const opener = tokenText(list[i]).replace(/\r?\n$/, "");
         if (!opener.startsWith("##|")) continue;
         const line = list[i].line;
         const rest = opener.slice(3);
         if (rest.startsWith('"')) {
            const words = rest.slice(1).trim().split(/\s+/).filter(Boolean);
            if (words.length > 1) {
               add(
                  line,
                  "notebook-multiword-opener",
                  `a \`##|"\` opener takes at most one word, the block's name, but this one has \`${words.join(" ")}\`, and text on the opener line is not shown. Fix: put the prose on the lines below the opener.`,
               );
            } else if (words.length === 1 && inNotebooks) {
               add(
                  line,
                  "notebook-named-block",
                  `this block is named \`${words[0]}\`, and names are for dashboard text tiles; a notebook ignores it. Fix: remove the name from the opener.`,
               );
            }
         } else if (rest.replace(/[\s()]/g, "").toLowerCase() === "markdown") {
            add(
               line,
               "notebook-markdown-opener",
               `\`${opener.trim()}\` opens a block that is not a prose block, so its body is not a ${inNotebooks ? "markdown cell" : "text tile"}. Did you mean \`##|"\`?`,
            );
         }
         let j = i + 1;
         let firstRun: ParseToken | undefined;
         let nested: ParseToken | undefined;
         while (
            j < list.length &&
            symbolOf(list[j]) === "BLOCK_ANNOTATION_TEXT"
         ) {
            if (!firstRun && /^\s*run\s*:/.test(tokenText(list[j])))
               firstRun = list[j];
            if (!nested && /^\s*##\|/.test(tokenText(list[j])))
               nested = list[j];
            j++;
         }
         const end =
            j < list.length && symbolOf(list[j]) === "BLOCK_ANNOTATION_END"
               ? list[j]
               : undefined;
         const swallowed = firstRun
            ? `, including the run: on line ${firstRun.line}, which is prose here and never runs`
            : "";
         if (!end) {
            add(
               line,
               "notebook-unterminated-block",
               `this block is never closed, so it runs to the end of the file and everything after the opener is prose${swallowed}. Fix: add a \`|##\` line where the prose ends.`,
            );
            continue;
         }
         if (nested) {
            add(
               line,
               "notebook-block-swallows-run",
               `this block runs to the \`|##\` on line ${end.line}, and line ${nested.line} inside it opens another block, so a \`|##\` was probably missed before it. Fix: add \`|##\` before line ${nested.line}.`,
            );
         }
         const trailing = tokenText(end).replace(/^\|##/, "").trim();
         if (tokenText(end).startsWith("|##") && trailing) {
            add(
               end.line,
               "notebook-text-after-closer",
               `the text after the closing \`|##\` (\`${trailing}\`) is dropped, not shown. Fix: put it on its own \`##"\` line, or inside the block.`,
            );
         }
      }
   }

   function lintArtifact(tagNote: { text: string; line: number }): void {
      const tag = motlyTag([tagNote.text])?.tag("artifact");
      if (!tag) return;
      const kind = tagText(tag, "kind");
      const properties = Object.keys(tag.dict ?? {});
      if (inNotebooks) {
         if (kind === undefined) {
            add(
               tagNote.line,
               "notebook-kind-missing",
               `this notebook's artifact tag has no \`kind\`. ${ARTIFACT_KIND_FIX}`,
            );
         } else if (kind !== "notebook") {
            add(
               tagNote.line,
               "notebook-kind-unknown",
               KNOWN_KINDS.includes(kind)
                  ? `\`kind=${kind}\` is not a notebook kind. ${ARTIFACT_KIND_FIX}`
                  : `\`kind=${kind}\` is not a kind Publisher knows (notebook). ${ARTIFACT_KIND_FIX}`,
            );
         }
         if (properties.includes("tiles")) {
            add(
               tagNote.line,
               "notebook-tiles",
               `\`tiles\` builds a dashboard grid and does nothing under ${NOTEBOOKS_DIR}/; a notebook's cells are the statements in the file. Fix: remove \`tiles\`, or move the file to ${DASHBOARDS_DIR}/.`,
            );
         }
         return;
      }
      if (kind === "notebook") {
         add(
            tagNote.line,
            "notebook-kind-under-dashboards",
            `\`kind=notebook\` marks a notebook, but this file is under ${DASHBOARDS_DIR}/, which serves dashboards. Fix: move the file to ${NOTEBOOKS_DIR}/, or remove \`kind\`.`,
         );
      } else if (kind !== undefined && !KNOWN_KINDS.includes(kind)) {
         add(
            tagNote.line,
            "notebook-kind-unknown",
            `\`kind=${kind}\` is not a kind Publisher knows (notebook). Fix: remove \`kind\`.`,
         );
      }
   }
}

/** The findings as compile problems, at their lines, for a /compile of a notebook or dashboard file. */
export function notebookLintProblems(
   modelPath: string,
   text: string,
   url: string,
): LogMessage[] {
   return lintNotebookText(modelPath, text).map((finding) => {
      const line = finding.line - 1;
      return {
         code: finding.code,
         severity: "warn",
         message: finding.message,
         at: {
            url,
            range: {
               start: { line, character: 0 },
               end: { line, character: 0 },
            },
         },
      } as LogMessage;
   });
}
