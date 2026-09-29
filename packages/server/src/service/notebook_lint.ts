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
   parseTextOpener,
   readTextBlocks,
   translateToParse,
   type ParseNode,
   type ParseToken,
   type TokenStreamShape,
} from "./notebook";
import { motlyParseErrors, motlyTag, tagNumeric, tagText } from "./motly";

/** One finding on a file under `notebooks/` or `dashboards/`. */
export interface NotebookLintFinding {
   /** 1-based. */
   line: number;
   code: string;
   /** Starts with `Line N:` and ends with the fix. */
   message: string;
   /** An error is a file that cannot do what it says; a warn is one that does something other than it reads as. */
   severity: "warn" | "error";
}

/** `text` is reserved for the dashboard text tile; it is not an unknown kind. */
const KNOWN_KINDS = ["notebook", "dashboard", "text"];

const ARTIFACT_KIND_FIX = "Fix: write `## artifact { kind=notebook }`.";

/** The first `max` characters of a line, for quoting it in a message. */
const quoted = (line: string, max = 60) => line.trim().slice(0, max);

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
   if (
      !parse ||
      !root ||
      typeof root.getChild !== "function" ||
      !tokens ||
      !vocabulary
   ) {
      return [];
   }
   const notebookParse = parse;
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
   const add = (
      line: number,
      code: string,
      message: string,
      severity: "warn" | "error" = "warn",
   ) =>
      findings.push({
         line,
         code,
         severity,
         message: `Line ${line}: ${message}`,
      });

   lintBlocks();
   lintHeadings();

   // The statements, in file order.
   const children: ParseNode[] = [];
   for (let i = 0; i < (root.childCount ?? 0); i++)
      children.push(root.getChild(i));

   let artifact: { text: string; line: number; startIndex: number } | undefined;
   let givensEnabled = false;
   let firstGiven: number | undefined;
   const spans: [number, number][] = [];
   let closerLine: number | undefined;
   // What sits above the artifact tag that only the header may hold, judged once the tag is found.
   const aboveArtifact: { line: number; code: string; what: string }[] = [];
   const modelNotes: string[] = [];
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
            modelNotes.push(noteText);
            if (/^##!\s*experimental\b[\s\S]*\bgivens\b/.test(noteText))
               givensEnabled = true;
            if (!artifact && note.start && isArtifactNoteText(noteText)) {
               artifact = {
                  text: noteText,
                  line: lineOfNode(note),
                  startIndex: note.start.startIndex,
               };
            } else if (!artifact && inNotebooks) {
               const first = noteText.split("\n", 1)[0];
               // Flags, the `"` route (a description) and `(text)` blocks (found on their own) may sit in the header.
               if (
                  !/^##!/.test(noteText) &&
                  !/^#{1,2}\|?"/.test(noteText) &&
                  !parseTextOpener(first)
               ) {
                  aboveArtifact.push({
                     line: lineOfNode(note),
                     code: "notebook-tag-above-artifact",
                     what: quoted(first),
                  });
               }
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
      } else if (inNotebooks && !artifact) {
         const first = firstCodeToken(child);
         aboveArtifact.push({
            line: first?.line ?? lineOfNode(child),
            code: "notebook-statement-above-artifact",
            what: quoted(
               first
                  ? text.slice(map[first.startIndex]).split(/\r?\n/, 1)[0]
                  : nodeText(child).split("\n", 1)[0],
            ),
         });
      }
   });

   // Without an artifact note the file is a helper model, not a served notebook.
   if (inNotebooks && !artifact) return [];
   for (const { line, code, what } of aboveArtifact) {
      add(
         line,
         code,
         `\`${what}\` sits above the \`## artifact\` tag, and only \`##!\` flags, \`//\` comments and \`"\` notes may. Fix: move it below the artifact tag.`,
         "error",
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
   if (artifact && !inNotebooks) lintUnreferencedTextBlocks(artifact);

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

   /** The first token of a statement that is code, not one of the tag lines above it. */
   function firstCodeToken(node: ParseNode): ParseToken | undefined {
      const from = node.start?.startIndex ?? 0;
      const to = node.stop?.stopIndex ?? -1;
      return tokens?.find(
         (t) =>
            t.channel === 0 &&
            t.startIndex >= from &&
            t.stopIndex <= to &&
            !/ANNOTATION/.test(symbolOf(t) ?? ""),
      );
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

   /** A `##` line that reads as a heading or a sentence is model tags to Malloy, and is never shown. */
   function lintHeadings(): void {
      for (const token of tokens as ParseToken[]) {
         if (symbolOf(token) !== "DOC_ANNOTATION") continue;
         const line = tokenText(token).replace(/\r?\n$/, "");
         const content = /^##[ \t]+(\S.*)$/.exec(line)?.[1];
         if (
            content &&
            /^[A-Za-z_]\w*[ \t]+[A-Za-z0-9_]/.test(content) &&
            !/[={]/.test(content)
         ) {
            add(
               token.line,
               "notebook-heading-line",
               `\`${quoted(line)}\` is read as model tags, not shown as prose. Did you mean \`##"\`?`,
            );
         }
      }
   }

   function lintBlocks(): void {
      const list = tokens as ParseToken[];
      for (let i = 0; i < list.length; i++) {
         if (symbolOf(list[i]) !== "DOC_BLOCK_ANNOTATION_BEGIN") continue;
         const opener = tokenText(list[i]).replace(/\r?\n$/, "");
         if (!opener.startsWith("##|")) continue;
         const line = list[i].line;
         const rest = opener.slice(3);
         const textOpener = parseTextOpener(opener);
         if (/^("|\(text\))\S/.test(rest)) {
            add(
               line,
               "notebook-block-opener-spacing",
               `\`${quoted(opener)}\` has no space after the route, so Malloy drops the note. Did you mean \`##|(text) name\` or \`##|"\`?`,
            );
         } else if (textOpener) {
            if (inNotebooks) {
               add(
                  line,
                  "notebook-text-block",
                  `a \`(text)\` block is a dashboard text tile, and a notebook does not show it. Fix: write \`##|"\` to make it a markdown cell, or move the file to ${DASHBOARDS_DIR}/.`,
               );
            }
            if (textOpener.name === undefined) {
               add(
                  line,
                  "notebook-text-block-name",
                  textOpener.rest === ""
                     ? "a `(text)` block needs a name, the tile's entry in `tiles=[…]`. Fix: write `##|(text) name`, where the name is a bare word of letters, digits and underscores."
                     : `\`${quoted(textOpener.rest)}\` is not a valid name for a \`(text)\` block, which takes exactly one bare word. Fix: write \`##|(text) name\`, where the name is letters, digits and underscores and does not start with a digit.`,
                  "error",
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
            // An indented `##|` is quoted prose; a missed closer leaves the next opener at column 0.
            if (!nested && /^##\|/.test(tokenText(list[j]))) nested = list[j];
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

   /** A `(text)` block is a tile only when `tiles` names it. */
   function lintUnreferencedTextBlocks(tagNote: { text: string }): void {
      const tiles = motlyTag([tagNote.text])
         ?.tag("artifact")
         ?.array("tiles")
         ?.map((tile) => tagText(tile));
      for (const block of readTextBlocks(notebookParse, text)) {
         if (block.name === undefined || tiles?.includes(block.name)) continue;
         add(
            block.line,
            "notebook-text-block-unreferenced",
            `the \`(text)\` block \`${block.name}\` is not named by any entry in \`tiles=[…]\`, so no tile shows it. Fix: add \`${block.name} { kind=text }\` to \`tiles\`, or delete the block.`,
         );
      }
   }

   function lintArtifact(tagNote: { text: string; line: number }): void {
      if (inNotebooks) {
         const [parseError] = motlyParseErrors([tagNote.text]);
         if (parseError !== undefined) {
            add(
               tagNote.line,
               "notebook-artifact-unparsed",
               `the \`## artifact\` tag does not parse (${parseError}), so its properties are not read. ${ARTIFACT_KIND_FIX}`,
               "error",
            );
            return;
         }
      }
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
                  : `\`kind=${kind}\` is not a kind Publisher knows (dashboard, notebook). ${ARTIFACT_KIND_FIX}`,
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
            `\`kind=${kind}\` is not a kind Publisher knows (dashboard, notebook). Fix: remove \`kind\`.`,
         );
      }
      if (properties.includes("dashboard_columns")) {
         const alias = tagText(tag, "dashboard_columns") ?? "";
         const canonical = tagNumeric(
            motlyTag(modelNotes)?.tag("dashboard"),
            "columns",
         );
         if (
            canonical !== undefined &&
            tagNumeric(tag, "dashboard_columns") !== canonical
         ) {
            add(
               tagNote.line,
               "notebook-columns-conflict",
               `\`dashboard_columns=${alias}\` in the artifact tag and \`dashboard { columns=${canonical} }\` disagree about the grid width. Fix: keep \`dashboard { columns=… }\` and remove \`dashboard_columns\`.`,
               "error",
            );
         } else {
            add(
               tagNote.line,
               "notebook-columns-alias",
               `\`dashboard_columns=${alias}\` is a deprecated spelling of the grid width. Fix: write \`dashboard { columns=${alias} }\` instead.`,
            );
         }
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
         severity: finding.severity,
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
