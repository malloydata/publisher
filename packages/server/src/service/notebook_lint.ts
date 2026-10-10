// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type LogMessage } from "@malloydata/malloy";
import { isUnparsedDashboardTagFinding } from "./dashboard";
import {
   attachedNowhereFix,
   callAccessor,
   codePointMap,
   DASHBOARDS_DIR,
   documentKind,
   isArtifactNoteText,
   isDocumentModelPath,
   isMarkdownNote,
   isRuleNode,
   MARKDOWN_ROUTE,
   NOTEBOOKS_DIR,
   parseMarkdownOpener,
   readMarkdownBlocks,
   routeOfNote,
   TEXT_BLOCK_NAME,
   translateToParse,
   type ParseNode,
   type ParseToken,
   type TokenStreamShape,
} from "./notebook";
import {
   docCommentText,
   motlyParseErrors,
   motlyTag,
   tagNumeric,
   tagText,
} from "./motly";

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

const TILE_ENTRY_FIX =
   'Fix: every `source -> view` entry in `tiles` is a quoted string (`"orders_tiles -> headline"`), and a text entry is a bare name followed by `{ kind=text }` (`intro { kind=text }`).';

/** The first `max` characters of a line, for quoting it in a message. */
const quoted = (line: string, max = 60) => line.trim().slice(0, max);

/** A name a text tile can take, made from a token that is not one. */
const asTileName = (token: string) => {
   const name = token.replace(/[^A-Za-z0-9_]/g, "_");
   return /^[A-Za-z_]/.test(name) ? name : `_${name}`;
};

const tileEntryFix = (name: string) =>
   `write \`##|(markdown) ${name}\` and list \`${name} { kind=text }\` in \`tiles\``;

/** A text tile for prose that cannot sit on the opener line: the name goes there and the prose below it. */
const tileBlockFix = (name: string, body: string) =>
   `write \`##|(markdown) ${name}\`, put \`${body}\` on the line below it, and list \`${name} { kind=text }\` in \`tiles\``;

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
   if (!isDocumentModelPath(modelPath)) return [];
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
   // The artifact tag decides what the file is; the folder only when the tag names no kind.
   let tagKind: string | undefined;
   let hasTiles = false;
   for (let i = 0; i < tokens.length; i++) {
      const name = symbolOf(tokens[i]);
      if (name !== "DOC_ANNOTATION" && name !== "DOC_BLOCK_ANNOTATION_BEGIN")
         continue;
      let note = tokenText(tokens[i]);
      for (
         let j = i + 1;
         name === "DOC_BLOCK_ANNOTATION_BEGIN" &&
         j < tokens.length &&
         symbolOf(tokens[j]) === "BLOCK_ANNOTATION_TEXT";
         j++
      )
         note += tokenText(tokens[j]);
      if (!isArtifactNoteText(note)) continue;
      const tag = motlyTag([note])?.tag("artifact");
      tagKind = tagText(tag, "kind");
      // A tag that does not parse still says `tiles=`, and the layout lint explains its parse error.
      hasTiles = (tag?.has("tiles") ?? false) || /\btiles\s*=/.test(note);
      break;
   }
   const kind = documentKind(modelPath, tagKind);
   // The cell format only: a notebook written as tiles is read and linted like a dashboard.
   const inNotebooks = kind === "notebook" && !hasTiles;
   const layoutNotebook = kind === "notebook" && hasTiles;
   const page = kind === "notebook" ? "notebook" : "dashboard";
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
   const docNotes: { line: number; text: string }[] = [];
   children.forEach((child, index) => {
      if (!isRuleNode(child)) {
         const token = child?.symbol;
         const name = token ? symbolOf(token) : undefined;
         if (!token || name === "EOF" || name === "SEMI") return;
         if (closerLine !== undefined) {
            add(
               closerLine,
               "notebook-block-closed-early",
               closedEarly(token.line),
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
            const route = routeOfNote(noteText);
            const first = noteText.split("\n", 1)[0];
            // A block note ends at its closer, which is not part of the tag text.
            const bodyText = noteText.replace(/\r?\n\|##[^\n]*\n?$/, "");
            modelNotes.push(bodyText);
            if (/^##!\s*experimental\b[\s\S]*\bgivens\b/.test(noteText))
               givensEnabled = true;
            docNotes.push({ line: lineOfNode(note), text: bodyText });
            if (!artifact && note.start && isArtifactNoteText(noteText)) {
               artifact = {
                  text: bodyText,
                  line: lineOfNode(note),
                  startIndex: note.start.startIndex,
               };
            } else if (!artifact && inNotebooks) {
               if (route === MARKDOWN_ROUTE || route === "text") {
                  aboveArtifact.push({
                     line: lineOfNode(note),
                     code: "notebook-markdown-above-artifact",
                     what: quoted(first),
                  });
                  // Flags and the `"` route (a description) may sit in the header.
               } else if (
                  !/^##!/.test(noteText) &&
                  !/^#{1,2}\|?"/.test(noteText)
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
      if (layoutNotebook && callAccessor(child, "runStatement")) {
         const line = keywordLine(child, "RUN");
         add(
            line,
            "notebook-layout-run",
            "a `run:` is never shown in a notebook written as tiles, whose cells are the entries in `tiles=[…]`. Fix: define the query as a `view:` on a source, list `source -> view` in `tiles`, and delete the `run:`.",
         );
      }
      if (callAccessor(child, "ignoredObjectAnnotations")) {
         const notes = leadingNotes(child);
         for (const note of notes.filter((n) => isMarkdownNote(n.text))) {
            add(
               note.line,
               "notebook-markdown-attached-nowhere",
               `\`${quoted(note.text)}\` annotates no statement, since what follows it (the end of the file, an import or export, or a \`##\` model-level note) takes no annotation. Fix: ${inNotebooks ? attachedNowhereFix(/^#\|/.test(note.text)) : `${tileEntryFix("name")}, with the text as its body, or delete it.`}`,
               "error",
            );
         }
         if (inNotebooks && notes.some((n) => !isMarkdownNote(n.text))) {
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
      } else if (inNotebooks && !artifact) {
         for (const note of leadingNotes(child).filter((n) =>
            isMarkdownNote(n.text),
         )) {
            aboveArtifact.push({
               line: note.line,
               code: "notebook-markdown-above-artifact",
               what: quoted(note.text),
            });
         }
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

   // A query-level `# artifact` also makes a dashboard, so only a file with neither tag is a helper.
   const queryLevelArtifact =
      !inNotebooks &&
      (tokens as ParseToken[]).some(
         (token) =>
            symbolOf(token) !== "DOC_ANNOTATION" &&
            /^#\s*artifact\b/.test(tokenText(token)),
      );
   // A helper model's lines would only be noise to quote.
   if (!artifact && !queryLevelArtifact) return [];
   const belowTag = (line: number) =>
      artifact !== undefined && line > artifact.line;
   // Where a statement's own leading notes sit: the only place a `(markdown)` note is read.
   const leadingNoteLines = new Set<number>();
   for (const child of children)
      if (isRuleNode(child) && !callAccessor(child, "docAnnotations"))
         for (const note of leadingNotes(child))
            leadingNoteLines.add(note.line);
   lintBlocks();
   lintHeadings();
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
   if (artifact && !inNotebooks) lintUnreferencedMarkdownBlocks(artifact);
   if (artifact && !inNotebooks) lintDescriptionBelow(artifact.line);

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
            "this comment sits directly above a cell but is not part of it, so the notebook does not show it. Fix: write it as a `##(markdown)` prose note, or move it inside the statement it describes.",
         );
      });
   }

   /** The `#` notes a statement opens with, up to its first token that is not one; a block's is its opener line. */
   function leadingNotes(node: ParseNode): { text: string; line: number }[] {
      const from = node.start?.startIndex ?? 0;
      const to = node.stop?.stopIndex ?? -1;
      const notes: { text: string; line: number }[] = [];
      for (const token of tokens as ParseToken[]) {
         if (token.startIndex < from) continue;
         if (token.startIndex > to) break;
         if (token.channel !== 0) continue;
         const name = symbolOf(token);
         if (name === "ANNOTATION" || name === "BLOCK_ANNOTATION_BEGIN") {
            notes.push({
               text: tokenText(token).replace(/\r?\n$/, ""),
               line: token.line,
            });
         } else if (
            name !== "BLOCK_ANNOTATION_TEXT" &&
            name !== "BLOCK_ANNOTATION_END"
         )
            break;
      }
      return notes;
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
         const kind = symbolOf(token);
         if (kind === "ANNOTATION") {
            lintAttachedLine(token);
            continue;
         }
         if (kind !== "DOC_ANNOTATION") continue;
         const line = tokenText(token).replace(/\r?\n$/, "");
         if (/^##"\S/.test(line)) {
            const rest = quoted(line).slice(3);
            const fix = belowTag(token.line)
               ? inNotebooks
                  ? `Did you mean \`##(markdown) ${rest}\`?`
                  : `Fix: ${tileEntryFix("name")}, with the text as its body.`
               : `Did you mean \`##" ${rest}\`?`;
            add(
               token.line,
               "notebook-block-opener-spacing",
               `\`${quoted(line)}\` has no space after the route, so Malloy drops the note. ${fix}`,
            );
            continue;
         }
         if (/^##\(markdown\)\S/.test(line)) {
            const fix = inNotebooks
               ? `Did you mean \`##(markdown) ${quoted(line).slice(12)}\`?${placement("##", token.line)}`
               : `Fix: ${tileEntryFix("name")}, with the text as its body.`;
            add(
               token.line,
               "notebook-block-opener-spacing",
               `\`${quoted(line)}\` has no space after the route, so Malloy drops the note. ${fix}`,
            );
            continue;
         }
         if (
            /^##\(?markdown\)?(?=[ \t]|$)/i.test(line) &&
            !isMarkdownNote(line)
         ) {
            add(
               token.line,
               "notebook-markdown-opener",
               `\`${quoted(line)}\` is not on the \`(markdown)\` route, so it is not shown as prose. ${
                  inNotebooks
                     ? `Did you mean \`##(markdown)\`?${placement("##", token.line)}`
                     : `Fix: ${tileEntryFix("name")}, with the text as its body.`
               }`,
            );
            continue;
         }
         if (
            !inNotebooks &&
            (isMarkdownNote(line) || routeOfNote(line) === "text")
         ) {
            add(
               token.line,
               "notebook-markdown-block-unnamed",
               `\`${quoted(line)}\` is a floating \`(${routeOfNote(line)})\` line, which a ${page} does not show, since its text tiles are named blocks listed in \`tiles=[…]\`. Fix: ${tileEntryFix("name")}, or delete the line.`,
            );
            continue;
         }
         const content = /^##[ \t]+(\S.*)$/.exec(line)?.[1];
         if (
            content &&
            /^[A-Za-z_]\w*[ \t]+[A-Za-z0-9_]/.test(content) &&
            !/[={]/.test(content)
         ) {
            const fix = !belowTag(token.line)
               ? 'Did you mean `##"`?'
               : inNotebooks
                 ? "Did you mean `##(markdown)`?"
                 : `Fix: ${tileEntryFix("name")}, with the text as its body.`;
            add(
               token.line,
               "notebook-heading-line",
               `\`${quoted(line)}\` is read as model tags, not shown as prose. ${fix}`,
            );
         }
      }
   }

   /** A one-line `#` note that is meant to be a statement's prose, or is prose nothing reads. */
   function lintAttachedLine(token: ParseToken): void {
      const line = tokenText(token).replace(/\r?\n$/, "");
      if (/^#\(markdown\)\S/.test(line)) {
         add(
            token.line,
            "notebook-block-opener-spacing",
            `\`${quoted(line)}\` has no space after the route, so Malloy drops the note. Did you mean \`#(markdown) ${quoted(line).slice(11)}\`?${placement("#", token.line)}`,
         );
      } else if (
         /^#\(?markdown\)?(?=[ \t]|$)/i.test(line) &&
         !isMarkdownNote(line)
      ) {
         add(
            token.line,
            "notebook-markdown-opener",
            `\`${quoted(line)}\` is not on the \`(markdown)\` route, so it is not shown as prose. Did you mean \`#(markdown)\`?${placement("#", token.line)}`,
         );
      } else if (isMarkdownNote(line) && !leadingNoteLines.has(token.line)) {
         add(
            token.line,
            "notebook-markdown-nested",
            nestedMarkdownMessage(line),
         );
      }
   }

   function closedEarly(strayLine: number): string {
      return `this \`|##\` closes the block, so the text after it on line ${strayLine} is not prose and does not compile. Fix: a body line cannot start with \`|##\`, so reword it if the block should go on, or delete the stray text if the block is over.`;
   }

   function nestedMarkdownMessage(note: string): string {
      return `\`${quoted(note)}\` sits inside a statement, where nothing reads a \`(markdown)\` note, so it is not shown. Fix: ${
         inNotebooks
            ? "move it above the statement it describes"
            : `a ${page} reads no attached note, so ${tileEntryFix("name")}, with the text as its body, or delete it`
      }.`;
   }

   /** What else applies to a `(markdown)` spelling fix where the note sits, since a note is read only in its own place. */
   function placement(sigil: "#" | "##", line: number): string {
      if (sigil === "#" && !leadingNoteLines.has(line))
         return inNotebooks
            ? " It also sits inside a statement, where nothing reads a `(markdown)` note, so move it above the statement it describes."
            : " It also sits inside a statement, and a dashboard reads no attached note, so use a text tile or delete it.";
      if (sigil === "##" && inNotebooks && !belowTag(line))
         return " It also sits above the `## artifact` tag, where a `(markdown)` note is not read, so put it below the tag.";
      return "";
   }

   function lintBlocks(): void {
      const list = tokens as ParseToken[];
      for (let i = 0; i < list.length; i++) {
         const begin = symbolOf(list[i]);
         if (
            begin !== "DOC_BLOCK_ANNOTATION_BEGIN" &&
            begin !== "BLOCK_ANNOTATION_BEGIN"
         )
            continue;
         const opener = tokenText(list[i]).replace(/\r?\n$/, "");
         // `##|` is a floating note, `#|` one attached to the statement below it.
         const sigil = begin === "DOC_BLOCK_ANNOTATION_BEGIN" ? "##" : "#";
         if (!opener.startsWith(`${sigil}|`)) continue;
         const closer = `|${sigil}`;
         const line = list[i].line;
         const rest = opener.slice(sigil.length + 1);
         const markdown = parseMarkdownOpener(opener);
         const spelled = `(${markdown?.route})`;
         // Only a top-level statement's leading notes are read; a note nested in a statement is not.
         const topLevel = sigil === "##" || leadingNoteLines.has(line);
         const dashboardTile = sigil === "##" && !inNotebooks;
         const glued = /^("|\(markdown\))(\S.*)$/.exec(rest);
         if (glued) {
            const [, glueRoute, after] = glued;
            // A `"` block is a description above a notebook's tag and in a dashboard, a cell below it.
            const description =
               glueRoute === '"' &&
               (sigil === "#" || !inNotebooks || !belowTag(line));
            // A dashboard's description below its tag is read only when nothing sits above it, so a text tile is the safe fix.
            const asTile =
               dashboardTile && (glueRoute !== '"' || belowTag(line));
            const fix = !asTile
               ? `write \`${sigil}|${description ? '"' : "(markdown)"}\` and put \`${after}\` on the line below it`
               : TEXT_BLOCK_NAME.test(after)
                 ? `put a space after the route, as in \`##|(markdown) ${after}\`, and list \`${after} { kind=text }\` in \`tiles\``
                 : tileBlockFix("name", after);
            add(
               line,
               "notebook-block-opener-spacing",
               `\`${quoted(opener)}\` has no space after the route, so Malloy drops the note. Fix: ${fix}.${asTile || glueRoute === '"' ? "" : placement(sigil, line)}`,
            );
         } else if (markdown) {
            const opensText = markdown.rest !== "" && !markdown.name;
            if (!topLevel) {
               add(
                  line,
                  "notebook-markdown-nested",
                  nestedMarkdownMessage(opener),
               );
            } else if (
               opensText &&
               dashboardTile &&
               !/\s/.test(markdown.rest)
            ) {
               add(
                  line,
                  "notebook-markdown-opener-text",
                  `\`${quoted(markdown.rest)}\` is not a valid name for a \`${spelled}\` tile, which takes one bare word of letters, digits and underscores that does not start with a digit. Fix: ${tileEntryFix(asTileName(markdown.rest))}.`,
                  "error",
               );
            } else if (opensText) {
               add(
                  line,
                  "notebook-markdown-opener-text",
                  `\`${quoted(markdown.rest)}\` follows \`${sigil}|${spelled}\` on its opener line, where only one bare word may go (a name), so the block would show it as its first line. Fix: ${dashboardTile ? tileBlockFix("name", markdown.rest) : "move it into the body, on the line below the opener"}.${placement(sigil, line)}`,
                  "error",
               );
            } else if (markdown.name && (sigil === "#" || inNotebooks)) {
               add(
                  line,
                  "notebook-markdown-block-named",
                  `the name \`${markdown.name}\` on this \`${spelled}\` block means nothing ${sigil === "#" ? "on a block attached to a statement" : "in a notebook, which shows every block as a cell"}, and it is not shown. Fix: remove the name.`,
               );
            } else if (!markdown.name && dashboardTile) {
               add(
                  line,
                  "notebook-markdown-block-unnamed",
                  `an unnamed \`${spelled}\` block is not shown on a ${page}, whose text tiles are named blocks listed in \`tiles=[…]\`. Fix: ${tileEntryFix("name")}, or delete the block.`,
               );
            }
         } else if (/^[ \t]*\(?markdown\)?(?=[ \t]|$)/i.test(rest)) {
            add(
               line,
               "notebook-markdown-opener",
               `\`${opener.trim()}\` opens a block that is not on the \`(markdown)\` route, so its body is not ${sigil === "#" ? "the statement's prose" : inNotebooks ? "a markdown cell" : "a text tile"}. ${dashboardTile ? `Fix: ${tileEntryFix("name")}.` : `Did you mean \`${sigil}|(markdown)\`?${placement(sigil, line)}`}`,
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
            // An indented opener is quoted prose; a missed closer leaves the next opener at column 0.
            if (!nested && tokenText(list[j]).startsWith(`${sigil}|`))
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
               `this block is never closed, so it runs to the end of the file and everything after the opener is prose${swallowed}. Fix: add a \`${closer}\` line where the prose ends.`,
            );
            continue;
         }
         if (nested) {
            add(
               line,
               "notebook-block-swallows-run",
               `this block runs to the \`${closer}\` on line ${end.line}, and line ${nested.line} inside it opens another block, so a \`${closer}\` was probably missed before it. Fix: add \`${closer}\` before line ${nested.line}.`,
            );
         }
         const trailing = tokenText(end).replace(closer, "").trim();
         if (tokenText(end).startsWith(closer) && trailing) {
            const own =
               sigil !== "##"
                  ? ""
                  : !belowTag(end.line)
                    ? 'on its own `##"` line, or '
                    : inNotebooks
                      ? "on its own `##(markdown)` line, or "
                      : `in its own text tile (${tileEntryFix("name")}), or `;
            add(
               end.line,
               "notebook-text-after-closer",
               `the text after the closing \`${closer}\` (\`${trailing}\`) is dropped, not shown. Fix: put it ${own}inside the block.`,
            );
         }
      }
   }

   /** A dashboard with no description above its tag reads one from below it, where a notebook's would be a cell. */
   function lintDescriptionBelow(artifactLine: number): void {
      const prose = (n: { text: string }) => docCommentText([n.text]);
      if (docNotes.some((n) => n.line < artifactLine && prose(n) !== undefined))
         return;
      const below = docNotes.find(
         (n) => n.line > artifactLine && prose(n) !== undefined,
      );
      if (!below) return;
      add(
         below.line,
         "notebook-description-below-artifact",
         `this \`"\` note below \`## artifact\` is the ${page}'s description only because nothing sits above the tag. Fix: move it above \`## artifact\`.`,
      );
   }

   /** A `(markdown)` block is a tile only when `tiles` names it with `{ kind=text }`. */
   function lintUnreferencedMarkdownBlocks(tagNote: { text: string }): void {
      if (motlyParseErrors([tagNote.text])[0] !== undefined) return;
      const entries =
         motlyTag([tagNote.text])?.tag("artifact")?.array("tiles") ?? [];
      const textTiles = entries
         .filter((tile) => tagText(tile, "kind") === "text")
         .map((tile) => tagText(tile));
      const bare = entries
         .filter((tile) => tagText(tile, "kind") !== "text")
         .map((tile) => tagText(tile));
      for (const block of readMarkdownBlocks(notebookParse, text)) {
         if (block.name === undefined || textTiles.includes(block.name))
            continue;
         add(
            block.line,
            "notebook-markdown-block-unreferenced",
            bare.includes(block.name)
               ? `the \`(${block.route})\` block \`${block.name}\` is named by a tile with no \`kind=text\`, so that tile is read as a query and the block is not shown. Fix: write \`${block.name} { kind=text }\` in \`tiles\`.`
               : `the \`(${block.route})\` block \`${block.name}\` is not named by any entry in \`tiles=[…]\`, so it is not shown on the ${page}. Fix: delete the block, or list \`${block.name} { kind=text }\` in \`tiles\`.`,
         );
      }
   }

   function lintArtifact(tagNote: { text: string; line: number }): void {
      const [parseError] = motlyParseErrors([tagNote.text]);
      if (parseError !== undefined) {
         add(
            tagNote.line,
            "notebook-artifact-unparsed",
            `the \`## artifact\` tag does not parse (${parseError}), so its properties are not read. ${/\btiles\s*=/.test(tagNote.text) ? TILE_ENTRY_FIX : inNotebooks ? ARTIFACT_KIND_FIX : "Fix: correct the tag so it parses."}`,
            "error",
         );
         return;
      }
      const tag = motlyTag([tagNote.text])?.tag("artifact");
      if (!tag) return;
      const declared = tagText(tag, "kind");
      const properties = Object.keys(tag.dict ?? {});
      const home = kind === "notebook" ? NOTEBOOKS_DIR : DASHBOARDS_DIR;
      if (!modelPath.startsWith(`${home}/`)) {
         add(
            tagNote.line,
            "notebook-other-folder",
            `this ${kind} is served from ${modelPath.split("/")[0]}/, where the other kind is created. It works either way. Fix: move it to ${home}/ if you want folders to match kinds.`,
         );
      }
      if (inNotebooks) {
         if (declared === undefined) {
            add(
               tagNote.line,
               "notebook-kind-missing",
               `this notebook's artifact tag has no \`kind\`. ${ARTIFACT_KIND_FIX}`,
            );
         } else if (declared !== "notebook") {
            add(
               tagNote.line,
               "notebook-kind-unknown",
               KNOWN_KINDS.includes(declared)
                  ? `\`kind=${declared}\` is not a notebook kind. ${ARTIFACT_KIND_FIX}`
                  : `\`kind=${declared}\` is not a kind Publisher knows (dashboard, notebook). ${ARTIFACT_KIND_FIX}`,
            );
         }
         return;
      }
      if (declared === "text") {
         add(
            tagNote.line,
            "notebook-kind-text-on-dashboard",
            "`kind=text` marks a tile entry, so it does not mark this document. Fix: remove `kind`, or write `kind=dashboard` or `kind=notebook`.",
         );
      } else if (declared !== undefined && !KNOWN_KINDS.includes(declared)) {
         add(
            tagNote.line,
            "notebook-kind-unknown",
            `\`kind=${declared}\` is not a kind Publisher knows (dashboard, notebook). Fix: remove \`kind\`.`,
         );
      }
      if (layoutNotebook) {
         for (const entry of tag.array("tiles") ?? []) {
            const ignored = ["colspan", "break"].filter((p) => entry.has(p));
            if (ignored.length === 0) continue;
            add(
               tagNote.line,
               "notebook-tile-layout-ignored",
               `\`${ignored.join("`, `")}\` on the \`${tagText(entry) ?? "a"}\` entry is ignored, since a notebook is one column and every tile fills it. Fix: remove ${ignored.length > 1 ? "them" : "it"}.`,
            );
         }
      }
      const columns = motlyTag(modelNotes)?.tag("dashboard");
      if (layoutNotebook && columns?.has("columns")) {
         const width = tagNumeric(columns, "columns");
         if (width !== 1) {
            add(
               tagNote.line,
               "notebook-columns-ignored",
               "`dashboard { columns }` other than 1 is ignored on a notebook, which is always one column. Fix: remove `dashboard { columns }`.",
            );
         }
      }
      // A single query has no grid, and dashboard.ts already says so.
      if (
         properties.includes("dashboard_columns") &&
         properties.includes("tiles")
      ) {
         const alias = tagText(tag, "dashboard_columns") ?? "";
         const dashboardTag = motlyTag(modelNotes)?.tag("dashboard");
         const canonical = dashboardTag?.has("columns")
            ? (tagText(dashboardTag, "columns") ?? "")
            : undefined;
         if (
            canonical !== undefined &&
            tagNumeric(tag, "dashboard_columns") !==
               tagNumeric(dashboardTag, "columns")
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

/**
 * True for a notebook-lint finding that the dashboard lint also reported for
 * the same file, in its own words: an `## artifact` tag that does not parse. A
 * caller that runs both lints drops this copy, so the file is reported once.
 *
 * Decided by what the dashboard lint actually reported, not by the folder. A
 * `dashboards/` file that did not compile, or whose tag makes it a notebook,
 * gets no dashboard finding, and the notebook lint's is then the only one.
 */
export function reportedByDashboardLint(
   finding: { code?: string },
   modelPath: string,
   dashboardFindings: readonly { model?: string; message?: string }[],
): boolean {
   return (
      finding.code === "notebook-artifact-unparsed" &&
      dashboardFindings.some(
         (f) =>
            f.model === modelPath &&
            isUnparsedDashboardTagFinding(f.message ?? ""),
      )
   );
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
