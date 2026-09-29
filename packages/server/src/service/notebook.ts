// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MODEL_FILE_SUFFIX } from "../constants";
import type { AnnotationNote } from "./annotations";

/** The package-relative directory a served notebook must live in. */
export const NOTEBOOKS_DIR = "notebooks";

/**
 * True for a package-relative path that notebook discovery considers: a
 * `.malloy` directly inside the top-level `notebooks/` directory. Only the
 * candidate filter; the file is a notebook only if it carries a model-level
 * `## artifact` note.
 */
export function isNotebookModelPath(modelPath: string): boolean {
   if (!modelPath.endsWith(MODEL_FILE_SUFFIX)) return false;
   const segments = modelPath.split("/");
   return segments.length === 2 && segments[0] === NOTEBOOKS_DIR;
}

const ARTIFACT_NOTE = /^##[ \t]*artifact\b/;

export function isArtifactNoteText(text: string): boolean {
   return ARTIFACT_NOTE.test(text);
}

/** The 0-based line of the first own `## artifact` note, if the file has one. */
export function artifactNoteLine(
   notes: readonly AnnotationNote[],
): number | undefined {
   return notes.find((n) => isArtifactNoteText(n.text))?.at?.range.start.line;
}

/**
 * The texts of the own notes above the artifact line, of every route, in the
 * order `ownLevelNotes` yields them (block notes first); all of them when the
 * file has no artifact note. The doc-comment reader that consumes them keeps
 * only the `"` route.
 */
export function docNotesAboveArtifact(
   notes: readonly AnnotationNote[],
): string[] {
   const line = artifactNoteLine(notes);
   return notes
      .filter((n) => line === undefined || (n.at?.range.start.line ?? 0) < line)
      .map((n) => n.text);
}

/**
 * Whether raw file text has a line matching `artifact` outside a `##|"` (or
 * `#|`) block body, whose prose could otherwise pass for a tag. For a file that
 * did not compile, where no note can be read.
 */
export function hasArtifactLineOutsideBlocks(
   source: string,
   artifactLine: RegExp,
): boolean {
   let closer: string | undefined;
   for (const line of source.split(/\r?\n/)) {
      const trimmed = line.trimStart();
      if (closer) {
         if (trimmed.startsWith(closer)) closer = undefined;
         continue;
      }
      const opener = /^(#{1,2})\|/.exec(trimmed);
      if (opener) {
         const wanted = `|${opener[1]}`;
         if (!trimmed.includes(wanted, opener[0].length)) closer = wanted;
         continue;
      }
      if (artifactLine.test(trimmed)) return true;
   }
   return false;
}

export function claimsToBeANotebook(source: string): boolean {
   return hasArtifactLineOutsideBlocks(source, ARTIFACT_NOTE);
}
