// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What makes a file a report, and which layout it has (`docs/reports-design.md`
 * §1, §2). One rule for the whole server: discovery, the lint, the compile
 * check, the write path and the deprecated dashboard and notebook routes all
 * read it from here, so none of them can disagree about a file.
 *
 * A report is a `.malloy` file in the package, outside `public/`, whose
 * model-level `## artifact` tag parses and does not set `kind=`, and which is
 * not the package's root `index.malloy`. A legacy `.malloynb` is also a report
 * (linear, read-only). WHERE a file lives carries no meaning: `dashboards/` and
 * `notebooks/` are ordinary directory names.
 */

import {
   INDEX_MODEL_NAME,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
} from "../constants";
import { motlyTag, tagText } from "./motly";
import { isArtifactNoteText, modelNotesOfText } from "./notebook";

/** How a report's tiles are placed: one column top to bottom, or a grid. */
export type ReportLayout = "grid" | "linear";

/** The package directory Publisher serves as raw static files; never a report's home. */
const PUBLIC_DIR = "public";

/**
 * True for a path inside the package's `public/` directory. The first segment
 * is compared without case: on a case-insensitive file system `Public/x.malloy`
 * IS `public/x.malloy`, and refusing it on a case-sensitive one costs nothing.
 * A symlink into `public/` is caught where the write path resolves the real
 * directory (`Environment.writeModelFileTransactional`).
 */
export function isUnderPublicDir(modelPath: string): boolean {
   const first = modelPath.replace(/^\.\//, "").split("/")[0];
   return first.toLowerCase() === PUBLIC_DIR;
}

/**
 * True for a package-relative path that CAN be a report: a `.malloy` or a
 * `.malloynb`, outside `public/`, and not the root `index.malloy` (which is the
 * surface, never a report). Whether it IS one depends on its tags.
 */
export function isReportEligiblePath(modelPath: string): boolean {
   if (
      !modelPath.endsWith(MODEL_FILE_SUFFIX) &&
      !modelPath.endsWith(NOTEBOOK_FILE_SUFFIX)
   )
      return false;
   if (isUnderPublicDir(modelPath)) return false;
   return modelPath !== INDEX_MODEL_NAME;
}

/**
 * Why a report cannot be written at `modelPath` (already a safe relative
 * path), or undefined when it can: a `.malloy` outside `public/` other than
 * the root `index.malloy`, the surface. The root name is compared without
 * case, like `public/`: on a case-insensitive file system `Index.malloy` IS
 * the surface. The one rule for the write route and the write itself.
 */
export function reportWritePathProblem(modelPath: string): string | undefined {
   if (!modelPath.endsWith(MODEL_FILE_SUFFIX))
      return `Only a report, a \`.malloy\` file, can be written here, not \`${modelPath}\`.`;
   if (isUnderPublicDir(modelPath))
      return `\`${modelPath}\` is in \`public/\`, which is served as raw static files, so a report cannot be written there.`;
   if (modelPath.replace(/^\.\//, "").toLowerCase() === INDEX_MODEL_NAME)
      return `\`${INDEX_MODEL_NAME}\` at the package root is the package's surface, never a report, so it cannot be written here.`;
   return undefined;
}

/** A report's display label: its basename without the extension. Never its identity. */
export function reportName(modelPath: string): string {
   const basename = modelPath.slice(modelPath.lastIndexOf("/") + 1);
   for (const suffix of [MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX]) {
      if (basename.endsWith(suffix)) return basename.slice(0, -suffix.length);
   }
   return basename;
}

/** A `layout=` value, or undefined for anything else. */
export function parseLayoutValue(
   value: string | undefined,
): ReportLayout | undefined {
   return value === "grid" || value === "linear" ? value : undefined;
}

/** Which rule decided a layout, for the lint's fix-its. */
type LayoutDecidedBy = "layout" | "tiles" | "default";

/**
 * What a file's model-level notes make it (`docs/reports-design.md` §1, §2).
 *
 * - `none`: no model-level `## artifact` note, so not a report.
 * - `unparsed`: an artifact note the tag parser rejects, so not a report; the
 *   lint says why.
 * - `kind-removed`: the tag sets `kind=`, which is no longer read, so not a
 *   report; the lint says to write `layout=`.
 * - `report`: a report, with its layout.
 */
export type ReportTag =
   | { status: "none" }
   | { status: "unparsed" }
   | { status: "kind-removed"; kind: string }
   | { status: "report"; layout: ReportLayout; decidedBy: LayoutDecidedBy };

/**
 * The one reader of a report's tag, over a file's model-level notes: its own
 * compiled `##` notes, or the same notes read off its text
 * ({@link readReportTagOfText}). Discovery, the lint, the compile check and the
 * write path all read a file through here, so they cannot disagree about it.
 *
 * The layout, first match wins: `layout=grid|linear`; else grid when the tag
 * lists `tiles` (an empty list counts) or a model-level `dashboard { … }` grid
 * tag is present; else linear.
 */
export function readReportTag(modelNotes: readonly string[]): ReportTag {
   const tags = motlyTag(modelNotes);
   const artifact = tags?.tag("artifact");
   if (!artifact)
      return modelNotes.some(isArtifactNoteText)
         ? { status: "unparsed" }
         : { status: "none" };
   const kind = tagText(artifact, "kind");
   if (kind !== undefined) return { status: "kind-removed", kind };
   const layout = parseLayoutValue(tagText(artifact, "layout"));
   if (layout) return { status: "report", layout, decidedBy: "layout" };
   if (artifact.has("tiles") || tags?.tag("dashboard") !== undefined)
      return { status: "report", layout: "grid", decidedBy: "tiles" };
   return { status: "report", layout: "linear", decidedBy: "default" };
}

/** {@link readReportTag} over text that is not compiled: a file that failed to, or text handed to the write path or the compile check. */
export function readReportTagOfText(text: string): ReportTag {
   return readReportTag(modelNotesOfText(text));
}

/**
 * Whether text carries a model-level artifact tag at all, parsed or not: what
 * the write path asks of a body before compiling it, so a tag the author got
 * wrong reaches the compile and its findings rather than "not a report".
 */
export function carriesArtifactTag(text: string): boolean {
   return readReportTagOfText(text).status !== "none";
}
