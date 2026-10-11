// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What makes a file a report, and which layout it has (`docs/reports-design.md`
 * §1, §2). One rule for the whole server: discovery, the lint, the compile
 * check, the write path and the deprecated dashboard and notebook routes all
 * read it from here, so none of them can disagree about a file.
 *
 * A report is a `.malloy` file in the package, outside `public/`, that carries
 * an artifact tag (a model-level `## artifact`, or a deprecated query-level
 * `# artifact` on a `query:`), and is not the package's root `index.malloy`.
 * A legacy `.malloynb` is also a report (linear, read-only). WHERE a file lives
 * carries no meaning: `dashboards/` and `notebooks/` are ordinary directory
 * names.
 */

import type { Tag } from "@malloydata/malloy-tag";
import {
   INDEX_MODEL_NAME,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
} from "../constants";
import { motlyTag, tagText } from "./motly";
import {
   ANY_ARTIFACT_NOTE,
   artifactTagText,
   hasArtifactLineOutsideBlocks,
   type DocumentKind,
   type LineTest,
} from "./notebook";

/** How a report's tiles are placed: one column top to bottom, or a grid. */
export type ReportLayout = "grid" | "linear";

/** The package directory Publisher serves as raw static files; never a report's home. */
export const PUBLIC_DIR = "public";

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

/** A report's display label: its basename without the extension. Never its identity. */
export function reportName(modelPath: string): string {
   const basename = modelPath.slice(modelPath.lastIndexOf("/") + 1);
   for (const suffix of [MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX]) {
      if (basename.endsWith(suffix)) return basename.slice(0, -suffix.length);
   }
   return basename;
}

/** `layout=` values, and the legacy `kind=` values read as one. */
export function parseLayoutValue(
   value: string | undefined,
): ReportLayout | undefined {
   return value === "grid" || value === "linear" ? value : undefined;
}

/** `kind=dashboard` → grid, `kind=notebook` → linear; anything else names no layout. */
export function layoutOfKind(
   kind: string | undefined,
): ReportLayout | undefined {
   if (kind === "dashboard") return "grid";
   if (kind === "notebook") return "linear";
   return undefined;
}

/** The legacy document kind a layout is served as on the deprecated routes. */
export function kindOfLayout(layout: ReportLayout): DocumentKind {
   return layout === "grid" ? "dashboard" : "notebook";
}

/** What the layout rule reads off a file's tags. */
export interface ReportLayoutFacts {
   /** The model-level artifact tag's `layout`, as written. */
   layout?: string;
   /** The model-level artifact tag's `kind`, as written. */
   kind?: string;
   /** A model-level `## artifact` tag is present. */
   modelTag: boolean;
   /** A query-level `# artifact` tag is present (deprecated). */
   queryTag: boolean;
   /** The model-level tag lists `tiles` (an empty list counts). */
   tiles: boolean;
   /** A model-level `dashboard { … }` grid tag is present. */
   gridTag: boolean;
}

/** Which rule decided a layout, for the lint's fix-its. */
export type LayoutDecidedBy = "layout" | "kind" | "query" | "tiles" | "default";

export interface ResolvedReportLayout {
   layout: ReportLayout;
   decidedBy: LayoutDecidedBy;
   /** Set when `layout=` and `kind=` both name a layout and disagree; `layout=` wins. */
   conflict?: { layout: ReportLayout; kind: ReportLayout };
}

/**
 * §2, in order, first match wins:
 *
 * 1. `layout=` in the model-level tag, or the legacy `kind=` (dashboard → grid,
 *    notebook → linear). Both present and disagreeing is read by `layout=` and
 *    reported as an error by the lint.
 * 2. A query-level `# artifact` is grid.
 * 3. A model-level tag that lists `tiles` or sits beside a `dashboard { … }`
 *    grid tag is grid.
 * 4. Anything else is linear.
 */
export function resolveReportLayout(
   facts: ReportLayoutFacts,
): ResolvedReportLayout {
   const byLayout = facts.modelTag ? parseLayoutValue(facts.layout) : undefined;
   const byKind = facts.modelTag ? layoutOfKind(facts.kind) : undefined;
   if (byLayout) {
      return {
         layout: byLayout,
         decidedBy: "layout",
         ...(byKind &&
            byKind !== byLayout && {
               conflict: { layout: byLayout, kind: byKind },
            }),
      };
   }
   if (byKind) return { layout: byKind, decidedBy: "kind" };
   if (facts.queryTag) return { layout: "grid", decidedBy: "query" };
   if (facts.modelTag && (facts.tiles || facts.gridTag))
      return { layout: "grid", decidedBy: "tiles" };
   return { layout: "linear", decidedBy: "default" };
}

/** The layout facts of a parsed model-level tag (all own `##` notes) and the queries' tags. */
export function layoutFactsOfTags(
   modelTag: Tag | undefined,
   queryTagged: boolean,
): ReportLayoutFacts {
   const artifact = modelTag?.tag("artifact");
   return {
      layout: tagText(artifact, "layout"),
      kind: tagText(artifact, "kind"),
      modelTag: artifact !== undefined,
      queryTag: queryTagged,
      tiles: artifact?.has("tiles") === true,
      gridTag: modelTag?.tag("dashboard") !== undefined,
   };
}

/** The layout facts of a compiled file: its own model notes, and each of its own named queries' notes. */
export function layoutFactsOfNotes(
   modelNotes: readonly string[],
   queryNotes: readonly (readonly string[])[],
): ReportLayoutFacts {
   return layoutFactsOfTags(
      motlyTag(modelNotes),
      queryNotes.some((notes) => queryNotesCarryArtifact(notes)),
   );
}

/** Whether one query's notes carry a parsed `# artifact` tag. */
export function queryNotesCarryArtifact(notes: readonly string[]): boolean {
   return Boolean(motlyTag(notes)?.tag("artifact"));
}

/**
 * A single-`#` tag line (a query-level `# artifact`), as the generous text test
 * reads it: an `artifact` property anywhere outside a quoted string.
 */
export const QUERY_ARTIFACT_NOTE: LineTest = {
   test(text: string): boolean {
      return /^#(?!#)/.test(text) && ANY_ARTIFACT_NOTE.test(text);
   },
};

/**
 * Whether raw file text claims to be a report: an artifact tag on a `#` or `##`
 * line outside a block body. For a file that did not compile, and for text the
 * write path is handed. Generous on purpose: a false negative hides a real
 * report, and the write path's post-reload verify rolls back a file discovery
 * then does not serve.
 */
export function textClaimsToBeAReport(source: string): boolean {
   return hasArtifactLineOutsideBlocks(source, ANY_ARTIFACT_NOTE);
}

/** The layout facts read off raw text, for a file that did not compile or text not yet saved. */
export function layoutFactsOfText(source: string): ReportLayoutFacts {
   const line = artifactTagText(source);
   const tag = line === undefined ? undefined : motlyTag([line]);
   const facts = layoutFactsOfTags(
      tag,
      hasArtifactLineOutsideBlocks(source, QUERY_ARTIFACT_NOTE),
   );
   // A model-level tag that does not parse still says what it is to an author.
   if (line !== undefined && !facts.modelTag) {
      facts.modelTag = true;
      facts.layout = /\blayout\s*=\s*(\w+)/.exec(line)?.[1];
      facts.kind = /\bkind\s*=\s*(\w+)/.exec(line)?.[1];
      facts.tiles = /\btiles\s*=/.test(line);
   }
   return facts;
}

/** The layout of text that claims to be a report, by §2. */
export function layoutOfText(source: string): ReportLayout {
   return resolveReportLayout(layoutFactsOfText(source)).layout;
}

/**
 * The body of the `artifact { … }` property in a tag line, or "" for a bare
 * `# artifact`. Brace-counted, outside quoted strings, so a nested
 * `givens { … }` stays whole.
 */
export function artifactPropertyBody(note: string): string {
   const match = /(^|[\s{])artifact\b/.exec(note);
   if (!match) return "";
   let i = match.index + match[0].length;
   while (i < note.length && /\s/.test(note[i])) i++;
   if (note[i] !== "{") return "";
   const start = i + 1;
   let depth = 0;
   for (; i < note.length; i++) {
      const c = note[i];
      if (c === '"' || c === "'") {
         const quote = c;
         i++;
         while (i < note.length && note[i] !== quote)
            i += note[i] === "\\" ? 2 : 1;
         continue;
      }
      if (c === "{") depth++;
      else if (c === "}") {
         depth--;
         if (depth === 0) return note.slice(start, i).trim();
      }
   }
   return note.slice(start).trim();
}

/**
 * The model-level rewrite of a query-level `# artifact`: the same properties,
 * `layout=grid`, and the query as the one tile. Mechanical, because the
 * query-level tag's properties are a subset of the model-level tag's.
 */
export function queryArtifactRewrite(
   queryName: string,
   queryNotes: readonly string[],
): string {
   const note = queryNotes.find((text) => /\bartifact\b/.test(text)) ?? "";
   const body = artifactPropertyBody(note)
      .replace(/\bkind\s*=\s*\w+\s*/g, "")
      .replace(/\blayout\s*=\s*\w+\s*/g, "")
      .trim();
   const columns = tagText(motlyTag(queryNotes)?.tag("dashboard"), "columns");
   const tile = /^[A-Za-z_][A-Za-z0-9_]*$/.test(queryName)
      ? queryName
      : `\`${queryName}\``;
   return (
      `## artifact { layout=grid${body ? ` ${body}` : ""} tiles=["${tile}"] }` +
      (columns !== undefined ? ` dashboard { columns=${columns} }` : "")
   );
}
