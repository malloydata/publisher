// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setsArtifactProperty, tagAnnotation } from "../../utils/malloyText";
import type { ReportLayout } from "../Package/documentLocation";
import { loadMalloyTag } from "./loadMalloy";
import {
   isRule,
   readModelNotes,
   Reader,
   translate,
   type Ctx,
} from "./malloyTree";
import { parseTagLines } from "./tagParse";

/**
 * What a file's model-level notes make it, by the server's rule
 * (`readReportTag`, `docs/reports-design.md` §1, §2):
 *
 * - `none`: no model-level `## artifact` note, so not a report.
 * - `unparsed`: an artifact note the tag parser rejects, so not a report.
 * - `kind-removed`: the tag sets the removed `kind=`, so not a report.
 * - `report`: a report, with its layout.
 */
export type ReportTag =
   | { status: "none" }
   | { status: "unparsed"; errors: string[] }
   | { status: "kind-removed"; kind: string }
   | { status: "cells-removed" }
   | { status: "report"; layout: ReportLayout };

type Parsed = { tag: TagReads; log: Array<{ message: string }> };
type TagReads = {
   tag(key: string): TagReads | undefined;
   text(key: string): string | undefined;
   has(key: string): boolean;
};

/** A tag property as text; `text()` throws on a malformed date literal, which reads as no value. */
const textOf = (tag: TagReads, key: string): string | undefined => {
   try {
      return tag.text(key);
   } catch {
      return undefined;
   }
};

/** Malloy's route rule: a routed note (`##(markdown)`, `##"`) or a flag (`##!`) is never a tag. */
const onTagRoute = (note: string) => /^##\|?(?:[ \t\r\n]|$)/.test(note);

/** Why a `kind=` tag makes no report, and the fix: the server's message, word for word. */
export const kindRemovedReason = (kind: string) =>
   `\`kind=${kind}\` is no longer read, so this file is not a report. Fix: remove \`kind=${kind}\` and write \`layout=grid\` or \`layout=linear\`.`;

/** Why a linear tag with no `tiles` makes no report, and the fix: the server's message, capitalized. */
export const CELLS_REMOVED_REASON =
   "A linear report written as `run:` cells is no longer read, so this file is not a report. Fix: list its tiles in `tiles=[…]` (a text tile per prose block, a query tile per view), or remove the `## artifact` tag.";

/**
 * The one reader of a report's tag in the SDK, over a file's model-level
 * notes (`ParsedMalloy.modelNotes`). The server's `readReportTag` is the same
 * rule over the same notes; `reportLayoutParity.json` holds the two together.
 *
 * The layout, first match wins: `layout=grid|linear`; else grid when the tag
 * lists `tiles` (an empty list counts) or a model-level `dashboard { … }` grid
 * tag is present; else linear. A linear tag with no `tiles` key (an empty
 * list counts as one) is the retired cell format, and no report.
 */
export function readReportTag(
   modelNotes: readonly string[],
   parseAnnotation: (lines: string[]) => Parsed,
): ReportTag {
   // A note naming an environment value is dropped before parsing, as the server drops it.
   const motly = modelNotes
      .filter(onTagRoute)
      .filter((note) => !note.includes("@env."))
      .map(tagAnnotation);
   const { tag, errors } =
      motly.length > 0
         ? parseTagLines(parseAnnotation, motly)
         : { tag: undefined, errors: [] };
   const artifact = tag?.tag("artifact");
   if (!artifact)
      return modelNotes.some(setsArtifactProperty)
         ? { status: "unparsed", errors }
         : { status: "none" };
   const kind = textOf(artifact, "kind");
   if (kind !== undefined) return { status: "kind-removed", kind };
   const named = textOf(artifact, "layout");
   const hasTiles = artifact.has("tiles");
   const layout: ReportLayout =
      named === "grid" || named === "linear"
         ? named
         : hasTiles || tag?.tag("dashboard") !== undefined
           ? "grid"
           : "linear";
   if (layout === "linear" && !hasTiles) return { status: "cells-removed" };
   return { status: "report", layout };
}

/**
 * {@link readReportTag} over raw text, parsed for its notes and nothing else:
 * the answer for a file whatever else is wrong with it, as the server reads a
 * file that does not compile.
 */
export async function readReportTagOfText(text: string): Promise<ReportTag> {
   const { parseAnnotation } = await loadMalloyTag();
   let notes: string[] = [];
   try {
      const root = (await translate(text, "file://report.malloy")).parse
         ?.root as Ctx | undefined;
      if (root && isRule(root) && typeof root.getChild === "function")
         notes = readModelNotes(new Reader(text), root);
   } catch {
      notes = [];
   }
   return readReportTag(notes, parseAnnotation as never);
}
