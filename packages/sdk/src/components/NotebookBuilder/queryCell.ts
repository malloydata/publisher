// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   chartLineText,
   isChartPick,
   type ChartState,
} from "../DashboardBuilder/chartLine";
import { isIdentifier } from "../DashboardBuilder/malloyText";

/** What a query cell the builder adds runs: a view of a source, and an optional caption. */
export interface QueryRun {
   source: string;
   view: string;
   caption?: string;
}

/** A copy of the server's `AUTHORIZE_TAG_LIKE`, kept in step by a parity spec; a caption is a note the server's caller guard reads anywhere. */
export const AUTHORIZE_TAG_LIKE = String.raw`##?\|?[ \t]*(?:[([{<][ \t]*)?(?:(?:(?:row|source)[-_]?)?authorize|access[-_]?filter)(?=[)\]}>]|[ \t]|$)`;

const malloyName = (name: string) =>
   isIdentifier(name) ? name : `\`${name}\``;

/** Why a caption cannot be written, or undefined when it can. */
export function captionProblem(caption: string): string | undefined {
   if (/[\r\n]/.test(caption))
      return "A caption is one line; it cannot hold a line break.";
   if (caption.trim() === "") return "A caption cannot be empty.";
   if (new RegExp(AUTHORIZE_TAG_LIKE, "iu").test(caption))
      return "A caption cannot contain what reads as an access-control tag (authorize, row_authorize, source_authorize or access_filter).";
   return undefined;
}

/** Why `run` cannot be written, or undefined when it can. */
export function queryRunProblem(
   run: QueryRun,
   chart: ChartState | undefined,
   reachable: readonly string[] | undefined,
): string | undefined {
   for (const [what, name] of [
      ["source", run.source],
      ["view", run.view],
   ] as const)
      if (name.trim() === "" || /[`\r\n\\]/.test(name))
         return `The ${what} name ${JSON.stringify(name)} cannot be written as a Malloy name.`;
   if (reachable === undefined)
      return "The sources this notebook can read are not known, so a query cannot be added.";
   if (!reachable.includes(run.source))
      return `The source "${run.source}" is not one this notebook can read.`;
   if (run.caption !== undefined) {
      const problem = captionProblem(run.caption);
      if (problem) return problem;
   }
   if (chart === "custom")
      return "A line the picker does not model cannot be written to a new cell.";
   if (chart !== undefined && chart !== "default" && chart !== "none")
      if (!isChartPick(chart))
         return `"${chart}" is not a chart this editor writes.`;
   return undefined;
}

/** A new query cell: caption, then the chart line, then the run, so its tags sit directly above the statement they annotate. */
export function queryCellText(
   run: QueryRun,
   chart: ChartState | undefined,
   nl = "\n",
): string {
   const lines: string[] = [];
   if (run.caption !== undefined) lines.push(`#" ${run.caption.trim()}`);
   if (chart !== undefined && chart !== "default" && chart !== "custom")
      lines.push(chartLineText(chart));
   lines.push(`run: ${malloyName(run.source)} -> ${malloyName(run.view)}`);
   return lines.map((line) => line + nl).join("");
}
