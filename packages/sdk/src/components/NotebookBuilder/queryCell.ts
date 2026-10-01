// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   chartLineText,
   isChartPick,
   type ChartState,
} from "../DashboardBuilder/chartLine";
import { annotationTextProblem } from "../DashboardBuilder/annotationText";
import { isBareName } from "../DashboardBuilder/malloyText";

/** What a query cell the builder adds runs: a view of a source, and an optional caption. */
export interface QueryRun {
   source: string;
   view: string;
   caption?: string;
}

const malloyName = (name: string) =>
   isBareName(name) ? name : `\`${name}\``;

/** Why a caption cannot be written, or undefined when it can. */
export function captionProblem(caption: string): string | undefined {
   return (
      annotationTextProblem("caption", caption) ??
      (caption.trim() === "" ? "A caption cannot be empty." : undefined)
   );
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
