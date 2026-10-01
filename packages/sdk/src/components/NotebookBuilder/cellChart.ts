// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { chartStateOf, type ChartState } from "../DashboardBuilder/chartLine";
import type { QueryChart } from "./readNotebookSource";

/** Why a read cell's chart cannot be changed here, or undefined when it can. */
export function chartLocked(chart: QueryChart | undefined): string | undefined {
   if (!chart) return "This cell has no place for a chart line.";
   if (chart.unmodelled)
      return `This cell has a chart line the editor does not model (${chart.unmodelled}), so its chart cannot be changed here.`;
   if (chart.lines.length > 1)
      return "This cell has more than one chart line, so its chart cannot be changed here.";
   return undefined;
}

/** The state a cell's chart control shows: the document's, else what the file's own lines say. */
export const pickerState = (
   docChart: ChartState | undefined,
   opened: QueryChart | undefined,
): ChartState =>
   docChart ?? chartStateOf((opened?.lines ?? []).map((line) => line.text));
