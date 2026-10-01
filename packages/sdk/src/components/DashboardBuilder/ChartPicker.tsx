// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MenuItem, TextField } from "@mui/material";
import { useId } from "react";
import type { CatalogView } from "./catalog";
import type { ChartPick, ChartState } from "./chartLine";

export interface ChartChoice {
   value: ChartState;
   label: string;
}

const LABELS: Record<ChartPick, string> = {
   line_chart: "Line",
   bar_chart: "Bar",
   big_value: "Big value",
   scatter_chart: "Scatter",
   shape_map: "Shape map",
   segment_map: "Segment map",
};

/**
 * What a cell's chart can be set to. `big_value` is offered only for a view that is all aggregates, since the renderer errors on a grouped one, and a map only for a view that already carries that map tag.
 * Whatever the cell has now stays in the list, so the select never shows a value it does not offer.
 */
export function chartChoices(
   view: Pick<CatalogView, "chart" | "aggregateOnly"> | undefined,
   current: ChartState,
): ChartChoice[] {
   const offered = (pick: ChartPick) =>
      current === pick ||
      (pick === "big_value"
         ? view?.aggregateOnly === true
         : pick === "shape_map" || pick === "segment_map"
           ? view?.chart === pick
           : true);
   const picks: ChartPick[] = [
      "line_chart",
      "bar_chart",
      "big_value",
      "scatter_chart",
      "shape_map",
      "segment_map",
   ];
   return [
      { value: "default", label: "Default" },
      { value: "none", label: "No chart (table)" },
      ...picks
         .filter(offered)
         .map((pick) => ({ value: pick, label: LABELS[pick] })),
      ...(current === "custom"
         ? [{ value: "custom" as const, label: "As written" }]
         : []),
   ];
}

export function ChartPicker({
   state,
   view,
   cellLabel,
   disabledReason,
   onOpen,
   onChange,
}: {
   state: ChartState;
   /** The catalog's view this cell runs, when it is one the catalog knows. */
   view: Pick<CatalogView, "chart" | "aggregateOnly"> | undefined;
   /** Which cell this is, so the control is named apart from its neighbours. */
   cellLabel: string;
   disabledReason?: string;
   /** The choices are being looked at, which is when a host may fetch what it needs to offer more. */
   onOpen?: () => void;
   onChange: (next: ChartState) => void;
}) {
   const choices = chartChoices(view, state);
   const reasonId = useId();
   return (
      <TextField
         select
         size="small"
         variant="standard"
         label="Chart"
         value={state}
         disabled={disabledReason !== undefined}
         onChange={(event) => onChange(event.target.value as ChartState)}
         // On screen and described-by, not only in a tooltip that a keyboard or screen reader never reaches.
         helperText={disabledReason}
         FormHelperTextProps={{ id: reasonId }}
         SelectProps={{
            ...(onOpen ? { onOpen } : {}),
            SelectDisplayProps: {
               "aria-label": `Chart, ${cellLabel}`,
               "aria-labelledby": undefined,
               ...(disabledReason ? { "aria-describedby": reasonId } : {}),
            },
         }}
         sx={{ minWidth: 140 }}
      >
         {choices.map((choice) => (
            <MenuItem
               key={choice.value}
               value={choice.value}
               disabled={choice.value === "custom"}
            >
               {choice.label}
            </MenuItem>
         ))}
      </TextField>
   );
}
