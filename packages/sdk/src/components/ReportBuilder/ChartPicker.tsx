// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ListItemText, MenuItem, TextField } from "@mui/material";
import { useId } from "react";
import type { CatalogView } from "./catalog";
import { drawsAs, type ChartPick, type ChartState } from "./chartLine";
import { chartShapeGap } from "../RenderedResult/chartShapeProblem";

export interface ChartChoice {
   value: ChartState;
   label: string;
   /** Listed but not pickable; `reason` says what the view would need. */
   disabled?: boolean;
   reason?: string;
}

/** The reason a chart that plots rows cannot draw a view with no group-by: said in the reader's words, not the renderer's. */
const NO_X_AXIS = (chart: string) =>
   `This view has no field for the x-axis: ${chart.toLowerCase()} needs a group by, such as a date or a category`;

/** Why a view is absent, so the reason on a disabled choice says whether to wait or to pick another. */
export type ViewStatus = "loading" | "unlisted";

const LABELS: Record<ChartPick, string> = {
   line_chart: "Line",
   bar_chart: "Bar",
   big_value: "Big value",
   scatter_chart: "Scatter",
   shape_map: "Shape map",
   segment_map: "Segment map",
   combo_chart: "Bar and line",
};

/** A view's own chart, as a reason names it. */
const VIEW_CHART_LABEL = (chart: string | undefined) =>
   chart === "big_value"
      ? "big numbers"
      : chart && chart in LABELS
        ? `a ${LABELS[chart as ChartPick].toLowerCase()} chart`
        : "its own chart";

/**
 * Every chart a cell can be set to; one the view cannot render is listed disabled with its reason, so it is discoverable rather than missing.
 * Whatever the cell has now stays selectable, so the select never shows a value it does not offer.
 */
export function chartChoices(
   view: Pick<CatalogView, "chart" | "aggregateOnly" | "shape"> | undefined,
   current: ChartState,
   viewStatus?: ViewStatus,
): ChartChoice[] {
   const unknown =
      view !== undefined
         ? undefined
         : viewStatus === "loading"
           ? "Still loading this view's details"
           : viewStatus === "unlisted"
             ? "Not a view the catalog lists, so its shape is unknown"
             : undefined;
   const shape = view?.shape;
   // A pick is a line merged over the view's own tags, never one that takes
   // the view's chart off, so it draws only where the renderer would pick it
   // from the merged tag ({@link drawsAs}).
   const overridden = (pick: ChartPick | "none"): string | undefined =>
      view === undefined || current === pick || drawsAs(pick, view.chart)
         ? undefined
         : `This view is drawn as ${VIEW_CHART_LABEL(view.chart)} in the model, which a report tile cannot switch to ${pick === "none" ? "a table" : `a ${LABELS[pick].toLowerCase()} chart`}`;
   const reasonFor = (pick: ChartPick): string | undefined => {
      if (current === pick) return undefined;
      if (pick === "big_value")
         return view?.aggregateOnly === true
            ? overridden(pick)
            : shape && shape.groupBys > 0
              ? "This view has a group by, so it is more than one row; big numbers need a view of totals only"
              : (unknown ?? "Needs a view with only totals (no group by)");
      if (pick === "shape_map" || pick === "segment_map")
         return view?.chart === pick
            ? undefined
            : (unknown ?? "Needs a view that already carries a map chart");
      if (view?.chart === pick) return undefined;
      const drawnOver = overridden(pick);
      if (drawnOver) return drawnOver;
      // The charts that plot rows, by the same rule the result panel applies.
      // An unknown shape leaves them pickable, since nothing says they cannot
      // draw, except a bar and line chart, which few views can fill: a bare
      // `# combo_chart` draws the first total as bars on the left axis and the
      // second as a line on the right, and any other count is an error.
      if (!shape)
         return pick === "combo_chart"
            ? (unknown ??
                 "Needs a view with a group by and exactly two totals: the first is drawn as bars, the second as a line")
            : undefined;
      const chart = `A ${LABELS[pick].toLowerCase()} chart`;
      switch (chartShapeGap(pick, shape)) {
         case "no_x_axis":
            return NO_X_AXIS(chart);
         case "combo_totals":
            return `A bar and line chart needs exactly two totals, the first drawn as bars and the second as a line; this view has ${shape.totals}`;
         case "no_y_axis":
            return `This view has no total for the y-axis: ${chart.toLowerCase()} needs at least one aggregate`;
         default:
            return undefined;
      }
   };
   const picks: ChartPick[] = [
      "line_chart",
      "bar_chart",
      "big_value",
      "scatter_chart",
      "shape_map",
      "segment_map",
      "combo_chart",
   ];
   const tableReason = overridden("none");
   return [
      { value: "default", label: "From the view" },
      {
         value: "none",
         label: "Table",
         ...(tableReason ? { disabled: true, reason: tableReason } : {}),
      },
      ...picks.map((pick): ChartChoice => {
         const reason = reasonFor(pick);
         return {
            value: pick,
            label: LABELS[pick],
            ...(reason ? { disabled: true, reason } : {}),
         };
      }),
      ...(current === "custom"
         ? [{ value: "custom" as const, label: "As written" }]
         : []),
   ];
}

export function ChartPicker({
   state,
   view,
   viewStatus,
   cellLabel,
   disabledReason,
   onOpen,
   onChange,
   variant = "standard",
}: {
   state: ChartState;
   /** The catalog's view this cell runs, when it is one the catalog knows. */
   view: Pick<CatalogView, "chart" | "aggregateOnly" | "shape"> | undefined;
   /** Why `view` is absent, worded into the reasons on the charts that need its shape. */
   viewStatus?: ViewStatus;
   /** Which cell this is, so the control is named apart from its neighbours. */
   cellLabel: string;
   disabledReason?: string;
   /** The choices are being looked at, which is when a host may fetch what it needs to offer more. */
   onOpen?: () => void;
   onChange: (next: ChartState) => void;
   /** The field's look: underlined in a compact menu, outlined beside a dialog's other fields. */
   variant?: "standard" | "outlined";
}) {
   const choices = chartChoices(view, state, viewStatus);
   const reasonId = useId();
   return (
      <TextField
         select
         size="small"
         variant={variant}
         label="Viz type"
         value={state}
         disabled={disabledReason !== undefined}
         onChange={(event) => {
            const next = event.target.value as ChartState;
            // A disabled item still receives a click from a focusable-disabled menu.
            if (!choices.find((choice) => choice.value === next)?.disabled)
               onChange(next);
         }}
         // On screen and described-by, not only in a tooltip that a keyboard or screen reader never reaches.
         helperText={disabledReason}
         FormHelperTextProps={{ id: reasonId }}
         SelectProps={{
            ...(onOpen ? { onOpen } : {}),
            // The menu items carry a reason line; the closed field shows the label alone.
            renderValue: (value) =>
               choices.find((choice) => choice.value === value)?.label ??
               String(value),
            // A disabled choice stays focusable so a keyboard user reaches its reason.
            MenuProps: { MenuListProps: { disabledItemsFocusable: true } },
            SelectDisplayProps: {
               "aria-label": `Viz type, ${cellLabel}`,
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
               disabled={choice.value === "custom" || choice.disabled === true}
            >
               <ListItemText
                  primary={choice.label}
                  {...(choice.reason ? { secondary: choice.reason } : {})}
               />
            </MenuItem>
         ))}
      </TextField>
   );
}
