// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ListItemText, MenuItem, TextField } from "@mui/material";
import { useId } from "react";
import type { CatalogView } from "./catalog";
import type { ChartPick, ChartState } from "./chartLine";

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

/**
 * Every chart a cell can be set to; one the view cannot render is listed disabled with its reason, so it is discoverable rather than missing.
 * Whatever the cell has now stays selectable, so the select never shows a value it does not offer.
 */
export function chartChoices(
   view:
      | Pick<CatalogView, "chart" | "aggregateOnly" | "comboShape" | "shape">
      | undefined,
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
   const reasonFor = (pick: ChartPick): string | undefined => {
      if (current === pick) return undefined;
      if (pick === "big_value")
         return view?.aggregateOnly === true
            ? undefined
            : shape
              ? "This view has a group by, so it is more than one row; big numbers need a view of totals only"
              : (unknown ?? "Needs a view with only totals (no group by)");
      // A bare `# combo_chart` draws the first total as bars on the left axis
      // and the second as a line on the right, over the group-by; any other
      // count of totals is an error the renderer shows in the tile.
      if (pick === "combo_chart")
         return view?.comboShape === true || view?.chart === pick
            ? undefined
            : shape && shape.groupBys === 0
              ? NO_X_AXIS("A bar and line chart")
              : shape
                ? `A bar and line chart needs exactly two totals, the first drawn as bars and the second as a line; this view has ${shape.totals}`
                : (unknown ??
                  "Needs a view with a group by and exactly two totals: the first is drawn as bars, the second as a line");
      if (pick === "shape_map" || pick === "segment_map")
         return view?.chart === pick
            ? undefined
            : (unknown ?? "Needs a view that already carries a map chart");
      // The same check, for the charts that plot rows: each needs a field to
      // spread the rows along the x axis, and a total to plot. Unknown shape
      // stays pickable, as before: nothing says it cannot draw.
      if (!shape || view?.chart === pick) return undefined;
      const chart = `A ${LABELS[pick].toLowerCase()} chart`;
      if (shape.groupBys === 0) return NO_X_AXIS(chart);
      if (shape.totals === 0)
         return `This view has no total for the y-axis: ${chart.toLowerCase()} needs at least one aggregate`;
      return undefined;
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
   return [
      { value: "default", label: "From the view" },
      { value: "none", label: "Table" },
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
   view:
      | Pick<CatalogView, "chart" | "aggregateOnly" | "comboShape" | "shape">
      | undefined;
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
