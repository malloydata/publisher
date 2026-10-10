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

/** Why a view is absent, so the reason on a disabled choice says whether to wait or to pick another. */
export type ViewStatus = "loading" | "unlisted";

const LABELS: Record<ChartPick, string> = {
   line_chart: "Line",
   bar_chart: "Bar",
   big_value: "Big value",
   scatter_chart: "Scatter",
   shape_map: "Shape map",
   segment_map: "Segment map",
};

/**
 * Every chart a cell can be set to; one the view cannot render is listed disabled with its reason, so it is discoverable rather than missing.
 * Whatever the cell has now stays selectable, so the select never shows a value it does not offer.
 */
export function chartChoices(
   view: Pick<CatalogView, "chart" | "aggregateOnly"> | undefined,
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
   const reasonFor = (pick: ChartPick): string | undefined => {
      if (current === pick) return undefined;
      if (pick === "big_value")
         return view?.aggregateOnly === true
            ? undefined
            : (unknown ?? "Needs a view with only totals (no group by)");
      if (pick === "shape_map" || pick === "segment_map")
         return view?.chart === pick
            ? undefined
            : (unknown ?? "Needs a view that already carries a map chart");
      return undefined;
   };
   const picks: ChartPick[] = [
      "line_chart",
      "bar_chart",
      "big_value",
      "scatter_chart",
      "shape_map",
      "segment_map",
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
   view: Pick<CatalogView, "chart" | "aggregateOnly"> | undefined;
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
