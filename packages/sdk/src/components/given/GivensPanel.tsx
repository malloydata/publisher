// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Button, Paper, Stack, Typography } from "@mui/material";
import { Given } from "../../client";
import { GivenValue } from "../../hooks/givenValue";
import { useFilterEditing } from "./filterEditing";
import {
   AddFilterButton,
   FilterButton,
   FilterSettingsButton,
} from "./FilterButton";
import { GivenInput } from "./GivenInput";

/**
 * How the control row is laid out.
 *
 * - `panel`: the vertical "Filters" block a notebook shows above its cells.
 * - `bar`: a report's row of filter buttons, one per control, each opening
 *   its control in a popover (`FilterButton`). Both report layouts use it.
 *
 * Two presentations of one control implementation, rather than two
 * implementations: a control gains a behavior (a suggest dropdown, an Apply
 * button) on both surfaces at once.
 */
export type GivensLayout = "panel" | "bar";

export interface GivensPanelProps {
   givens: Given[];
   values: Map<string, GivenValue>;
   onChange: (name: string, value: GivenValue) => void;
   onReset: () => void;
   layout?: GivensLayout;
   /** Resolved `suggest` options per given name. */
   options?: Map<string, string[]>;
   optionsLoading?: boolean;
   /** Givens whose `suggest` query failed, from `useSuggestOptions.failed`. */
   optionsFailed?: ReadonlySet<string>;
   /**
    * Set to render an Apply button, for `autorun=false`. Absent means controls
    * take effect as they change, which is the default everywhere.
    */
   apply?: {
      onApply: () => void;
      /** Whether anything has changed since the last apply. */
      pending: boolean;
   };
   /** Overrides the `panel` layout's "Filters" heading. */
   title?: string;
}

/**
 * Renders the control row: one input per declared `given:`.
 * Returns null when the model declares no givens, so callers can drop
 * `<GivensPanel ... />` unconditionally without a length guard.
 *
 * A "Reset" button appears in the header when at least one value is set; it
 * fires `onReset`, which the parent should wire to `useGivensState.reset`. That
 * puts the document's starting values back rather than emptying the row, so the
 * button and the state agree on what "Reset" means.
 */
export function GivensPanel({
   givens,
   values,
   onChange,
   onReset,
   layout = "panel",
   options,
   optionsLoading,
   optionsFailed,
   apply,
   title,
}: GivensPanelProps) {
   // In the builder the bar is also where filters are added, so it draws with
   // none: an empty row is the "+ Filter" button alone.
   const editing = useFilterEditing();
   if (givens.length === 0 && !(editing && layout === "bar")) return null;
   // Some value actually SET, not merely some entry present. `paramsToGivens`
   // records `?X=` as a null entry, so counting entries offered Reset for a
   // control that is unset, and pressing it changed nothing and left the empty
   // parameter in the URL.
   const hasValues = Array.from(values.values()).some((v) => v !== null);

   const inputs = givens.map((given) => (
      <Box key={given.name}>
         <GivenInput
            given={given}
            value={given.name ? values.get(given.name) : undefined}
            onChange={(next) => given.name && onChange(given.name, next)}
            options={given.name ? options?.get(given.name) : undefined}
            optionsLoading={optionsLoading}
            optionsFailed={
               given.name ? optionsFailed?.has(given.name) : undefined
            }
         />
      </Box>
   ));

   const actions = (
      <Stack direction="row" alignItems="center" spacing={1}>
         {apply && (
            <Button
               variant="contained"
               size="small"
               disableElevation
               onClick={apply.onApply}
               disabled={!apply.pending}
            >
               Apply
            </Button>
         )}
         {hasValues && (
            <Button variant="text" size="small" onClick={onReset}>
               Reset
            </Button>
         )}
      </Stack>
   );

   if (layout === "bar") {
      // A row of filter buttons with no box of its own: the controls are the
      // edges, and a card around them was the one box on the page drawn around
      // other boxes.
      return (
         <Stack
            direction="row"
            role="group"
            aria-label={title ?? "Filters"}
            useFlexGap
            sx={{ flexWrap: "wrap", alignItems: "center", gap: 1 }}
         >
            {givens.map((given) => (
               <FilterButton
                  key={given.name}
                  given={given}
                  value={given.name ? values.get(given.name) : undefined}
                  onChange={(next) => given.name && onChange(given.name, next)}
                  options={given.name ? options?.get(given.name) : undefined}
                  optionsLoading={optionsLoading}
                  optionsFailed={
                     given.name ? optionsFailed?.has(given.name) : undefined
                  }
               />
            ))}
            {editing?.unrendered.map((filter) => (
               <FilterSettingsButton
                  key={filter.name}
                  name={filter.name}
                  label={filter.label}
               />
            ))}
            <AddFilterButton />
            <Box sx={{ ml: "auto" }}>{actions}</Box>
         </Stack>
      );
   }

   return (
      <Paper
         elevation={0}
         sx={{
            p: 3,
            backgroundColor: "transparent",
            border: "none",
            boxShadow: "none",
         }}
      >
         <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            sx={{ mb: 2 }}
         >
            <Typography
               variant="subtitle2"
               sx={{ fontWeight: "fontWeightMedium", color: "text.primary" }}
            >
               {title ?? "Filters"}
            </Typography>
            {actions}
         </Stack>
         <Box
            sx={{
               display: "grid",
               // Equal columns, so controls line up in a grid rather than
               // packing by their own widths. 220px is a deliberate floor: at
               // 250 a four-given notebook came out three-across with a lone
               // control stranded on a second row at the common page width.
               gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
               // Each control is label + field + a helper line or two, so rows
               // are top-aligned; a taller helper must not push its neighbours'
               // fields down.
               alignItems: "start",
               columnGap: 3,
               rowGap: 2.5,
            }}
         >
            {inputs}
         </Box>
      </Paper>
   );
}
