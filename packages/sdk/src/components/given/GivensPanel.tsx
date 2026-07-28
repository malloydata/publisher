import { Box, Button, Paper, Stack, Typography } from "@mui/material";
import { Given } from "../../client";
import { GivenValue } from "../../hooks/givenValue";
import { GivenInput } from "./GivenInput";

/**
 * How the control row is laid out.
 *
 * - `panel` — the vertical "Parameters" block a notebook shows above its cells.
 * - `bar` — a horizontal filter bar above a dashboard grid.
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
   onClearAll: () => void;
   layout?: GivensLayout;
   /** Resolved `suggest` options per given name. */
   options?: Map<string, string[]>;
   optionsLoading?: boolean;
   /**
    * Set to render an Apply button, for `autorun=false`. Absent means controls
    * take effect as they change, which is the default everywhere.
    */
   apply?: {
      onApply: () => void;
      /** Whether anything has changed since the last apply. */
      pending: boolean;
   };
   /** Overrides the `panel` layout's "Parameters" heading. */
   title?: string;
}

/**
 * Renders the control row — one input per declared `given:`.
 * Returns null when the model declares no givens, so callers can drop
 * `<GivensPanel ... />` unconditionally without a length guard.
 *
 * A "Reset" button appears in the header when at least one value is set; it
 * fires `onClearAll` which the parent should wire to `useGivensState.clearAll`.
 */
export function GivensPanel({
   givens,
   values,
   onChange,
   onClearAll,
   layout = "panel",
   options,
   optionsLoading,
   apply,
   title,
}: GivensPanelProps) {
   if (givens.length === 0) return null;
   const hasValues = values.size > 0;

   const inputs = givens.map((given) => (
      <Box
         key={given.name}
         sx={
            layout === "bar"
               ? { minWidth: 200, flex: "1 1 200px", maxWidth: 320 }
               : undefined
         }
      >
         <GivenInput
            given={given}
            value={given.name ? values.get(given.name) : undefined}
            onChange={(next) => given.name && onChange(given.name, next)}
            options={given.name ? options?.get(given.name) : undefined}
            optionsLoading={optionsLoading}
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
               sx={{ textTransform: "none" }}
            >
               Apply
            </Button>
         )}
         {hasValues && (
            <Button
               variant="text"
               size="small"
               onClick={onClearAll}
               sx={{ textTransform: "none" }}
            >
               Reset
            </Button>
         )}
      </Stack>
   );

   if (layout === "bar") {
      return (
         <Paper
            elevation={0}
            sx={{
               p: 2,
               border: 1,
               borderColor: "divider",
               borderRadius: 1,
               backgroundColor: "background.paper",
            }}
         >
            <Stack
               direction="row"
               alignItems="flex-start"
               spacing={2}
               useFlexGap
               sx={{ flexWrap: "wrap" }}
            >
               {inputs}
               <Box sx={{ flex: "0 0 auto", ml: "auto", pt: 0.5 }}>
                  {actions}
               </Box>
            </Stack>
         </Paper>
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
               sx={{ fontWeight: 600, color: "#333" }}
            >
               {title ?? "Parameters"}
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
