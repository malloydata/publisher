// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CancelIcon from "@mui/icons-material/Cancel";
import FilterListIcon from "@mui/icons-material/FilterList";
import { Box, Chip, Stack, Tooltip, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { SecondaryButton } from "../buttons";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { BuilderControl } from "./controls";

/** The `DashboardBar` is this tall and sticky; the strip pins just below it. */
const TOOLBAR_HEIGHT_PX = 49;

/**
 * The strip under the header where the dashboard's controls are configured —
 * the ONE place: a chip per control opens its window, and "Add filter"
 * declares a new one. The live control row the host renders sits under it.
 */
export function FilterStrip({
   controls: controlList,
   tileCount,
   unknownFieldsOf,
   onEdit,
   onAdd,
   addDisabledReason,
   onRemove,
   children,
}: {
   controls: BuilderControl[];
   tileCount: number;
   /** Bindings a control has that its tiles' sources cannot take. */
   unknownFieldsOf: (name: string, type: string | undefined) => string[];
   onEdit: (control: BuilderControl) => void;
   onAdd: () => void;
   /** Why adding is off; set, the button is disabled and says so. */
   addDisabledReason?: string;
   /** Take a control off the dashboard, as its window's Remove does. */
   onRemove: (name: string) => void;
   /** The host's live control row. */
   children?: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <>
         {/* The filter band, as every dashboard builder has one. The header is the
       dashboard's controls as this FILE has them: a chip per control,
       which opens its window; the × on a chip removes it, as the window's
       Remove does. The live control row the caller
       passes in sits directly under, showing the same controls as a
       reader gets them — from the saved file. */}
         <Stack
            sx={{
               gap: 1,
               position: "sticky",
               // Under the toolbar, which pins at the top of the same scroller.
               top: TOOLBAR_HEIGHT_PX,
               zIndex: 4,
               bgcolor: theme.background,
               pb: 1,
            }}
         >
            <Stack
               direction="row"
               aria-label="Filters"
               sx={{
                  gap: 1,
                  alignItems: "center",
                  flexWrap: "wrap",
                  minHeight: 32,
               }}
            >
               <FilterListIcon
                  sx={{ fontSize: 18, color: theme.tileTitle, opacity: 0.7 }}
               />
               <Typography
                  variant="subtitle2"
                  sx={{ color: theme.tileTitle, mr: 0.5 }}
               >
                  Filters
               </Typography>
               {controlList.length === 0 && (
                  <Typography
                     variant="body2"
                     sx={{ color: theme.tileTitle, opacity: 0.8 }}
                  >
                     None yet.
                  </Typography>
               )}
               {controlList.map((control) => {
                  const unknown = unknownFieldsOf(control.name, control.type);
                  return (
                     <Tooltip
                        key={control.name}
                        title={
                           unknown.length > 0
                              ? `$${control.name} · cannot filter on: ${unknown.join(", ")}`
                              : `$${control.name} · ${
                                   control.origin === "dashboard"
                                      ? "declared here"
                                      : "from the model"
                                } · ${control.boundTiles} of ${tileCount} tiles`
                        }
                     >
                        <Chip
                           size="small"
                           label={control.label ?? control.name}
                           aria-label={`Edit filter ${control.name}`}
                           // Warning where a binding names a field the source
                           // does not have: the package would refuse the file.
                           color={unknown.length > 0 ? "warning" : "default"}
                           variant={
                              control.origin === "dashboard"
                                 ? "filled"
                                 : "outlined"
                           }
                           onClick={() => onEdit(control)}
                           onDelete={() => onRemove(control.name)}
                           deleteIcon={
                              <CancelIcon
                                 aria-label={`Remove filter ${control.name}`}
                              />
                           }
                           sx={{
                              // Faint when nothing binds it: declared, but not yet a
                              // control a reader would see.
                              opacity: control.boundTiles === 0 ? 0.6 : 1,
                              cursor: "pointer",
                              transition: "opacity 120ms",
                           }}
                        />
                     </Tooltip>
                  );
               })}
               <Box sx={{ ml: "auto" }}>
                  <SecondaryButton
                     label="Filter"
                     icon={<AddIcon />}
                     onClick={() => onAdd()}
                     ariaLabel="Add filter"
                     ariaHasPopup="dialog"
                     {...(addDisabledReason
                        ? { disabled: true, disabledReason: addDisabledReason }
                        : {})}
                  />
               </Box>
            </Stack>

            {children}
         </Stack>
      </>
   );
}
