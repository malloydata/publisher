// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import CancelIcon from "@mui/icons-material/Cancel";
import FilterListIcon from "@mui/icons-material/FilterList";
import { Chip, Stack, Tooltip, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { BuilderControl } from "./controls";

/**
 * The strip where the dashboard's controls are configured — the ONE place: a
 * chip per control opens its window, and "Add filter" beside them declares a
 * new one. The live control row the host renders sits under it.
 */
export function FilterStrip({
   controls: controlList,
   tileCount,
   unknownFieldsOf,
   onEdit,
   onAdd,
   onRemove,
   children,
}: {
   controls: BuilderControl[];
   tileCount: number;
   /** Bindings a control has that its tiles' sources cannot take. */
   unknownFieldsOf: (name: string, type: string | undefined) => string[];
   onEdit: (control: BuilderControl) => void;
   onAdd: () => void;
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
               // Pinned at the top of the scroller, so the controls stay in
               // reach on a long page.
               top: 0,
               zIndex: 4,
               bgcolor: theme.background,
               pb: 1,
            }}
         >
            <Stack
               direction="row"
               sx={{ gap: 2, alignItems: "center", minHeight: 32 }}
            >
               <Stack
                  direction="row"
                  aria-label="Filters"
                  sx={{
                     gap: 1,
                     alignItems: "center",
                     flexWrap: "wrap",
                     flex: 1,
                     minWidth: 0,
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
                     const unknown = unknownFieldsOf(
                        control.name,
                        control.type,
                     );
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
                  {/* Beside the chips and shaped like one, as the next filter
                   would be: outlined where they are filled, so it reads as the
                   empty slot rather than another filter. */}
                  <Chip
                     size="small"
                     variant="outlined"
                     icon={<AddIcon />}
                     label="Filter"
                     aria-label="Add filter"
                     aria-haspopup="dialog"
                     onClick={() => onAdd()}
                     sx={{
                        cursor: "pointer",
                        color: theme.tileTitle,
                        borderStyle: "dashed",
                        "& .MuiChip-icon": { color: "inherit", fontSize: 16 },
                     }}
                  />
               </Stack>
            </Stack>

            {children}
         </Stack>
      </>
   );
}
